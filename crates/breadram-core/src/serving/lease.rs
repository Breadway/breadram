//! Request leases: what breadram knows about in-flight inference.
//!
//! A lease records that a request is in flight. It records nothing about the
//! request itself, and that is structural rather than a filtering step: there is
//! nowhere to put a prompt, a completion, a response body, or a credential. A
//! lease holds a client label, a request kind, whether the response is streamed,
//! an optional idempotency key, and monotonic timestamps. "The gateway never
//! logs prompts" is therefore checkable by reading the type rather than by
//! auditing every call site.
//!
//! Two behaviours matter for memory management:
//!
//! - **Idleness is proven, not assumed.** A backend is idle only when no lease
//!   is in flight, and the idle period is reported from the moment the last one
//!   ended, so the policy minimum still applies.
//! - **A retry is not a second request.** A request that arrives with the same
//!   idempotency key while the original is still running joins it instead of
//!   counting twice, and one that arrives after it finished is replayed rather
//!   than dispatched again. A client that retries a long stream must not be able
//!   to keep a backend permanently awake by accident.

use std::collections::{BTreeMap, VecDeque};

use crate::inventory::WorkloadId;
use crate::policy::activity::{ActivityEvidence, ActivitySource};

/// How many finished request keys are remembered, so a retry is recognized.
///
/// Bounded on purpose: a busy gateway must not grow with traffic. A key that has
/// been evicted is treated as a new request, which is the safe direction, since
/// the alternative would be serving a stale answer forever.
pub const RETRY_MEMORY: usize = 1_024;

/// A client label, used in diagnostics.
///
/// A name, never a credential. This is the most identifying thing a lease holds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClientId(pub String);

/// What a request is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    /// An interactive request. The client is waiting on the response.
    Interactive,
    /// Admitted to a queue but not yet dispatched. It will run, so it holds the
    /// backend open unless policy says otherwise.
    Queued,
    /// A background or batch request, which yields first.
    Background,
}

impl RequestKind {
    /// The stable name used on the wire and in diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            RequestKind::Interactive => "interactive",
            RequestKind::Queued => "queued",
            RequestKind::Background => "background",
        }
    }
}

/// The public shape of a request, with no content in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestMeta {
    /// Who is asking.
    pub client: ClientId,
    /// What kind of request it is.
    pub kind: RequestKind,
    /// Whether the response is streamed. A stream holds its lease for its whole
    /// duration, which is what stops a backend sleeping mid-stream.
    pub streaming: bool,
    /// A client supplied idempotency key, when there is one.
    ///
    /// An identifier, not a payload. Two requests with the same key are the
    /// same request.
    pub key: Option<String>,
}

impl RequestMeta {
    /// An interactive, non-streamed request.
    pub fn new(client: impl Into<String>) -> Self {
        Self {
            client: ClientId(client.into()),
            kind: RequestKind::Interactive,
            streaming: false,
            key: None,
        }
    }

    /// The same request, with an idempotency key.
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// The same request, marked as a stream.
    pub fn streaming(mut self) -> Self {
        self.streaming = true;
        self
    }

    /// The same request, with a kind.
    pub fn with_kind(mut self, kind: RequestKind) -> Self {
        self.kind = kind;
        self
    }
}

/// A lease id, unique within one tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseId(pub u64);

/// The result of admitting a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// A new request, counted now.
    Started(LeaseId),
    /// The same request again while it is still running. Counted once.
    Joined(LeaseId),
    /// The same request again after it finished. Not dispatched again.
    Replayed(LeaseId),
}

impl Admission {
    /// The lease this admission refers to.
    pub fn lease(self) -> LeaseId {
        match self {
            Admission::Started(id) | Admission::Joined(id) | Admission::Replayed(id) => id,
        }
    }
}

/// One in-flight request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// The lease id.
    pub id: LeaseId,
    /// The client that opened it.
    pub client: ClientId,
    /// What kind of request it is.
    pub kind: RequestKind,
    /// Whether the response is streamed.
    pub streaming: bool,
    /// The idempotency key, when there is one.
    pub key: Option<String>,
    /// When it was admitted, in monotonic milliseconds.
    pub started_at_ms: u64,
    /// How many clients are waiting on this same request.
    pub joined: usize,
}

/// What is in flight for one backend.
///
/// The caller owns the locking: one tracker per backend, mutated by one owner at
/// a time. Nothing here spawns a task or holds a handle, which is what makes the
/// whole lifecycle testable without a live provider.
#[derive(Debug)]
pub struct Leases {
    next_id: u64,
    active: BTreeMap<LeaseId, Lease>,
    finished: VecDeque<(String, LeaseId)>,
    last_activity_ms: u64,
}

impl Leases {
    /// An idle tracker.
    pub fn new(now_ms: u64) -> Self {
        Self {
            next_id: 1,
            active: BTreeMap::new(),
            finished: VecDeque::new(),
            last_activity_ms: now_ms,
        }
    }

    /// Admit a request, or recognize it as one already known.
    pub fn admit(&mut self, meta: RequestMeta, now_ms: u64) -> Admission {
        if let Some(key) = &meta.key {
            if let Some(lease) = self
                .active
                .values_mut()
                .find(|lease| lease.key.as_deref() == Some(key.as_str()))
            {
                lease.joined += 1;
                return Admission::Joined(lease.id);
            }
            if let Some((_, id)) = self.finished.iter().find(|(finished, _)| finished == key) {
                return Admission::Replayed(*id);
            }
        }
        let id = LeaseId(self.next_id);
        self.next_id += 1;
        self.active.insert(
            id,
            Lease {
                id,
                client: meta.client,
                kind: meta.kind,
                streaming: meta.streaming,
                key: meta.key,
                started_at_ms: now_ms,
                joined: 0,
            },
        );
        self.last_activity_ms = now_ms;
        Admission::Started(id)
    }

    /// Release a lease because the request finished.
    ///
    /// Returns the lease that ended, or `None` when it was already gone, which
    /// happens when a duplicate finish follows a cancellation.
    pub fn finish(&mut self, id: LeaseId, now_ms: u64) -> Option<Lease> {
        let lease = self.active.remove(&id)?;
        if let Some(key) = &lease.key {
            self.remember(key.clone(), id);
        }
        self.last_activity_ms = now_ms;
        Some(lease)
    }

    /// Promote a queued request to a running one, as a dispatch does.
    ///
    /// Returns whether the lease was queued. A request that is already running
    /// is left alone, so a doubled dispatch cannot double count it.
    pub fn dispatch(&mut self, id: LeaseId, now_ms: u64) -> bool {
        let Some(lease) = self.active.get_mut(&id) else {
            return false;
        };
        if lease.kind != RequestKind::Queued {
            return false;
        }
        lease.kind = RequestKind::Interactive;
        self.last_activity_ms = now_ms;
        true
    }

    /// Release a lease because the client went away or the request was
    /// cancelled. A cancelled request is not remembered as finished, so a retry
    /// is dispatched again rather than answered from nothing.
    pub fn cancel(&mut self, id: LeaseId, now_ms: u64) -> Option<Lease> {
        let lease = self.active.remove(&id)?;
        self.last_activity_ms = now_ms;
        Some(lease)
    }

    /// Release every lease belonging to one client, as a disconnect does.
    ///
    /// A streaming client that disappears must not leave the backend as
    /// permanently busy.
    pub fn cancel_client(&mut self, client: &ClientId, now_ms: u64) -> Vec<Lease> {
        let ids: Vec<LeaseId> = self
            .active
            .values()
            .filter(|lease| &lease.client == client)
            .map(|lease| lease.id)
            .collect();
        let mut released = Vec::new();
        for id in ids {
            if let Some(lease) = self.active.remove(&id) {
                self.last_activity_ms = now_ms;
                released.push(lease);
            }
        }
        released
    }

    /// Forget everything, as a restart does.
    ///
    /// A restart loses the in-flight requests: their clients see a closed
    /// connection and retry, and a retry carries the same idempotency key, which
    /// is why the memory is cleared rather than guessed at.
    pub fn reset(&mut self, now_ms: u64) {
        self.active.clear();
        self.finished.clear();
        self.last_activity_ms = now_ms;
    }

    fn remember(&mut self, key: String, id: LeaseId) {
        while self.finished.len() >= RETRY_MEMORY {
            self.finished.pop_front();
        }
        self.finished.push_back((key, id));
    }

    /// How many distinct requests are in flight.
    pub fn in_flight(&self) -> usize {
        self.active.len()
    }

    /// How many are not queued, so they are doing work right now.
    pub fn dispatched(&self) -> usize {
        self.active
            .values()
            .filter(|lease| lease.kind != RequestKind::Queued)
            .count()
    }

    /// How many are queued but not yet dispatched.
    pub fn queued(&self) -> usize {
        self.active
            .values()
            .filter(|lease| lease.kind == RequestKind::Queued)
            .count()
    }

    /// How many are streams.
    pub fn streaming(&self) -> usize {
        self.active.values().filter(|lease| lease.streaming).count()
    }

    /// Whether anything is in flight.
    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// The in-flight leases.
    pub fn active(&self) -> impl Iterator<Item = &Lease> {
        self.active.values()
    }

    /// The lease with this id, when it is still in flight.
    pub fn get(&self, id: LeaseId) -> Option<&Lease> {
        self.active.get(&id)
    }

    /// When the last lease started or ended, in monotonic milliseconds.
    pub fn last_activity_ms(&self) -> u64 {
        self.last_activity_ms
    }

    /// The idle period so far, or `None` while something is in flight.
    pub fn idle_since_ms(&self) -> Option<u64> {
        if self.active.is_empty() {
            Some(self.last_activity_ms)
        } else {
            None
        }
    }

    /// The evidence the planner consumes.
    pub fn evidence(&self) -> ActivityEvidence {
        match self.idle_since_ms() {
            Some(since_ms) => ActivityEvidence::Idle { since_ms },
            None => ActivityEvidence::Active,
        }
    }
}

/// The leases of one workload, presented as activity evidence.
///
/// This is the whole bridge between serving coordination and policy: policy sees
/// [`ActivitySource`] and never learns that leases, adapters, or a gateway
/// exist.
#[derive(Debug)]
pub struct WorkloadLeases {
    workload: WorkloadId,
    leases: Leases,
}

impl WorkloadLeases {
    /// A tracker for one workload, idle as of now.
    pub fn new(workload: WorkloadId, now_ms: u64) -> Self {
        Self {
            workload,
            leases: Leases::new(now_ms),
        }
    }

    /// The workload these leases belong to.
    pub fn workload(&self) -> &WorkloadId {
        &self.workload
    }

    /// The tracker.
    pub fn leases(&self) -> &Leases {
        &self.leases
    }

    /// The tracker, for mutation.
    pub fn leases_mut(&mut self) -> &mut Leases {
        &mut self.leases
    }
}

impl ActivitySource for WorkloadLeases {
    fn evidence(&self, workload: &WorkloadId) -> ActivityEvidence {
        // A workload this tracker does not serve is unknown, not idle. An
        // unknown answer is never treated as idle, which is the safe direction.
        if &self.workload == workload {
            self.leases.evidence()
        } else {
            ActivityEvidence::Unknown
        }
    }
}
