//! Conservative vLLM observation and level-2 restoration ordering.
//! No development endpoint is enabled and no control request is emitted here.

use std::sync::Mutex;

use serde::Deserialize;

use super::{AdapterCapabilities, AdapterCapability, ProbeOutcome, ProbeRequest, WorkloadAdapter};
use crate::config::ProviderKind;
use crate::inventory::Workload;
use crate::policy::activity::ActivityEvidence;

/// One configured server, treated as one workload even when workers are distributed.
pub struct VllmAdapter {
    name: String,
    endpoint: String,
    scope: String,
    capabilities: AdapterCapabilities,
    sleeping: Mutex<Option<bool>>,
}

impl VllmAdapter {
    pub fn new(name: String, endpoint: String, scope: String) -> Self {
        Self {
            name,
            endpoint,
            scope,
            capabilities: AdapterCapabilities::configured(&[
                AdapterCapability::Discovery,
                AdapterCapability::Activity,
            ]),
            sleeping: Mutex::new(None),
        }
    }
}

#[derive(Deserialize)]
struct Sleeping {
    is_sleeping: bool,
}

impl WorkloadAdapter for VllmAdapter {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> ProviderKind {
        ProviderKind::Vllm
    }
    fn endpoint(&self) -> &str {
        &self.endpoint
    }
    fn capabilities(&self) -> &AdapterCapabilities {
        &self.capabilities
    }
    fn owns(&self, workload: &Workload) -> bool {
        workload.id.scope == self.scope
    }
    fn discover(&self) -> Vec<ProbeRequest> {
        vec![ProbeRequest::get(format!("{}/is_sleeping", self.endpoint))]
    }
    fn observe(&self, outcomes: &[ProbeOutcome], _at_ms: u64) -> Vec<String> {
        let url = format!("{}/is_sleeping", self.endpoint);
        let reading = outcomes
            .iter()
            .find(|outcome| outcome.request.url == url)
            .and_then(|outcome| outcome.result.as_ref().ok())
            .filter(|response| response.status == 200)
            .and_then(|response| serde_json::from_str::<Sleeping>(&response.body).ok());
        *self
            .sleeping
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            reading.map(|reading| reading.is_sleeping);
        vec![format!("provider {}: vLLM observation only; sleeping is not proof of restored weights or idle requests; host and device memory unknown", self.name)]
    }
    fn activity(&self, _workload: &Workload) -> ActivityEvidence {
        // Sleep state can be a partial wake, not an absence of live requests.
        ActivityEvidence::Unknown
    }
}

/// Ordered evidence required after discarding weights at sleep level 2.
/// A failed worker invalidates the entire workload's restoration evidence.
#[derive(Debug, Default, PartialEq, Eq)]
pub enum RestoreState {
    #[default]
    MissingWeights,
    WeightsAllocated,
    WeightsReloaded,
    CacheAllocated,
    Verified,
}

impl RestoreState {
    /// Advance only the next step; out-of-order or failed evidence invalidates all.
    pub fn record(&mut self, step: RestoreStep, success: bool) -> bool {
        let next = match (&*self, step, success) {
            (Self::MissingWeights, RestoreStep::AllocateWeights, true) => Self::WeightsAllocated,
            (Self::WeightsAllocated, RestoreStep::ReloadWeights, true) => Self::WeightsReloaded,
            (Self::WeightsReloaded, RestoreStep::AllocateCache, true) => Self::CacheAllocated,
            (Self::CacheAllocated, RestoreStep::VerifyInference, true) => Self::Verified,
            _ => {
                *self = Self::MissingWeights;
                return false;
            }
        };
        *self = next;
        true
    }

    pub fn may_serve(&self) -> bool {
        *self == Self::Verified
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RestoreStep {
    AllocateWeights,
    ReloadWeights,
    AllocateCache,
    VerifyInference,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{AdapterCapability, Response};

    #[test]
    fn incomplete_or_failed_restore_never_serves() {
        let mut state = RestoreState::default();
        assert!(!state.record(RestoreStep::VerifyInference, true));
        for step in [
            RestoreStep::AllocateWeights,
            RestoreStep::ReloadWeights,
            RestoreStep::AllocateCache,
        ] {
            assert!(state.record(step, true));
            assert!(!state.may_serve());
        }
        assert!(state.record(RestoreStep::VerifyInference, true));
        assert!(state.may_serve());
        assert!(!state.record(RestoreStep::VerifyInference, false));
        assert!(!state.may_serve());
    }

    #[test]
    fn disabled_denied_and_partial_responses_clear_sleep_observation() {
        let adapter = VllmAdapter::new(
            "vllm".into(),
            "http://127.0.0.1:8000".into(),
            "unit:vllm.service".into(),
        );
        for response in [
            Response::new(200, r#"{"is_sleeping":true}"#),
            Response::new(404, "{}"),
            Response::new(401, "{}"),
            Response::new(200, "{}"),
            Response::new(500, "{}"),
        ] {
            adapter.observe(
                &[ProbeOutcome {
                    request: adapter.discover().remove(0),
                    result: Ok(response.clone()),
                }],
                100,
            );
            assert_eq!(
                *adapter.sleeping.lock().unwrap(),
                if response.status == 200 && response.body.contains("true") {
                    Some(true)
                } else {
                    None
                }
            );
        }
        assert!(!adapter
            .capabilities()
            .supports(AdapterCapability::SleepModel));
        assert!(!adapter.inventory_is_current());
    }
}
