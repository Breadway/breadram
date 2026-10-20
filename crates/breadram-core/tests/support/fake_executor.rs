//! A scripted coordinator executor, shared by the core suites.
//!
//! The fake scripts every failure mode the coordinator must report honestly: a
//! partial write, a lost connection, a timeout, a refusal, a busy target, a
//! vanished target, and a target that already has a confirmed effect. RAM-29
//! shared support.

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use breadram_core::config::ActionKind;
use breadram_core::coordinator::{
    AppliedChange, Confirmation, Executor, ExecutorError, MemoryObservation, OwnedValue,
    OwnerToken, Precondition,
};

use super::GIB;

pub fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[derive(Default)]
pub struct FakeExecutor {
    pub supported: Mutex<Vec<ActionKind>>,
    pub precondition: Mutex<Option<Precondition>>,
    pub applies: Mutex<VecDeque<Result<AppliedChange, ExecutorError>>>,
    pub confirmations: Mutex<VecDeque<Confirmation>>,
    pub observations: Mutex<HashMap<String, MemoryObservation>>,
    pub after_observation: Mutex<Option<MemoryObservation>>,
    pub values: Mutex<HashMap<String, String>>,
    pub writes: Mutex<Vec<(String, String)>>,
    pub apply_calls: Mutex<usize>,
    pub effect_absent: Mutex<bool>,
}

impl FakeExecutor {
    pub fn with(kinds: &[ActionKind], initial_host_ram: u64, after_host_ram: u64) -> Self {
        let executor = Self {
            supported: Mutex::new(kinds.to_vec()),
            ..Self::default()
        };
        lock(&executor.observations).insert(
            "unit:app.service".to_string(),
            MemoryObservation {
                host_ram_bytes: Some(initial_host_ram),
                device_ram_bytes: Some(8 * GIB),
            },
        );
        *lock(&executor.after_observation) = Some(MemoryObservation {
            host_ram_bytes: Some(after_host_ram),
            device_ram_bytes: Some(6 * GIB),
        });
        executor
    }

    pub fn script_apply(self, outcome: Result<AppliedChange, ExecutorError>) -> Self {
        lock(&self.applies).push_back(outcome);
        self
    }

    pub fn script_confirmation(self, confirmation: Confirmation) -> Self {
        lock(&self.confirmations).push_back(confirmation);
        self
    }

    pub fn set_precondition(&self, precondition: Precondition) {
        *lock(&self.precondition) = Some(precondition);
    }

    pub fn revoke_support(&self, kind: ActionKind) {
        lock(&self.supported).retain(|candidate| *candidate != kind);
    }

    pub fn set_value(&self, key: &str, value: &str) {
        lock(&self.values).insert(key.to_string(), value.to_string());
    }

    pub fn value(&self, key: &str) -> Option<String> {
        lock(&self.values).get(key).cloned()
    }

    pub fn apply_count(&self) -> usize {
        *lock(&self.apply_calls)
    }

    pub fn write_count(&self) -> usize {
        lock(&self.writes).len()
    }
}

pub fn owned(key: &str, original: &str, last_applied: &str) -> OwnedValue {
    OwnedValue {
        key: key.to_string(),
        original: original.to_string(),
        last_applied: last_applied.to_string(),
        owner: OwnerToken("instance-1".to_string()),
    }
}

impl Executor for FakeExecutor {
    fn confirmed_effect_absent(
        &self,
        _request: &breadram_core::coordinator::ActionRequest,
    ) -> bool {
        *lock(&self.effect_absent)
    }
    fn supports(&self, kind: ActionKind) -> bool {
        lock(&self.supported).contains(&kind)
    }

    fn precondition(&self, _request: &breadram_core::coordinator::ActionRequest) -> Precondition {
        lock(&self.precondition)
            .clone()
            .unwrap_or(Precondition::Ready)
    }

    fn apply(
        &self,
        _request: &breadram_core::coordinator::ActionRequest,
    ) -> Result<AppliedChange, ExecutorError> {
        *lock(&self.apply_calls) += 1;
        let outcome = lock(&self.applies)
            .pop_front()
            .unwrap_or_else(|| Ok(AppliedChange::default()));
        if outcome.is_ok() {
            if let Some(after) = *lock(&self.after_observation) {
                lock(&self.observations).insert("unit:app.service".to_string(), after);
            }
            if let Ok(change) = &outcome {
                for value in &change.values {
                    self.set_value(&value.key, &value.last_applied);
                }
            }
        }
        outcome
    }

    fn confirm(&self, _request: &breadram_core::coordinator::ActionRequest) -> Confirmation {
        lock(&self.confirmations)
            .pop_front()
            .unwrap_or(Confirmation::Confirmed)
    }

    fn observe(&self, target: &breadram_core::inventory::WorkloadId) -> MemoryObservation {
        lock(&self.observations)
            .get(&target.scope)
            .copied()
            .unwrap_or_default()
    }

    fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError> {
        Ok(self.value(key))
    }

    fn write_value(&self, key: &str, value: &str) -> Result<(), ExecutorError> {
        lock(&self.writes).push((key.to_string(), value.to_string()));
        self.set_value(key, value);
        Ok(())
    }
}
