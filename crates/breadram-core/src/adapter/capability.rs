//! Versioned, per adapter capability sets.
//!
//! An adapter's capabilities are either discovered by a bounded read-only probe
//! or explicitly configured, and they carry the probe version that produced
//! them. Two rules keep this conservative:
//!
//! 1. Only [`CapabilityState::Available`] counts as support for an action. A
//!    degraded or unavailable reading means the action is not driven, and a
//!    capability that was never probed is simply absent.
//! 2. A reading from a probe version this build does not understand is
//!    discarded in full and the adapter falls back to observation, rather than
//!    having its answers misread.

use std::collections::BTreeMap;

use crate::capability::CapabilityState;

/// The capability probe contract this build understands.
///
/// Bump it whenever the meaning of a probe changes, so an older or newer
/// adapter's results are not silently reinterpreted.
pub const CAPABILITY_PROBE_VERSION: u32 = 1;

/// A capability an adapter may or may not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AdapterCapability {
    /// Whether the adapter can be discovered at all.
    Discovery,
    /// Read-only inventory of the resources it manages.
    Inventory,
    /// Whether it can tell whether a workload is busy or idle.
    Activity,
    /// Release a loaded model.
    UnloadModel,
    /// Ask a backend to sleep.
    SleepModel,
    /// Stop accepting new work and drain in-flight work.
    DrainLoad,
    /// Bring a slept backend back.
    WakeModel,
    /// The model administration surface, which an OpenAI-compatible inference
    /// API does not imply and often does not have.
    ModelAdministration,
}

impl AdapterCapability {
    /// The stable name used in reports.
    pub fn as_str(self) -> &'static str {
        match self {
            AdapterCapability::Discovery => "discovery",
            AdapterCapability::Inventory => "inventory",
            AdapterCapability::Activity => "activity",
            AdapterCapability::UnloadModel => "unload_model",
            AdapterCapability::SleepModel => "sleep_model",
            AdapterCapability::DrainLoad => "drain_load",
            AdapterCapability::WakeModel => "wake_model",
            AdapterCapability::ModelAdministration => "model_administration",
        }
    }
}

/// What an adapter can do, and the probe version that established it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterCapabilities {
    probe_version: u32,
    states: BTreeMap<AdapterCapability, CapabilityState>,
}

impl AdapterCapabilities {
    /// Nothing is known: every capability falls back to observation.
    pub fn observation_only() -> Self {
        Self {
            probe_version: CAPABILITY_PROBE_VERSION,
            states: BTreeMap::new(),
        }
    }

    /// Build from an explicit configuration of capabilities.
    ///
    /// This is the supported path for a deployment that would rather state what
    /// an adapter can do than have it probed.
    pub fn configured(configured: &[AdapterCapability]) -> Self {
        let mut capabilities = Self::observation_only();
        for capability in configured {
            capabilities.record(*capability, CapabilityState::Available);
        }
        capabilities
    }

    /// Build from probe readings, tagged with the version that produced them.
    ///
    /// A version mismatch discards the readings rather than trusting them.
    pub fn from_probe(
        probe_version: u32,
        readings: impl IntoIterator<Item = (AdapterCapability, CapabilityState)>,
    ) -> Self {
        if probe_version != CAPABILITY_PROBE_VERSION {
            return Self::observation_only();
        }
        let mut capabilities = Self::observation_only();
        for (capability, state) in readings {
            capabilities.record(capability, state);
        }
        capabilities
    }

    /// Record or replace one capability.
    pub fn record(&mut self, capability: AdapterCapability, state: CapabilityState) {
        self.states.insert(capability, state);
    }

    /// The probe version these readings came from.
    pub fn probe_version(&self) -> u32 {
        self.probe_version
    }

    /// The recorded state, if the capability was probed or configured.
    pub fn state(&self, capability: AdapterCapability) -> Option<&CapabilityState> {
        self.states.get(&capability)
    }

    /// Whether an action using this capability may be driven.
    ///
    /// Only a fully available capability counts, so a degraded reading cannot be
    /// used to trigger a control operation.
    pub fn supports(&self, capability: AdapterCapability) -> bool {
        matches!(self.state(capability), Some(CapabilityState::Available))
    }

    /// Whether this capability can be read or reported at all.
    pub fn observes(&self, capability: AdapterCapability) -> bool {
        self.state(capability)
            .is_some_and(CapabilityState::is_usable)
    }

    /// Whether nothing at all is available, so the adapter can only be observed.
    pub fn is_observation_only(&self) -> bool {
        self.states.values().all(|state| !state.is_usable())
    }

    /// The probed or configured capabilities.
    pub fn iter(&self) -> impl Iterator<Item = (AdapterCapability, &CapabilityState)> {
        self.states
            .iter()
            .map(|(capability, state)| (*capability, state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unprobed_capability_is_not_supported() {
        let capabilities = AdapterCapabilities::observation_only();
        assert!(!capabilities.supports(AdapterCapability::ModelAdministration));
        assert!(!capabilities.observes(AdapterCapability::ModelAdministration));
        assert!(capabilities.is_observation_only());
    }

    #[test]
    fn an_unknown_feature_falls_back_to_observation() {
        // A reading about one capability says nothing about another, and an
        // unknown capability is simply not supported.
        let capabilities = AdapterCapabilities::from_probe(
            CAPABILITY_PROBE_VERSION,
            [(AdapterCapability::Activity, CapabilityState::Available)],
        );
        assert!(capabilities.supports(AdapterCapability::Activity));
        assert!(!capabilities.supports(AdapterCapability::WakeModel));
        assert!(!capabilities.is_observation_only());
    }

    #[test]
    fn a_degraded_capability_is_reported_but_not_driven() {
        let capabilities = AdapterCapabilities::from_probe(
            CAPABILITY_PROBE_VERSION,
            [(
                AdapterCapability::SleepModel,
                CapabilityState::Degraded {
                    reason: "sleep is best effort".to_string(),
                },
            )],
        );
        assert!(capabilities.observes(AdapterCapability::SleepModel));
        assert!(
            !capabilities.supports(AdapterCapability::SleepModel),
            "a degraded control must not be driven"
        );
    }

    #[test]
    fn a_probe_from_another_version_is_discarded() {
        let capabilities = AdapterCapabilities::from_probe(
            CAPABILITY_PROBE_VERSION + 1,
            [(AdapterCapability::UnloadModel, CapabilityState::Available)],
        );
        assert!(capabilities.is_observation_only());
        assert!(!capabilities.supports(AdapterCapability::UnloadModel));
    }

    #[test]
    fn an_explicit_configuration_is_honoured() {
        let capabilities = AdapterCapabilities::configured(&[AdapterCapability::UnloadModel]);
        assert!(capabilities.supports(AdapterCapability::UnloadModel));
    }

    #[test]
    fn the_probe_version_is_reported() {
        assert_eq!(
            AdapterCapabilities::observation_only().probe_version(),
            CAPABILITY_PROBE_VERSION
        );
    }
}
