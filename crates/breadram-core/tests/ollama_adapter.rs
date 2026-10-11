//! The Ollama adapter, tested against recorded API shapes.
//!
//! Nothing here talks to a service. The bodies below are the documented
//! `/api/ps` and `/api/version` shapes, so the tests pin how the adapter reads
//! them, what it refuses to conclude, and that the only write it can produce is
//! the documented unload.

use breadram_core::adapter::{
    AdapterCapability, OllamaAdapter, OllamaError, ProbeRequest, Response, WorkloadAdapter,
    PS_PATH, VERSION_PATH,
};
use breadram_core::capability::CapabilityState;
use breadram_core::config::{ActionKind, ProviderKind};
use breadram_core::inventory::{ControlLevel, MemoryAccounting, Workload, WorkloadId};
use breadram_core::policy::activity::ActivityEvidence;
use breadram_core::serving::TrackingCompleteness;

const SCOPE: &str = "unit:ollama.service";

/// 2026-10-11T00:00:00Z, so the recorded expiries below are read against a fixed
/// instant rather than the clock the test happens to run on. Only keep-alive
/// policy uses it; activity evidence is monotonic and never does.
const FIXED_NOW_MS: u64 = 1_791_676_800_000;

fn workload() -> Workload {
    Workload {
        id: WorkloadId {
            scope: SCOPE.to_string(),
            generation: 12,
        },
        name: "ollama".to_string(),
        pids: vec![4242],
        accounting: MemoryAccounting::Cgroup {
            current_bytes: Some(4 * 1024 * 1024 * 1024),
        },
        control: ControlLevel::Eligible,
        limit_bytes: None,
    }
}

fn ps_response(models: &str) -> Response {
    Response::new(200, format!(r#"{{"models":[{models}]}}"#))
}

/// One GPU-held model and one CPU-only model.
fn two_models() -> Response {
    ps_response(
        r#"{"name":"llama3:8b","model":"llama3:8b","size":4294967296,"size_vram":3221225472,"expires_at":"2026-10-11T16:00:00Z"},
           {"name":"nomic-embed-text","model":"nomic-embed-text","size":2147483648,"size_vram":0,"expires_at":"2026-10-11T15:30:00Z"}"#,
    )
}

/// An adapter with probed capabilities and a successful model list.
fn observing(response: &Response, now_ms: u64) -> OllamaAdapter {
    let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_reference_time(FIXED_NOW_MS)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(response));
    adapter
        .observe_loaded(response, now_ms)
        .expect("the recorded body parses");
    adapter
}

#[test]
fn discovery_asks_only_the_documented_read_endpoints() {
    let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&ps_response("")));
    let requests = adapter.discover();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        // A probe can only be a read, by construction.
        assert_eq!(request.method.as_str(), "GET");
        assert!(matches!(request, ProbeRequest { .. }));
    }
    let urls: Vec<&str> = vec![
        "http://127.0.0.1:11434/api/version",
        "http://127.0.0.1:11434/api/ps",
    ];
    assert_eq!(requests[0].url, urls[0]);
    assert_eq!(requests[1].url, urls[1]);
    assert!(requests[0].url.ends_with(VERSION_PATH));
    assert!(requests[1].url.ends_with(PS_PATH));

    // An observation-only adapter asks nothing at all.
    let quiet = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE);
    assert!(quiet.discover().is_empty());
}

#[test]
fn inventory_keeps_host_and_device_memory_separate() {
    let adapter = observing(&two_models(), 1_000);
    let inventory = adapter.inventory();
    assert_eq!(inventory.len(), 2);

    let gpu = inventory
        .iter()
        .find(|resource| resource.name == "llama3:8b")
        .expect("the GPU model");
    assert_eq!(gpu.device_ram_bytes, Some(3_221_225_472));
    assert_eq!(
        gpu.host_ram_bytes,
        Some(4_294_967_296 - 3_221_225_472),
        "the host figure is the part not held in device memory"
    );

    let cpu = inventory
        .iter()
        .find(|resource| resource.name == "nomic-embed-text")
        .expect("the CPU model");
    assert_eq!(
        cpu.device_ram_bytes, None,
        "no VRAM means unknown, not zero"
    );
    assert_eq!(cpu.host_ram_bytes, Some(2_147_483_648));

    // A loaded model is never reported as idle: being resident says nothing
    // about whether anything is being inferred.
    assert!(inventory.iter().all(|resource| !resource.idle));

    // The model's file size is larger than the host figure, so it is never
    // presented as host memory usage.
    let model = adapter
        .loaded()
        .into_iter()
        .find(|model| model.name == "llama3:8b")
        .expect("loaded");
    assert!(model.host_model_bytes() < model.size_bytes);
}

#[test]
fn a_loaded_model_is_active_however_its_expiry_reads() {
    for expires in [
        "2099-01-01T00:00:00Z", // far future
        "2000-01-01T00:00:00Z", // already expired, and still listed
    ] {
        let body = ps_response(&format!(
            r#"{{"name":"llama3:8b","size":4294967296,"size_vram":0,"expires_at":"{expires}"}}"#
        ));
        let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
            .with_reference_time(FIXED_NOW_MS)
            .with_capabilities(OllamaAdapter::capabilities_from_probes(&body));
        adapter.observe_loaded(&body, 1_000).expect("parses");
        assert_eq!(
            adapter.activity(&workload()),
            ActivityEvidence::Active,
            "expiry is not proof that inference stopped: {expires}"
        );
    }
}

#[test]
fn an_empty_model_list_starts_the_idle_clock() {
    let empty = ps_response("");
    let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&empty));
    adapter.observe_loaded(&empty, 5_000).expect("parses");
    assert_eq!(
        adapter.activity(&workload()),
        ActivityEvidence::Idle { since_ms: 5_000 }
    );

    // Further empty readings do not restart the clock.
    adapter.observe_loaded(&empty, 9_000).expect("parses");
    assert_eq!(
        adapter.activity(&workload()),
        ActivityEvidence::Idle { since_ms: 5_000 }
    );

    // A model appearing resets it, and a later empty reading starts a new one.
    adapter
        .observe_loaded(&two_models(), 10_000)
        .expect("parses");
    assert_eq!(adapter.activity(&workload()), ActivityEvidence::Active);
    adapter.observe_loaded(&empty, 20_000).expect("parses");
    assert_eq!(
        adapter.activity(&workload()),
        ActivityEvidence::Idle { since_ms: 20_000 }
    );
}

#[test]
fn a_missing_service_is_unknown_and_offers_no_operation() {
    let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_reference_time(FIXED_NOW_MS)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&two_models()));
    // Prime it with a good reading, so the failure has to clear that state.
    adapter
        .observe_loaded(&two_models(), 1_000)
        .expect("parses");
    assert!(adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .is_some());

    let err = adapter
        .observe_loaded(&Response::new(503, ""), 2_000)
        .expect_err("a dead service is an error");
    assert!(matches!(err, OllamaError::Unsupported { .. }));
    assert!(adapter.last_error().is_some());
    assert_eq!(
        adapter.activity(&workload()),
        ActivityEvidence::Unknown,
        "a failed read proves nothing, so it is never idle"
    );
    assert!(
        adapter
            .operation(ActionKind::UnloadIdleModel, &workload())
            .is_none(),
        "and it must not act on the previous reading"
    );
    assert!(adapter.inventory().is_empty());
    assert!(adapter.loaded().is_empty());
}

#[test]
fn a_build_without_the_model_list_is_reported_unsupported() {
    let missing = Response::new(404, "404 page not found");
    let capabilities = OllamaAdapter::capabilities_from_probes(&missing);
    assert_eq!(
        capabilities.state(AdapterCapability::UnloadModel),
        Some(&CapabilityState::Unavailable {
            reason: "the endpoint does not expose this surface".to_string()
        })
    );

    let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_capabilities(capabilities);
    let err = adapter
        .observe_loaded(&missing, 1_000)
        .expect_err("an absent surface is reported, not guessed at");
    assert!(
        err.to_string().contains("does not expose"),
        "the reason must name the cause: {err}"
    );
    assert!(adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .is_none());
}

#[test]
fn a_read_that_cannot_be_trusted_is_not_acted_on() {
    // Configured capabilities alone are not enough: without a successful read
    // there is nothing to act on.
    let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE).with_capabilities(
        breadram_core::adapter::AdapterCapabilities::configured(&[
            AdapterCapability::Inventory,
            AdapterCapability::Activity,
            AdapterCapability::UnloadModel,
        ]),
    );
    assert!(adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .is_none());
    assert_eq!(adapter.activity(&workload()), ActivityEvidence::Unknown);

    let err = adapter
        .observe_loaded(&Response::new(200, "not json at all"), 1_000)
        .expect_err("a malformed body is an error");
    assert!(matches!(err, OllamaError::Malformed { .. }));
    assert!(!adapter.is_fresh());
    assert!(adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .is_none());

    // An older build that reports `model` instead of `name` still works.
    let legacy = ps_response(r#"{"model":"tinyllama","size":1024,"size_vram":0}"#);
    let legacy_adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&legacy));
    legacy_adapter
        .observe_loaded(&legacy, 1_000)
        .expect("parses");
    assert_eq!(legacy_adapter.loaded()[0].name, "tinyllama");

    // A body with neither field is skipped rather than guessed at.
    let unnamed = ps_response(r#"{"size":1024}"#);
    let unnamed_adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&unnamed));
    unnamed_adapter
        .observe_loaded(&unnamed, 1_000)
        .expect("parses");
    assert!(unnamed_adapter.loaded().is_empty());
}

#[test]
fn an_igpu_release_frees_host_memory_only() {
    // On an iGPU the model's device bytes are a carve-out of host RAM, so a
    // release frees host memory and no separate device memory. Reporting part of
    // it as device relief would misreport where the memory came from.
    let response = two_models();
    let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_reference_time(FIXED_NOW_MS)
        .with_shared_device_memory(true)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&response));
    adapter
        .observe_loaded(&response, 1_000)
        .expect("the recorded body parses");
    let operation = adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .expect("a release is offered");
    assert_eq!(
        operation.expected_host_ram_delta, -4_294_967_296i64,
        "the whole model is host memory on an iGPU"
    );
    assert_eq!(
        operation.expected_device_ram_delta, 0,
        "there is no separate device memory to free on an iGPU"
    );
    assert!(
        operation.note.contains("4.0 GiB"),
        "the note names the whole model, not the host-only split: {}",
        operation.note
    );
}

#[test]
fn the_unload_uses_the_documented_flow_and_confirms_the_absence() {
    let adapter = observing(&two_models(), 1_000);
    let operation = adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .expect("a release is offered");

    assert_eq!(operation.kind, ActionKind::UnloadIdleModel);
    assert_eq!(operation.request.method.as_str(), "POST");
    assert_eq!(operation.request.url, "http://127.0.0.1:11434/api/generate");
    let body = operation.request.body.as_deref().expect("a JSON body");
    assert!(
        body.contains(r#""keep_alive":0"#),
        "the documented release flow: {body}"
    );
    assert!(body.contains(r#""model":"llama3:8b""#), "{body}");

    // The largest model is chosen, and each release is separately confirmed.
    assert!(operation.expected_host_ram_delta < 0);
    assert!(operation.expected_device_ram_delta < 0);
    assert!(
        operation.reversible,
        "reloading costs a cold start, not a loss"
    );

    let confirmation = &operation.confirmation;
    assert_eq!(confirmation.request.url, "http://127.0.0.1:11434/api/ps");
    assert_eq!(confirmation.request.method.as_str(), "GET");
    assert!(
        confirmation.confirms(r#"{"models":[{"name":"nomic-embed-text"}]}"#),
        "the model is gone, so the release is confirmed"
    );
    assert!(
        !confirmation.confirms(&two_models().body),
        "the model is still listed, so it is not confirmed"
    );

    // Nothing else can be produced: no pull, no delete, no inference call.
    for url in [&operation.request.url, &confirmation.request.url] {
        assert!(!url.contains("pull"));
        assert!(!url.contains("delete"));
        assert!(!url.contains("/api/chat"));
        assert!(!url.contains("/api/embeddings"));
    }

    // The cold start is stated, not hidden: the next request pays for the
    // memory this frees. The host figure is the part not held in device memory.
    let note = &operation.note;
    assert!(note.contains("llama3:8b"), "{note}");
    assert!(note.contains("cold-start"), "{note}");
    assert!(note.contains("1.0 GiB"), "the memory this frees: {note}");
    assert_eq!(
        operation.expected_host_ram_delta,
        -((4_294_967_296i64) - 3_221_225_472),
        "only the host half is host memory"
    );
    assert_eq!(operation.expected_device_ram_delta, -3_221_225_472);
}

#[test]
fn pinned_models_are_never_released() {
    let pinned = ps_response(
        r#"{"name":"pinned:latest","size":4294967296,"size_vram":0,"expires_at":"2099-01-01T00:00:00Z"},
           {"name":"pinned-by-operator:latest","size":1073741824,"size_vram":0,"expires_at":"2026-10-11T16:00:00Z"},
           {"name":"ordinary:latest","size":536870912,"size_vram":0,"expires_at":"2026-10-11T16:00:00Z"}"#,
    );
    let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_reference_time(FIXED_NOW_MS)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&pinned))
        .with_pinned_models(vec!["pinned-by-operator:latest".to_string()]);
    adapter.observe_loaded(&pinned, 1_000).expect("parses");

    let targets: Vec<String> = adapter
        .unload_targets()
        .into_iter()
        .map(|model| model.name)
        .collect();
    assert_eq!(targets, vec!["ordinary:latest".to_string()]);

    // Each refusal names its own cause, so a model left in memory is explained
    // rather than skipped in silence.
    let refusals: Vec<(String, &str)> = adapter
        .release_refusals()
        .into_iter()
        .map(|(model, reason)| (model.name, reason))
        .collect();
    assert_eq!(refusals.len(), 2, "{refusals:?}");
    assert!(
        refusals
            .iter()
            .any(|(name, reason)| name == "pinned-by-operator:latest" && reason.contains("pinned")),
        "{refusals:?}"
    );
    assert!(
        refusals.iter().any(|(name, reason)| name == "pinned:latest"
            && reason.contains("far beyond an ordinary keep-alive")),
        "a decades-long residency is a keep-alive choice, not an idle model: {refusals:?}"
    );

    // The horizon is the operator's dial: raising it makes the long keep-alive
    // releasable, which is the documented way to override the default.
    let permissive = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_reference_time(FIXED_NOW_MS)
        .with_max_bounded_keep_alive(u64::MAX)
        .with_pinned_models(vec!["pinned-by-operator:latest".to_string()])
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&pinned));
    permissive.observe_loaded(&pinned, 1_000).expect("parses");
    assert_eq!(
        permissive.unload_targets().len(),
        2,
        "the horizon no longer refuses the long keep-alive, but the explicit pin still does"
    );

    let operation = adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .expect("the ordinary model may be released");
    assert!(operation
        .request
        .body
        .as_deref()
        .is_some_and(|body| body.contains("ordinary:latest")));

    // A model the service does not report an expiry for is treated as pinned:
    // Ollama does not report the keep-alive it was loaded with, so there is
    // nothing to distinguish it from an explicit pin.
    let indefinite = ps_response(r#"{"name":"resident:latest","size":1024,"size_vram":0}"#);
    let indefinite_adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_reference_time(FIXED_NOW_MS)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&indefinite));
    indefinite_adapter
        .observe_loaded(&indefinite, 1_000)
        .expect("parses");
    assert!(indefinite_adapter.unload_targets().is_empty());
    assert!(indefinite_adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .is_none());
}

#[test]
fn models_are_released_one_at_a_time_largest_first_and_an_unloaded_one_is_a_no_op() {
    let adapter = observing(&two_models(), 1_000);
    let first = adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .expect("the largest model first");
    assert!(first
        .request
        .body
        .as_deref()
        .is_some_and(|body| body.contains("llama3:8b")));

    // The release is confirmed, so the model list no longer holds it.
    // An expiry the adapter cannot read is also treated as a pin, so an
    // unfamiliar format never becomes a release.
    let unreadable =
        ps_response(r#"{"name":"odd:latest","size":1024,"size_vram":0,"expires_at":"soon"}"#);
    let unreadable_adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_reference_time(FIXED_NOW_MS)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&unreadable));
    unreadable_adapter
        .observe_loaded(&unreadable, 1_000)
        .expect("parses");
    assert!(unreadable_adapter.unload_targets().is_empty());
    assert!(unreadable_adapter.release_refusals()[0]
        .1
        .contains("could not be read"));

    let remaining = ps_response(
        r#"{"name":"nomic-embed-text","model":"nomic-embed-text","size":2147483648,"size_vram":0,"expires_at":"2026-10-11T15:30:00Z"}"#,
    );
    adapter.observe_loaded(&remaining, 2_000).expect("parses");
    let second = adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .expect("the next model is offered");
    assert!(second
        .request
        .body
        .as_deref()
        .is_some_and(|body| body.contains("nomic-embed-text")));

    // Nothing left to release: asking again is a no-op, not a write.
    let empty = ps_response("");
    adapter.observe_loaded(&empty, 3_000).expect("parses");
    assert!(adapter
        .operation(ActionKind::UnloadIdleModel, &workload())
        .is_none());
    assert!(adapter.unload_targets().is_empty());
}

#[test]
fn reported_requests_prove_idleness_when_tracking_is_complete() {
    // Without tracking, a resident model is active and relief is refused.
    let resident = two_models();
    let untracked = observing(&resident, 0);
    assert_eq!(untracked.activity(&workload()), ActivityEvidence::Active);

    // With complete tracking, the same resident model can be idle: a gateway
    // saw the requests and none is in flight.
    let tracked = observing(&resident, 0).with_tracking(TrackingCompleteness::Complete, 1_000);
    assert_eq!(
        tracked.activity(&workload()),
        ActivityEvidence::Idle { since_ms: 1_000 },
        "resident but unused is the state worth relieving"
    );

    let lease = tracked.begin_request(5_000);
    assert_eq!(
        tracked.activity(&workload()),
        ActivityEvidence::Active,
        "a request in flight is never idle"
    );
    tracked.end_request(lease, 6_000);
    assert_eq!(
        tracked.activity(&workload()),
        ActivityEvidence::Idle { since_ms: 6_000 }
    );

    // A cancelled stream releases its lease too.
    let lease = tracked.begin_request(7_000);
    tracked.cancel_request(lease, 7_500);
    assert_eq!(
        tracked.activity(&workload()),
        ActivityEvidence::Idle { since_ms: 7_500 }
    );

    // And the adapter still refuses relief once tracking says busy, even though
    // the model list is unchanged.
    let lease = tracked.begin_request(8_000);
    assert_eq!(tracked.activity(&workload()), ActivityEvidence::Active);
    tracked.end_request(lease, 9_000);
}

#[test]
fn the_adapter_reports_its_provider_and_version() {
    let adapter = observing(&two_models(), 1_000);
    assert_eq!(adapter.kind(), ProviderKind::Ollama);
    assert_eq!(adapter.endpoint(), "http://127.0.0.1:11434");
    assert_eq!(adapter.name(), "ollama");
    assert!(adapter.owns(&workload()));
    assert!(!adapter.owns(&Workload {
        id: WorkloadId {
            scope: "unit:llamacpp.service".to_string(),
            generation: 1,
        },
        name: "llama.cpp".to_string(),
        pids: vec![1],
        accounting: MemoryAccounting::RssEstimate {
            rss_bytes: 1024,
            processes: 1,
        },
        control: ControlLevel::ObserveOnly,
        limit_bytes: None,
    }));

    let observed = adapter
        .observe_version(&Response::new(200, r#"{"version":"0.12.6"}"#), 2_000)
        .expect("a version");
    assert_eq!(observed.as_deref(), Some("0.12.6"));
    assert_eq!(adapter.version().as_deref(), Some("0.12.6"));

    // A version endpoint that answers nonsense is reported, not guessed.
    assert!(adapter
        .observe_version(&Response::new(200, "plain text"), 2_500)
        .is_err());
}

#[test]
fn an_integrated_gpu_model_is_attributed_to_host_memory() {
    // On an iGPU the model's reported device bytes are a carve-out of host RAM,
    // so the whole model is host memory and there is no separate device figure.
    // Reporting it as device memory would hide where it really lives and could
    // let a relief decision treat the host as holding less than it does.
    let response = two_models();
    let adapter = OllamaAdapter::new("ollama", "http://127.0.0.1:11434", SCOPE)
        .with_reference_time(FIXED_NOW_MS)
        .with_shared_device_memory(true)
        .with_capabilities(OllamaAdapter::capabilities_from_probes(&response));
    adapter
        .observe_loaded(&response, 1_000)
        .expect("the recorded body parses");
    let inventory = adapter.inventory();
    let gpu = inventory
        .iter()
        .find(|resource| resource.name == "llama3:8b")
        .expect("the GPU model");
    assert_eq!(
        gpu.host_ram_bytes,
        Some(4_294_967_296),
        "the whole model is host memory on an iGPU"
    );
    assert_eq!(
        gpu.device_ram_bytes, None,
        "there is no separate device memory on an iGPU"
    );
}
