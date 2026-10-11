//! A gated real-hardware suite that reports skips honestly (RAM-29).
//!
//! The CPU-only suite must never need a model runtime. This gate probes for one
//! and says, in its output, whether each real-hardware suite can run or is
//! skipped. A missing runtime is a reported skip, never a silent pass, which is
//! what the card asks for.

mod support;

#[test]
fn real_hardware_availability_is_reported_not_assumed() {
    let llama = support::have("llama-server");
    let vllm = support::have("vllm");
    if llama {
        eprintln!("hardware: llama-server is present, so the llama.cpp suite can run when a model is configured");
    } else {
        eprintln!("skipped: llama.cpp hardware suite (no llama-server on PATH)");
    }
    if vllm {
        eprintln!("hardware: vllm is present");
    } else {
        eprintln!("skipped: vLLM hardware suite (no vllm on PATH)");
    }
    // The point of the gate is the honest report above; there is nothing to fail
    // when the hardware is simply absent.
}
