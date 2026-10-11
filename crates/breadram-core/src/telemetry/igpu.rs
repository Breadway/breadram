//! Integrated GPU shared memory (GTT), read-only.
//!
//! On an integrated GPU the "device" memory is a carve-out of system RAM: the
//! driver maps pages into the graphics translation table (GTT) from the same
//! physical memory the kernel already accounts. It must therefore be reported
//! separately from host available memory and never folded into it, and it must
//! not be confused with dedicated VRAM on a discrete card. A model loaded on an
//! iGPU is holding host RAM, and a relief decision made on a false zero would be
//! wrong.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::fs::FileSource;

/// The drm class root, where the kernel exposes per card device attributes.
pub const DRM_ROOT: &str = "/sys/class/drm";
/// The most cards scanned, so a host with many devices stays bounded.
pub const MAX_CARDS: u32 = 16;
/// The capability name for integrated GPU shared memory telemetry.
pub const CAP_GPU_GTT: &str = "gpu_gtt";

/// One card's GTT figures, as read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GttDevice {
    /// The card name, for example `card0`.
    pub card: String,
    /// GTT bytes in use, when readable.
    pub used_bytes: Option<u64>,
    /// GTT total bytes, when readable.
    pub total_bytes: Option<u64>,
}

/// The integrated GPU shared memory on this host.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GttMemory {
    /// One entry per card that exposes a GTT counter.
    pub devices: Vec<GttDevice>,
    /// Whether the drm class could be listed at all.
    pub drm_readable: bool,
}

impl GttMemory {
    /// Summed GTT in use across cards, when at least one reported it.
    pub fn used_bytes(&self) -> Option<u64> {
        sum(self.devices.iter().filter_map(|device| device.used_bytes))
    }

    /// Summed GTT total across cards, when at least one reported it.
    pub fn total_bytes(&self) -> Option<u64> {
        sum(self.devices.iter().filter_map(|device| device.total_bytes))
    }

    /// Whether an integrated GPU was found at all.
    pub fn present(&self) -> bool {
        !self.devices.is_empty()
    }

    /// The constraints this layout puts on relief, in words.
    pub fn notes(&self) -> Vec<String> {
        if !self.drm_readable {
            return vec![
                "the drm class could not be read, so whether this host has an integrated GPU is unknown"
                    .to_string(),
            ];
        }
        if !self.present() {
            return vec![
                "no integrated GPU shared memory (GTT) counter was found, so any iGPU model footprint is unknown rather than zero"
                    .to_string(),
            ];
        }
        let mut notes = vec![format!(
            "integrated GPU shared memory (GTT) is measured: {} in use, separate from host available memory and from dedicated VRAM; a model on an iGPU is holding host RAM",
            self.used_bytes()
                .map_or_else(|| "unknown".to_string(), |bytes| format!("{bytes} bytes"))
        )];
        if self.used_bytes().is_none() {
            notes.push(
                "a card exposes a GTT total but not a used count, so the iGPU footprint stays unknown"
                    .to_string(),
            );
        }
        notes
    }
}

fn sum(values: impl Iterator<Item = u64>) -> Option<u64> {
    let mut any = false;
    let mut total: u64 = 0;
    for value in values {
        any = true;
        total = total.saturating_add(value);
    }
    any.then_some(total)
}

/// Read the GTT counters under the drm class, bounded and read-only.
pub fn collect_gtt(fs: &dyn FileSource) -> GttMemory {
    let root = Path::new(DRM_ROOT);
    let Ok(names) = fs.list_dir(root) else {
        return GttMemory {
            devices: Vec::new(),
            drm_readable: false,
        };
    };
    let mut devices = Vec::new();
    for name in names {
        let Some(index) = name
            .strip_prefix("card")
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        if index >= MAX_CARDS {
            continue;
        }
        let base = root.join(&name).join("device");
        let used_bytes = read_u64(fs, &base.join("mem_info_gtt_used"));
        let total_bytes = read_u64(fs, &base.join("mem_info_gtt_total"));
        // A card directory with no GTT counter is not an integrated GPU.
        if used_bytes.is_none() && total_bytes.is_none() {
            continue;
        }
        devices.push(GttDevice {
            card: name,
            used_bytes,
            total_bytes,
        });
    }
    GttMemory {
        devices,
        drm_readable: true,
    }
}

fn read_u64(fs: &dyn FileSource, path: &Path) -> Option<u64> {
    fs.read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::fs::test_doubles::MemoryFs;

    #[test]
    fn a_gtt_counter_is_read_and_summed_across_cards() {
        let fs = MemoryFs::new()
            .with(
                "/sys/class/drm/card0/device/mem_info_gtt_used",
                "1073741824\n",
            )
            .with(
                "/sys/class/drm/card0/device/mem_info_gtt_total",
                "4294967296\n",
            )
            .with(
                "/sys/class/drm/card1/device/mem_info_gtt_used",
                "536870912\n",
            )
            .with(
                "/sys/class/drm/card1/device/mem_info_gtt_total",
                "2147483648\n",
            );
        let gtt = collect_gtt(&fs);
        assert!(gtt.drm_readable);
        assert_eq!(gtt.devices.len(), 2);
        assert_eq!(gtt.used_bytes(), Some(1_610_612_736));
        assert_eq!(gtt.total_bytes(), Some(6_442_450_944));
        assert!(gtt.notes().iter().any(|note| note.contains("GTT")));
    }

    #[test]
    fn a_card_without_a_gtt_counter_is_not_an_integrated_gpu() {
        let fs = MemoryFs::new()
            .with("/sys/class/drm/card0/device/vendor", "0x10de\n")
            .with("/sys/class/drm/card0/device/mem_info_vram_used", "1\n");
        let gtt = collect_gtt(&fs);
        assert!(gtt.drm_readable);
        assert!(!gtt.present(), "a dedicated card is not an iGPU");
        assert!(gtt.used_bytes().is_none());
        assert!(gtt
            .notes()
            .iter()
            .any(|note| note.contains("unknown rather than zero")));
    }

    #[test]
    fn an_unreadable_drm_class_is_unknown_not_zero() {
        let fs = MemoryFs::new();
        let gtt = collect_gtt(&fs);
        assert!(!gtt.drm_readable);
        assert!(gtt.used_bytes().is_none());
        assert!(gtt.notes().iter().any(|note| note.contains("unknown")));
    }

    #[test]
    fn a_used_count_without_a_total_is_still_reported() {
        let fs = MemoryFs::new().with("/sys/class/drm/card0/device/mem_info_gtt_used", "4096\n");
        let gtt = collect_gtt(&fs);
        assert!(gtt.present());
        assert_eq!(gtt.used_bytes(), Some(4096));
        assert_eq!(gtt.total_bytes(), None);
    }
}
