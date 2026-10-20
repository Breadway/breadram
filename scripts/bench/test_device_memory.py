import pathlib
import tempfile
import unittest

from device_memory import snapshot


class DeviceMemoryTests(unittest.TestCase):
    def test_missing_device_is_not_a_zero_usage_device(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(snapshot(root)["devices"], [])

    def test_absent_invalid_and_negative_counters_are_unknown(self):
        with tempfile.TemporaryDirectory() as root:
            device = pathlib.Path(root) / "card1" / "device"
            device.mkdir(parents=True)
            (device / "mem_info_gtt_used").write_text("1234\n")
            (device / "mem_info_gtt_total").write_text("invalid")
            (device / "mem_info_vram_used").write_text("-1")
            row = snapshot(root)["devices"][0]
            self.assertEqual(row["gtt_used_bytes"], 1234)
            self.assertIsNone(row["gtt_total_bytes"])
            self.assertIsNone(row["vram_used_bytes"])
            self.assertIsNone(row["vram_total_bytes"])
            self.assertEqual(row["attribution"], "whole_device_not_provider")


if __name__ == "__main__":
    unittest.main()
