from datetime import datetime
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from autorx.ozimux import OziUploader


def sample_telemetry(frame):
    return {
        "frame": frame,
        "id": "TEST1234",
        "datetime": "2026-09-25T00:00:00Z",
        "lat": -34.0,
        "lon": 138.0,
        "alt": 1000,
        "temp": -10.0,
        "type": "RS41",
        "freq": "401.520 MHz",
        "freq_float": 401.52,
        "datetime_dt": datetime(2026, 9, 25, 0, 0, frame % 60),
    }


def wait_for(condition, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


class TestOziUploader(unittest.TestCase):
    def test_payload_summary_requires_positive_update_rate(self):
        with self.assertRaises(ValueError):
            OziUploader(payload_summary_port=55672, update_rate=0)

    def test_payload_summary_throttles_and_keeps_latest(self):
        sent = []
        original_send = OziUploader.send_payload_summary

        def fake_send(self, telemetry):
            sent.append((telemetry["frame"], time.time()))

        OziUploader.send_payload_summary = fake_send
        uploader = OziUploader(payload_summary_port=55672, update_rate=0.2)

        try:
            uploader.add(sample_telemetry(1))
            self.assertTrue(wait_for(lambda: len(sent) == 1))
            self.assertEqual(sent[0][0], 1)

            uploader.add(sample_telemetry(2))
            uploader.add(sample_telemetry(3))

            self.assertTrue(wait_for(lambda: len(sent) == 2))
            self.assertEqual([entry[0] for entry in sent], [1, 3])
            self.assertGreaterEqual(sent[1][1] - sent[0][1], 0.18)
            time.sleep(0.3)
            self.assertEqual([entry[0] for entry in sent], [1, 3])
        finally:
            uploader.close()
            OziUploader.send_payload_summary = original_send


if __name__ == "__main__":
    unittest.main()
