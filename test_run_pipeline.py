"""Test download retry behaviour without network access or real delays."""

import unittest
from pathlib import Path
from unittest.mock import call, patch
from urllib.error import HTTPError, URLError

import run_pipeline


class DownloadRetryTests(unittest.TestCase):
    url = "https://example.invalid/month.zip"
    archive = Path("month.zip")
    month = "2024-08"

    def test_recovers_after_connection_reset(self):
        with (
            patch.object(
                run_pipeline,
                "download_zip",
                side_effect=[ConnectionResetError("connection dropped"), None],
            ) as download,
            patch.object(run_pipeline, "sleep") as sleep,
        ):
            run_pipeline.download_with_retry(self.url, self.archive, self.month)

        self.assertEqual(
            download.call_args_list, [call(self.url, self.archive)] * 2
        )
        sleep.assert_called_once_with(2)

    def test_stops_after_three_attempts(self):
        with (
            patch.object(
                run_pipeline,
                "download_zip",
                side_effect=URLError(TimeoutError("download timed out")),
            ) as download,
            patch.object(run_pipeline, "sleep") as sleep,
        ):
            with self.assertRaises(URLError):
                run_pipeline.download_with_retry(self.url, self.archive, self.month)

        self.assertEqual(
            download.call_args_list, [call(self.url, self.archive)] * 3
        )
        self.assertEqual(sleep.call_args_list, [call(2), call(4)])

    def test_http_403_passes_through_for_link_refresh(self):
        rejected = HTTPError(self.url, 403, "Forbidden", None, None)

        with (
            patch.object(run_pipeline, "download_zip", side_effect=rejected) as download,
            patch.object(run_pipeline, "sleep") as sleep,
        ):
            with self.assertRaises(HTTPError) as result:
                run_pipeline.download_with_retry(self.url, self.archive, self.month)

        self.assertIs(result.exception, rejected)
        download.assert_called_once_with(self.url, self.archive)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()