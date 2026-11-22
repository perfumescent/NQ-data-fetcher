"""Deterministic source/HTTP/recovery boundary tests; no production or external requests."""

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import requests

from src.jobs.etf_history import ETFHistoryJob, ETFHistoryPolicy, eligible_etfs, source_history_item
from src.providers.api_server_client import APIServerClient
from src.providers.eastmoney_nav import fetch_nav_table
from src.providers.etf_history import HistorySourceResult, fetch_source_table, serialize_sdk_table


def table_for(symbol="159696", dataset="price-history") -> dict:
    """Build one source-shaped fixture with valid complete SDK metadata.

    Args:
        symbol: Six-digit fixture ETF code; dataset: price-history or unit-nav-history.
    Returns:
        Complete source table containing one dated row; never used as production evidence.

    Created: 2026-10
    易错点: Fixtures exercise transmission, not business price/NAV validity or live source coverage.
    """
    return serialize_sdk_table(pd.DataFrame({"日期": ["2026-10-21"], "收盘": [1.5]}),
                               symbol=symbol, dataset=dataset, request_params={})


def response_for(rows: list[dict], total: int) -> Mock:
    """Wrap a source JSON fixture in the minimal requests response boundary.

    Args:
        rows: Native source record dictionaries; total: Advertised pagination record count.
    Returns:
        Mock HTTP response preserving all supplied native JSON values.

    Created: 2026-10
    易错点: Deliberately inconsistent total values are allowed so incomplete-page tests can reject them.
    """
    response = Mock()
    response.json.return_value = {"ErrCode": 0, "Data": {"LSJZList": rows, "newEnvelope": "kept"},
                                  "TotalCount": total, "PageSize": 200}
    return response


class SourceTableTests(unittest.TestCase):
    def test_complete_sdk_table_preserves_rows_columns_index_and_offsets(self):
        """Check lossless JSON-necessary serialization, including duplicate and missing source data.

        Args:
            self: Isolated unittest case; no source/server requests.
        Returns:
            None; assertions fail if any row, field, duplicate index or offset is changed.

        Created: 2026-10
        易错点: NaN/Infinity become null without deleting their rows; an index is source data too.
        """
        stamp = datetime.fromisoformat("2026-10-21T15:00:00+08:00")
        frame = pd.DataFrame([[date(2026, 10, 21), np.nan, np.inf, stamp, "unknown"],
                              [None, -1.0, None, stamp, "retained"]],
                             columns=["日期", "收盘", "成交量", "源时刻", "未来新增列"], index=[7, 7])
        original = frame.copy(deep=True)
        result = serialize_sdk_table(frame, symbol="159696", dataset="price-history",
                                     request_params={"adjust": "", "newParameter": np.int64(2)},
                                     fetched_at=stamp)
        self.assertEqual(result["columns"], frame.columns.tolist())
        self.assertEqual(result["index"], [7, 7])
        self.assertEqual(len(result["data"]), 2)
        self.assertEqual(result["data"][0], ["2026-10-21", None, None, stamp.isoformat(), "unknown"])
        self.assertIsNone(result["data"][1][0])
        self.assertEqual(result["requestParams"], {"adjust": "", "newParameter": 2})
        self.assertEqual(result["sourceLayer"], "sdk-table")
        json.dumps(result, allow_nan=False)
        pd.testing.assert_frame_equal(original, frame)

    def test_sina_price_fallback_is_complete_and_identified(self):
        """Verify a price-source failure uses only an accurately labelled independent price source.

        Args:
            self: Isolated unittest case with injected SDK tables.
        Returns:
            None; checks source identity, exact SDK request and complete out-of-window rows.

        Created: 2026-10
        易错点: Sina has no requested date bounds; old rows must survive rather than UI clipping.
        """
        frame = pd.DataFrame({"date": [date(2020, 1, 1), date(2026, 10, 21)],
                              "close": [None, 1.2], "postVol": [3, 4], "new": ["a", "b"]})
        sdk = Mock(side_effect=[OSError("offline"), frame])
        result = fetch_source_table("159696", "price-history", "20261001", "20261023", sdk_call=sdk)
        self.assertEqual(result.table["provider"], "sina")
        self.assertEqual(result.table["schema"], "sina-fund-history-v1")
        self.assertEqual(result.table["sdk"]["function"], "fund_etf_hist_sina")
        self.assertEqual(result.table["requestParams"], {"symbol": "sz159696"})
        self.assertEqual(len(result.table["data"]), 2)
        self.assertIsNone(result.table["data"][0][1])
        self.assertEqual(sdk.call_args_list[1].args[0], "fund_etf_hist_sina")

    def test_failed_nav_never_calls_price_or_sdk(self):
        """Verify unavailable NAV supplies no substitute source or clearing update.

        Args:
            self: Isolated unittest case with a failing HTTP paginator.
        Returns:
            None; source table remains None and price/SDK boundaries are never invoked.

        Created: 2026-10
        易错点: A good price does not make a NAV response successful and cannot provide its denominator.
        """
        sdk = Mock()
        result = fetch_source_table("159696", "unit-nav-history", "19700101", "20261023",
                                    sdk_call=sdk, nav_fetch=Mock(side_effect=TimeoutError()))
        self.assertIsNone(result.table)
        self.assertEqual(result.error, "TimeoutError")
        sdk.assert_not_called()

    def test_price_failures_return_no_nav_fallback(self):
        """Verify both price sources failing does not call NAV or fabricate a price.

        Args:
            self: Isolated unittest case with two failing SDK price sources.
        Returns:
            None; source result records failure, with no raw table.

        Created: 2026-10
        易错点: Empty price tables and unavailable prices are distinct from valid zero/null rows.
        """
        nav = Mock()
        result = fetch_source_table("159696", "price-history", "19700101", "20261023",
                                    sdk_call=Mock(side_effect=[OSError(), OSError()]), nav_fetch=nav)
        self.assertIsNone(result.table)
        self.assertIn("eastmoney:OSError", result.error)
        self.assertIn("sina:OSError", result.error)
        nav.assert_not_called()

    def test_complete_http_nav_keeps_native_unknown_fields_and_pages(self):
        """Verify paginated JSON NAV data survives unknown columns, null rows and unparsed dates.

        Args:
            self: Isolated unittest case with two HTTP pages and changing record key sets.
        Returns:
            None; all rows/columns/request bounds/envelope fields remain in the source table.

        Created: 2026-10
        易错点: Invalid FSRQ and empty DWJZ are raw facts; only Server can exclude them from pairing.
        """
        get = Mock(side_effect=[response_for([{"FSRQ": "bad-date", "DWJZ": None, "FHSP": "new"}], 2),
                                response_for([{"FSRQ": "2026-10-21", "DWJZ": "1.8000", "extra": 5}], 2)])
        table = fetch_nav_table("159696", "19700101", "20261023", get=get)
        self.assertEqual(table["columns"], ["FSRQ", "DWJZ", "FHSP", "extra"])
        self.assertEqual(table["data"], [["bad-date", None, "new", None], ["2026-10-21", "1.8000", None, 5]])
        self.assertEqual(table["index"], [[1, 0], [2, 0]])
        self.assertEqual(table["sourceLayer"], "upstream-json-records")
        self.assertEqual(table["requestParams"]["pages"][1]["pageIndex"], 2)
        self.assertEqual(table["sourcePageMetadata"][0]["Data"], {"newEnvelope": "kept"})
        self.assertEqual(get.call_args_list[0].kwargs["params"]["startDate"], "1970-01-01")

    def test_incomplete_nav_page_never_returns_partial_success(self):
        """Reject truncated source pagination before constructing an ingestible table.

        Args:
            self: Isolated unittest case with one record followed by an empty incomplete page.
        Returns:
            None; expected source error is raised.

        Created: 2026-10
        易错点: A valid first page does not prove the remaining history was obtained.
        """
        get = Mock(side_effect=[response_for([{"FSRQ": "2026-10-21", "DWJZ": "1.8"}], 2),
                                response_for([], 2)])
        with self.assertRaisesRegex(ValueError, "incomplete"):
            fetch_nav_table("159696", "19700101", "20261023", get=get)

    def test_changed_source_total_requires_complete_retry(self):
        """Reject a moving pagination snapshot so a newly inserted row cannot be silently skipped.

        Args:
            self: Isolated unittest case with inconsistent advertised source totals.
        Returns:
            None; expected source error is raised.

        Created: 2026-10
        易错点: Requests occur at different instants; a source update during pagination needs retry.
        """
        get = Mock(side_effect=[response_for([{"FSRQ": "2026-10-21"}], 2),
                                response_for([{"FSRQ": "2026-10-20"}], 3)])
        with self.assertRaisesRegex(ValueError, "changed"):
            fetch_nav_table("159696", "19700101", "20261023", get=get)


class RecoveryTests(unittest.TestCase):
    def test_existing_minute_plan_cannot_reach_history_adapter(self):
        """Exercise the real existing ETF scheduler with its minute plan and fake quote/meta sources.

        Args:
            self: Test case with an eligible verified ETF and patched external source boundaries.
        Returns:
            None; no complete history adapter or legacy history source is called.

        Created: 2026-10
        易错点: Adding a background history worker must not enable histories in the minute quote job.
        """
        from src.jobs.schedulers import RawJobScheduler
        item = {"code": "159696", "premiumEligible": True, "inceptionDate": "2023-08-17"}
        with patch("src.jobs.schedulers.AkShareProvider.get_etf_realtime", return_value=None), \
             patch("src.jobs.schedulers.AkShareProvider.get_fund_metadata", return_value={}), \
             patch("src.jobs.schedulers.fetch_backfill_items") as history, \
             patch("src.jobs.schedulers.AkShareProvider.get_etf_history_sina") as legacy:
            _, _, rows = RawJobScheduler._fetch_etf_raw(item, RawJobScheduler.incremental_plan())
        self.assertEqual(rows, [])
        history.assert_not_called()
        legacy.assert_not_called()

    def make_job(self, fetch_source=None, push_history=None, policy=None) -> ETFHistoryJob:
        """Construct a job with deterministic source/config/HTTP boundaries.

        Args:
            self: Test case; fetch_source/push_history: optional injectable callables; policy:
                optional acquisition policy, or enabled defaults with a single worker.
        Returns:
            New job; source calls return successful complete fixture tables by default.

        Created: 2026-10
        易错点: A new instance must start with no acknowledged history, even after prior tests.
        """
        source = fetch_source or Mock(side_effect=lambda code, dataset, *args, **kwargs:
                                      HistorySourceResult(code, dataset, table_for(code, dataset)))
        return ETFHistoryJob(policy or ETFHistoryPolicy(enabled=True, workers=1),
                             load_config=Mock(return_value={"etf": [{"code": "159696", "premiumEligible": True}]}),
                             fetch_source=source, push_history=push_history or Mock(return_value=True))

    def test_startup_overlap_reconcile_and_restart(self):
        """Verify explicit startup recovery, hourly guards, overlap refresh and weekly reconciliation.

        Args:
            self: Test case; controlled clock crosses an outage and weekly source revision boundary.
        Returns:
            None; only source/HTTP fixture boundaries are invoked.

        Created: 2026-10
        易错点: A restarted process never trusts a lost watermark; missing collection days are recoverable.
        """
        job = self.make_job()
        now = datetime.fromisoformat("2026-10-23T00:00:00+08:00")
        self.assertEqual(job.run_due(now)["ingested"], 2)
        self.assertEqual({call.args[2] for call in job.fetch_source.call_args_list}, {"19700101"})
        self.assertEqual(job.run_due(now + timedelta(minutes=1))["attempted"], 0)
        job.fetch_source.reset_mock()
        self.assertEqual(job.run_due(now + timedelta(days=2))["ingested"], 2)
        self.assertEqual({call.args[2] for call in job.fetch_source.call_args_list}, {"20261011"})
        job.fetch_source.reset_mock()
        job.run_due(now + timedelta(days=7))
        self.assertEqual({call.args[2] for call in job.fetch_source.call_args_list}, {"19700101"})
        restarted = self.make_job()
        restarted.run_due(now + timedelta(days=20))
        self.assertEqual({call.args[2] for call in restarted.fetch_source.call_args_list}, {"19700101"})

    def test_http_failure_keeps_full_recovery_pending(self):
        """Verify HTTP failure does not acknowledge a full source-history recovery.

        Args:
            self: Test case with source success and first ingest failure.
        Returns:
            None; failed dataset retries full history while successful dataset uses overlap.

        Created: 2026-10
        易错点: Fetch success is insufficient: only successful HTTP ingest advances source acknowledgement.
        """
        push = Mock(side_effect=[False, True, True, True])
        job = self.make_job(push_history=push)
        now = datetime(2026, 10, 23, tzinfo=timezone.utc)
        self.assertEqual(job.run_due(now)["failed"], 1)
        first_failed = push.call_args_list[0].args[0]["items"][0]["rawPayload"]["histories"][0]["dataset"]
        job.fetch_source.reset_mock()
        job.run_due(now + timedelta(hours=1))
        bounds = {call.args[1]: call.args[2] for call in job.fetch_source.call_args_list}
        self.assertEqual(bounds[first_failed], "19700101")
        self.assertEqual(set(bounds.values()), {"19700101", "20261009"})

    def test_source_failure_does_not_clear_successful_other_dataset(self):
        """Verify partially available source results are independently transmitted and retried.

        Args:
            self: Test case with permanently unavailable NAV and successful prices.
        Returns:
            None; only price items cross HTTP and NAV remains a full-history retry.

        Created: 2026-10
        易错点: No empty NAV item is sent, so an earlier successful NAV dataset cannot be erased.
        """
        source = Mock(side_effect=lambda code, dataset, *args, **kwargs:
                      HistorySourceResult(code, dataset, None, "TimeoutError") if dataset == "unit-nav-history"
                      else HistorySourceResult(code, dataset, table_for(code, dataset)))
        job = self.make_job(fetch_source=source)
        now = datetime(2026, 10, 23, tzinfo=timezone.utc)
        self.assertEqual(job.run_due(now), {"attempted": 2, "ingested": 1, "failed": 1})
        self.assertEqual(job.push_history.call_count, 1)
        self.assertEqual(job.push_history.call_args.args[0]["items"][0]["rawPayload"]["histories"][0]["dataset"], "price-history")
        source.reset_mock()
        job.run_due(now + timedelta(hours=1))
        self.assertEqual(next(call.args[2] for call in source.call_args_list if call.args[1] == "unit-nav-history"), "19700101")

    def test_collection_date_is_shanghai_and_source_date_is_untouched(self):
        """Verify collection-day envelope never claims to be a source valuation date.

        Args:
            self: Test case with a UTC clock crossing Shanghai midnight.
        Returns:
            None; source date stays old and collection date is the actual Shanghai date.

        Created: 2026-10
        易错点: updatedAt stays None when source update time is unknown; fetchedAt is separate metadata.
        """
        job = self.make_job()
        job.run_due(datetime.fromisoformat("2026-10-22T18:00:00+00:00"))
        item = job.push_history.call_args.args[0]["items"][0]
        self.assertEqual(item["date"], "2026-10-23")
        self.assertIsNone(item["updatedAt"])
        self.assertEqual(item["rawPayload"]["histories"][0]["data"][0][0], "2026-10-21")

    def test_feature_disabled_and_unverified_config_do_no_work(self):
        """Verify opt-in scheduling and verified existing universe boundaries.

        Args:
            self: Test case with LOF/unverified/duplicate ETF config entries.
        Returns:
            None; disabled jobs make no source requests and strict eligibility excludes other products.

        Created: 2026-10
        易错点: String true and integer 1 do not prove a verified ETF classification.
        """
        job = self.make_job(policy=ETFHistoryPolicy())
        self.assertEqual(job.run_due()["attempted"], 0)
        job.fetch_source.assert_not_called()
        rows = [{"code": "161128"}, {"code": "159660", "premiumEligible": "true"},
                {"code": "159696", "premiumEligible": True}, {"code": "159696", "premiumEligible": True},
                {"code": "159632", "premiumEligible": 1}]
        self.assertEqual([item["code"] for item in eligible_etfs({"etf": rows})], ["159696"])

    def test_policy_rejects_minute_scale_history(self):
        """Verify policy validation prevents accidental minute-scale history acquisition.

        Args:
            self: Test case with an invalid minute interval.
        Returns:
            None; invalid policy raises ValueError before any work is scheduled.

        Created: 2026-10
        易错点: Quote loop frequency is unrelated to source history retention/revision frequency.
        """
        with self.assertRaises(ValueError):
            replace(ETFHistoryPolicy(), interval_seconds=60).validate()

    def test_http_source_tables_are_sent_individually(self):
        """Verify complete history tables are never bundled into oversized legacy-sized batches.

        Args:
            self: Test case with two complete tables and a legacy point.
        Returns:
            None; verifies every original item is sent once without source field/row truncation.

        Created: 2026-10
        易错点: Chunking is a transport concern; only Server partitions source rows into dates.
        """
        first = source_history_item(table_for(), "2026-10-23")
        second = source_history_item(table_for(dataset="unit-nav-history"), "2026-10-23")
        legacy = {"symbol": "161128", "date": "2026-10-21", "rawPayload": {"t": "2026-10-21", "p": 1.2}}
        with patch.object(APIServerClient, "_post_raw", return_value=True) as post:
            self.assertTrue(APIServerClient.push_raw_history({"items": [first, legacy, second]}))
        self.assertEqual([call.args[1]["items"] for call in post.call_args_list], [[first], [legacy], [second]])

    def test_complete_table_ingest_uses_configured_timeout_and_legacy_keeps_twenty(self):
        """Verify large table HTTP requests have their own timeout without changing old raw slots.

        Args:
            self: Test case with mixed source/legacy history and a finite positive configured timeout.
        Returns:
            None; verifies the exact requests timeout at every transport boundary.

        Created: 2026-10
        易错点: A full table can fan out into thousands of Server SQL operations; old quote/legacy
            calls must retain their original 20-second behavior.
        """
        table = source_history_item(table_for(), "2026-10-23")
        legacy = {"symbol": "161128", "date": "2026-10-21", "rawPayload": {"p": 1.2}}
        response = Mock(status_code=200)
        with patch.dict("os.environ", {"ETF_HISTORY_INGEST_TIMEOUT_SECONDS": "240.5"}), \
             patch("src.providers.api_server_client.requests.post", return_value=response) as post:
            self.assertTrue(APIServerClient.push_raw_history({"items": [table, legacy]}))
            self.assertEqual([call.kwargs["timeout"] for call in post.call_args_list], [240.5, 20])
            APIServerClient.push_raw_realtime({"items": []})
            self.assertEqual(post.call_args.kwargs["timeout"], 20)

    def test_complete_table_default_timeout_is_180_and_invalid_values_fail_before_network(self):
        """Verify defaults and reject zero, negative, non-finite or malformed history timeouts.

        Args:
            self: Test case with a source table and isolated environment configuration.
        Returns:
            None; invalid values return False without network calls; unset defaults to 180 seconds.

        Created: 2026-10
        易错点: Invalid history configuration cannot turn an acknowledged recovery into success,
            and must not affect legacy-only requests that do not need the new setting.
        """
        item = source_history_item(table_for(), "2026-10-23")
        with patch.dict("os.environ", {}, clear=True), \
             patch("src.providers.api_server_client.requests.post", return_value=Mock(status_code=200)) as post:
            self.assertTrue(APIServerClient.push_raw_history({"items": [item]}))
            self.assertEqual(post.call_args.kwargs["timeout"], 180)
        for value in ("0", "-1", "nan", "inf", "not-a-number"):
            with self.subTest(value=value), patch.dict("os.environ", {"ETF_HISTORY_INGEST_TIMEOUT_SECONDS": value}), \
                 patch("src.providers.api_server_client.requests.post") as post:
                self.assertFalse(APIServerClient.push_raw_history({"items": [item]}))
                post.assert_not_called()

    def test_non_200_and_request_exception_do_not_acknowledge_ingest(self):
        """Verify rejected or timed-out full-table writes remain retryable failures.

        Args:
            self: Test case with HTTP 503 and requests Timeout transport failures.
        Returns:
            None; both source-table writes return False and preserve recovery retry semantics.

        Created: 2026-10
        易错点: A timeout may occur after Server committed raw; callers must not clear prior data
            or assume the request was rolled back, and later ingest remains idempotent.
        """
        item = source_history_item(table_for(), "2026-10-23")
        with patch("src.providers.api_server_client.requests.post", return_value=Mock(status_code=503, text="unavailable")):
            self.assertFalse(APIServerClient.push_raw_history({"items": [item]}))
        with patch("src.providers.api_server_client.requests.post", side_effect=requests.exceptions.Timeout()):
            self.assertFalse(APIServerClient.push_raw_history({"items": [item]}))


if __name__ == "__main__":
    unittest.main()
