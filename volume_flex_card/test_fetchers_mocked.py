"""Deterministic, network-free tests for all fetchers using unittest.mock.

Validates parsing, absolute notional calculation, unique trade IDs, and that
the HTTP layer is mocked (no real API calls).
"""

import unittest
from unittest.mock import patch, Mock

# Flexible imports: support running from project root (package imports)
# or from within the module directory (local imports)
try:
    from volume_flex_card.fetch_hyperliquid import fetch_user_fills as hl_fetch_user_fills, normalize_trade as hl_normalize_trade
    from volume_flex_card.fetch_dydx import fetch_fills as dydx_fetch_fills, normalize_fill as dydx_normalize_fill
    from volume_flex_card.fetch_gmx import fetch_trades as gmx_fetch_trades, normalize_trade as gmx_normalize_trade
    from volume_flex_card.fetch_drift import fetch_trades as drift_fetch_trades, normalize_trade as drift_normalize_trade
except ImportError:
    from fetch_hyperliquid import fetch_user_fills as hl_fetch_user_fills, normalize_trade as hl_normalize_trade
    from fetch_dydx import fetch_fills as dydx_fetch_fills, normalize_fill as dydx_normalize_fill
    from fetch_gmx import fetch_trades as gmx_fetch_trades, normalize_trade as gmx_normalize_trade
    from fetch_drift import fetch_trades as drift_fetch_trades, normalize_trade as drift_normalize_trade


class TestFetchersMocked(unittest.TestCase):
    @patch("requests.request")
    def test_hyperliquid_mock(self, mock_request):
        # Fixture: 3 fills
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: [
                {"coin": "BTC", "px": "50000", "sz": "1.0", "side": "B", "time": 1681247412000, "tid": 101},
                {"coin": "ETH", "px": "3000", "sz": "-0.5", "side": "A", "time": 1681247412100, "tid": 102},
                {"coin": "SOL", "px": 25, "sz": 2.0, "side": "B", "time": 1681247412200, "tid": 103},
            ],
            raise_for_status=lambda: None,
        )

        wallet = "0xabc"
        raw = hl_fetch_user_fills(wallet)
        self.assertEqual(len(raw), 3)

        trades = [hl_normalize_trade(r, wallet) for r in raw]
        self.assertEqual(len(trades), 3)

        # Unique trade IDs
        tids = [t["trade_id"] for t in trades]
        self.assertEqual(len(set(tids)), len(tids))

        # Absolute notional checks
        for t in trades:
            price = float(t["price"])  # px
            size = float(t["size"])   # sz
            notional = float(t["notional_value"])
            self.assertNotEqual(price, 0.0)
            self.assertNotEqual(size, 0.0)
            self.assertIsInstance(notional, (int, float))
            self.assertGreaterEqual(notional, 0.0)
            computed = abs(price * size)
            self.assertLess(abs(notional - computed), 0.01, f"Notional mismatch: {notional} vs {computed}")

        # Verify mock was used and method was POST
        self.assertTrue(mock_request.called)
        args, kwargs = mock_request.call_args
        self.assertEqual(args[0], "POST")

    @patch("requests.request")
    def test_dydx_mock(self, mock_request):
        # Fixture: 2 fills
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: {
                "fills": [
                    {"id": "f1", "market": "BTC-USD", "side": "BUY", "price": "50000", "size": "0.1", "createdAt": "2023-04-12T12:10:00Z"},
                    {"id": "f2", "market": "ETH-USD", "side": "SELL", "price": "3000", "size": "-0.5", "createdAt": "2023-04-12T12:11:00Z"},
                ]
            },
            raise_for_status=lambda: None,
        )

        addr = "0xdef"
        raw = dydx_fetch_fills(addr, subaccount=0, limit=10)
        self.assertEqual(len(raw), 2)

        trades = [dydx_normalize_fill(r, addr) for r in raw]
        self.assertEqual(len(trades), 2)

        tids = [t["trade_id"] for t in trades]
        self.assertEqual(len(set(tids)), len(tids))

        for t in trades:
            price = float(t["price"])  # price
            size = float(t["size"])   # size
            notional = float(t["notional_value"])
            self.assertNotEqual(price, 0.0)
            self.assertNotEqual(size, 0.0)
            self.assertIsInstance(notional, (int, float))
            self.assertGreaterEqual(notional, 0.0)
            computed = abs(price * size)
            self.assertLess(abs(notional - computed), 0.01, f"Notional mismatch: {notional} vs {computed}")

        self.assertTrue(mock_request.called)
        args, kwargs = mock_request.call_args
        self.assertEqual(args[0], "GET")

    @patch("requests.request")
    def test_gmx_mock(self, mock_request):
        # Fixture: 3 trades (GraphQL format)
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: {
                "data": {
                    "trades": [
                        {"id": "t1", "account": "0xabc", "marketAddress": "0xM1", "sizeInUsd": "1000", "sizeInTokens": "0.05", "executionPrice": "20000", "timestamp": 1681247412},
                        {"id": "t2", "account": "0xabc", "marketAddress": "0xM2", "sizeInUsd": "500",  "sizeInTokens": "-0.1", "executionPrice": "5000",  "timestamp": 1681247413},
                        {"id": "t3", "account": "0xabc", "marketAddress": "0xM3", "sizeInUsd": "250",  "sizeInTokens":  "2.0", "executionPrice":  "125",  "timestamp": 1681247414},
                    ]
                }
            },
            raise_for_status=lambda: None,
        )

        account = "0xabc"
        raw = gmx_fetch_trades(account, limit=10)
        self.assertEqual(len(raw), 3)

        trades = [gmx_normalize_trade(r, account) for r in raw]
        self.assertEqual(len(trades), 3)

        tids = [t["trade_id"] for t in trades]
        self.assertEqual(len(set(tids)), len(tids))

        for t in trades:
            price = float(t["price"])  # executionPrice
            size = float(t["size"])   # sizeInTokens
            notional = float(t["notional_value"])  # abs(price * size)
            self.assertNotEqual(price, 0.0)
            self.assertNotEqual(size, 0.0)
            self.assertIsInstance(notional, (int, float))
            self.assertGreaterEqual(notional, 0.0)
            computed = abs(price * size)
            self.assertLess(abs(notional - computed), 0.01, f"Notional mismatch: {notional} vs {computed}")
            # Ensure sells (size < 0) have non-negative notional
            side = t.get("side", "")
            if isinstance(side, str) and side.lower() == "sell":
                self.assertGreaterEqual(notional, 0.0)

        self.assertTrue(mock_request.called)
        args, kwargs = mock_request.call_args
        self.assertEqual(args[0], "POST")

    @patch("requests.request")
    def test_drift_mock(self, mock_request):
        # Drift returns quoteAssetAmount possibly in micro-units; we test 3 records
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: {
                "success": True,
                "records": [
                    {"ts": 1681247412, "marketIndex": 0, "marketType": "perp", "baseAssetAmount": "0.1", "quoteAssetAmount": 50000000, "price": "50000", "side": "buy"},
                    {"ts": 1681247413, "marketIndex": 1, "marketType": "perp", "baseAssetAmount": "-0.2", "quoteAssetAmount": -75000000, "price": "37500", "side": "sell"},
                    {"ts": 1681247414, "marketIndex": 2, "marketType": "perp", "baseAssetAmount": "1.0", "quoteAssetAmount": "1250000", "price": "1250", "side": "buy"},
                ]
            },
            raise_for_status=lambda: None,
        )

        account = "So1anaPubKey"
        raw = drift_fetch_trades(account)
        self.assertEqual(len(raw), 3)

        trades = [drift_normalize_trade(r, account) for r in raw]
        self.assertEqual(len(trades), 3)

        tids = [t["trade_id"] for t in trades]
        self.assertEqual(len(set(tids)), len(tids))

        # Drift-specific: notional_value comes from quoteAssetAmount normalization
        expected_notionals = [50.0, 75.0, 1.25]  # abs(quote)/1e6
        for (t, expected) in zip(trades, expected_notionals):
            notional = float(t["notional_value"])
            self.assertIsInstance(notional, (int, float))
            self.assertGreaterEqual(notional, 0.0)
            self.assertLess(abs(notional - expected), 0.01)
            # Sells should still be non-negative
            side = t.get("side", "")
            if isinstance(side, str) and side.lower() == "sell":
                self.assertGreaterEqual(notional, 0.0)

        self.assertTrue(mock_request.called)
        args, kwargs = mock_request.call_args
        self.assertEqual(args[0], "GET")


if __name__ == "__main__":
    unittest.main()