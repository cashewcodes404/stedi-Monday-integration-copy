"""Tests for monday_service.py — mock mode and service functions."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Ensure mock mode
os.environ.pop("MONDAY_API_TOKEN", None)
os.environ.pop("STEDI_API_KEY", None)

from services.monday_service import (
    is_mock_mode,
    get_order_item,
    update_claim_status,
    update_277_status,
    store_claim_pcn,
    create_claims_board_item,
    populate_era_data_on_claims_item,
    post_claim_update_to_monday,
)


class TestMockMode:
    def test_mock_mode_active(self):
        assert is_mock_mode() is True

    def test_get_order_item_mock(self):
        item = get_order_item("12345")
        assert item["id"] == "12345"
        assert item["name"] == "John TestPatient"
        assert len(item["column_values"]) > 0
        assert len(item["subitems"]) > 0

    def test_mock_order_has_subitems(self):
        item = get_order_item("99999")
        sub = item["subitems"][0]
        assert sub["name"] == "CGM Sensors"

    def test_update_claim_status_no_crash(self):
        """Mock mutations should not raise."""
        update_claim_status("12345", "Submitted")

    def test_update_277_status_no_crash(self):
        update_277_status("12345", "Accepted")

    def test_store_pcn_no_crash(self):
        store_claim_pcn("12345", "PCN_TEST", "CLAIM_001")

    def test_create_claims_board_item_returns_id(self):
        item = get_order_item("12345")
        result = create_claims_board_item(item, "CLAIM_001", "TestPayer")
        assert result  # should return a mock ID
        assert result.startswith("mock_")

    def test_populate_era_no_crash(self):
        populate_era_data_on_claims_item("mock_123", {
            "primary_paid": 450.0,
            "pr_amount": 50.0,
            "paid_date": "2026-03-15",
            "check_number": "CHK001",
        })

    def test_post_update_no_crash(self):
        post_claim_update_to_monday("12345", [
            {"claim_id": "C001", "payer": "Test", "pcn": "PCN001", "payload": {}}
        ], is_test=True)
