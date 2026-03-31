"""
test_migration.py
==================
Comprehensive tests for the Claims Board migration.

Tests cover:
1. claims_board_config.py — routing, validation, product categories
2. routes/order_to_claims.py — New Order Board → Claims Board migration
3. claim_infrastructure.py — pre-computed value support
4. claim_builder_service.py — Claims Board item to normalized orders
5. monday_service.py — new Claims Board functions
6. monday_webhook.py — dual routing
7. stedi_webhook.py — Claims Board mode for 277/835
8. Integration tests — full flow through FastAPI
"""

import sys
import os
import json
import pytest
from copy import deepcopy
from unittest.mock import patch, MagicMock

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Ensure mock mode
os.environ.pop("MONDAY_API_TOKEN", None)
os.environ.pop("STEDI_API_KEY", None)


# ============================================================
# 1. claims_board_config.py
# ============================================================

class TestClaimsBoardConfig:
    """Test the centralized config module."""

    def test_get_submission_source_default(self):
        """Default submission source should be order_board."""
        os.environ.pop("SUBMISSION_SOURCE", None)
        from claims_board_config import get_submission_source
        assert get_submission_source() == "order_board"

    def test_get_submission_source_claims_board(self):
        """Setting SUBMISSION_SOURCE=claims_board should work."""
        os.environ["SUBMISSION_SOURCE"] = "claims_board"
        from claims_board_config import get_submission_source, is_claims_board_mode
        assert get_submission_source() == "claims_board"
        assert is_claims_board_mode() is True
        os.environ.pop("SUBMISSION_SOURCE", None)

    def test_is_claims_board_mode_default_false(self):
        """Default mode should be order_board (not claims_board)."""
        os.environ.pop("SUBMISSION_SOURCE", None)
        from claims_board_config import is_claims_board_mode
        assert is_claims_board_mode() is False

    def test_product_categories_count(self):
        """Should have exactly 5 product categories."""
        from claims_board_config import PRODUCT_CATEGORIES
        assert len(PRODUCT_CATEGORIES) == 5

    def test_product_category_names(self):
        """Product categories should match expected names."""
        from claims_board_config import PRODUCT_CATEGORIES
        names = [p["name"] for p in PRODUCT_CATEGORIES]
        assert "Insulin Pump" in names
        assert "Infusion Set" in names
        assert "Cartridge" in names
        assert "CGM Sensors" in names
        assert "CGM Monitor" in names

    def test_hcpc_to_product_mapping(self):
        """Fixed HCPC codes should map to correct products."""
        from claims_board_config import HCPC_TO_PRODUCT
        assert HCPC_TO_PRODUCT.get("E0784") == "Insulin Pump"
        assert HCPC_TO_PRODUCT.get("A4239") == "CGM Sensors"
        assert HCPC_TO_PRODUCT.get("E2103") == "CGM Monitor"

    def test_payer_dependent_hcpc_mapping(self):
        """Payer-dependent HCPC codes should map to products."""
        from claims_board_config import HCPC_TO_PRODUCT
        assert HCPC_TO_PRODUCT.get("A4224") == "Infusion Set"
        assert HCPC_TO_PRODUCT.get("A4225") == "Cartridge"
        assert HCPC_TO_PRODUCT.get("A4232") == "Cartridge"

    def test_validate_config_no_placeholders(self):
        """All column IDs should be real (no cb_/nob_ placeholders remaining)."""
        from claims_board_config import validate_claims_board_config
        issues = validate_claims_board_config()
        # All column IDs have been replaced with real Monday IDs
        placeholder_issues = [i for i in issues if "placeholder" in i.lower()]
        assert len(placeholder_issues) == 0

    def test_board_id_helpers(self):
        """Board ID helpers should read from env."""
        os.environ["MONDAY_ORDER_BOARD_ID"] = "111"
        os.environ["MONDAY_CLAIMS_BOARD_ID"] = "222"
        os.environ["MONDAY_NEW_ORDER_BOARD_ID"] = "333"
        from claims_board_config import get_order_board_id, get_claims_board_id, get_new_order_board_id
        assert get_order_board_id() == "111"
        assert get_claims_board_id() == "222"
        assert get_new_order_board_id() == "333"
        os.environ.pop("MONDAY_ORDER_BOARD_ID", None)
        os.environ.pop("MONDAY_CLAIMS_BOARD_ID", None)
        os.environ.pop("MONDAY_NEW_ORDER_BOARD_ID", None)

    def test_claims_board_parent_write_map(self):
        """Write map should be reverse of column map."""
        from claims_board_config import (
            CLAIMS_BOARD_PARENT_COLUMN_MAP,
            CLAIMS_BOARD_PARENT_WRITE_MAP,
        )
        for col_id, semantic in CLAIMS_BOARD_PARENT_COLUMN_MAP.items():
            assert CLAIMS_BOARD_PARENT_WRITE_MAP[semantic] == col_id


# ============================================================
# 2. claim_infrastructure.py — pre-computed values
# ============================================================

class TestPreComputedValues:
    """Test that build_service_line_from_normalized_order respects pre-computed values."""

    def test_pre_computed_values_used(self):
        """When pre-computed fields are present, they should be used directly."""
        from claim_infrastructure import build_service_line_from_normalized_order

        order = {
            "order_date": "20260315",
            "service_date": "20260316",
            "item": "CGM Sensors",
            "variant": "Dexcom G7",
            "quantity": "6",
            "source_child_name": "CGM Sensors",
            "product_category": "",
            "units": "",
            "auth_id": "",
            "cgm_coverage": "Insulin",
            "primary_insurance_name": "Anthem BCBS Commercial",
            "pre_computed_hcpc": "A4239",
            "pre_computed_units": "3",
            "pre_computed_modifiers": ["KS"],
            "pre_computed_charge": "450.00",
        }

        result = build_service_line_from_normalized_order(order)

        assert result["procedure_code"] == "A4239"
        assert result["service_unit_count"] == "3"
        assert result["procedure_modifiers"] == ["KS"]
        assert result["line_item_charge_amount"] == "450.00"
        assert result["service_date"] == "20260316"

    def test_legacy_fallback_when_no_pre_computed(self):
        """When no pre-computed fields, should fall back to resolver functions."""
        from claim_infrastructure import build_service_line_from_normalized_order

        order = {
            "order_date": "20260315",
            "item": "CGM Sensors",
            "variant": "Dexcom G7",
            "quantity": "6",
            "source_child_name": "CGM Sensors",
            "product_category": "",
            "units": "",
            "auth_id": "",
            "cgm_coverage": "Insulin",
            "primary_insurance_name": "Anthem BCBS Commercial",
        }

        result = build_service_line_from_normalized_order(order)

        # Should still produce a procedure_code via resolvers
        assert result["procedure_code"] != ""
        assert result["service_unit_count"] != ""
        assert result["line_item_charge_amount"] != ""

    def test_partial_pre_computed_falls_back(self):
        """If only some pre-computed fields present, should fall back entirely."""
        from claim_infrastructure import build_service_line_from_normalized_order

        order = {
            "order_date": "20260315",
            "item": "CGM Sensors",
            "variant": "Dexcom G7",
            "quantity": "6",
            "source_child_name": "CGM Sensors",
            "product_category": "",
            "units": "",
            "auth_id": "",
            "cgm_coverage": "Insulin",
            "primary_insurance_name": "Anthem BCBS Commercial",
            "pre_computed_hcpc": "A4239",
            # Missing pre_computed_units and pre_computed_charge
        }

        result = build_service_line_from_normalized_order(order)

        # Should NOT use the pre_computed_hcpc since units/charge are missing
        # Instead falls back to resolver which may produce same code
        assert result["procedure_code"] != ""
        assert result["line_item_charge_amount"] != ""


# ============================================================
# 3. claim_builder_service.py — Claims Board support
# ============================================================

class TestClaimsBuilderService:
    """Test Claims Board item to normalized orders conversion."""

    def get_mock_claims_board_item(self):
        """Helper to create a mock Claims Board item with REAL column IDs."""
        return {
            "id": "test_cb_123",
            "name": "John TestPatient - Anthem BCBS Commercial",
            "column_values": [
                # Patient / Insurance (real Claims Board parent column IDs)
                {"id": "text_mktat89m",      "text": "TEST123456",     "value": None},   # Member ID
                {"id": "text_mkp3y5ax",      "text": "01/15/1980",     "value": None},   # DOB
                {"id": "text_mkxr2r9b",      "text": "1234567890",     "value": None},   # Doctor NPI
                {"id": "text_mkxrh4a4",      "text": "Jane Doctor",    "value": None},   # Doctor Name
                {"id": "text_mkwzbcme",      "text": "",               "value": None},   # Correlation ID
                {"id": "date_mkwr7spz",      "text": "2026-03-15",     "value": None},   # DOS
                {"id": "color_mky2gpz5",     "text": "E10.65",         "value": None},   # Diagnosis Code (status)
                {"id": "location_mkxxpesw",  "text": "123 Test St, Brooklyn, NY 11221", "value": None},  # Patient Address
                {"id": "location_mkxr251b",  "text": "456 Medical Ave, New York, NY 10001", "value": None},  # Doctor Address
                {"id": "color_mky1qvcf",     "text": "CGM",            "value": None},   # Subscription Type (status)
                {"id": "color_mkxmmm77",     "text": "Commercial",     "value": None},   # Insurance Type (status)
                {"id": "color_mkxmywtb",     "text": "Outstanding",    "value": None},   # Primary Status
                # NOTE: No Gender, CGM Coverage, or Doctor Phone columns on Claims Board
            ],
            "subitems": [
                {
                    "id": "sub_1",
                    "name": "Insulin Pump",
                    "column_values": [
                        # Real Claims Board subitem column IDs
                        {"id": "color_mm1cdvq8",     "text": "E0784",   "value": None},   # HCPC Code (STATUS)
                        {"id": "numeric_mm1czbyg",   "text": "1",       "value": None},   # Order Quantity
                        {"id": "formula_mm1cv57q",   "text": "1",       "value": None},   # Claim Qty (FORMULA)
                        {"id": "formula_mm1c7nen",   "text": "2500.00", "value": None},   # Est. Pay (FORMULA)
                        {"id": "color_mm1cjcmg",     "text": "Anthem BCBS Commercial", "value": None},
                        {"id": "color_mm1cnfsb",     "text": "Monthly",  "value": None},
                    ],
                },
                {
                    "id": "sub_2",
                    "name": "CGM Sensors",
                    "column_values": [
                        {"id": "color_mm1cdvq8",     "text": "A4239",  "value": None},
                        {"id": "numeric_mm1czbyg",   "text": "6",      "value": None},
                        {"id": "formula_mm1cv57q",   "text": "3",      "value": None},   # Claim Qty (FORMULA)
                        {"id": "formula_mm1c7nen",   "text": "450.00", "value": None},   # Est. Pay (FORMULA)
                        {"id": "color_mm1cjcmg",     "text": "Anthem BCBS Commercial", "value": None},
                        {"id": "color_mm1cnfsb",     "text": "Quarterly", "value": None},
                    ],
                },
                {
                    "id": "sub_3",
                    "name": "CGM Monitor",
                    "column_values": [
                        {"id": "color_mm1cdvq8",     "text": "",  "value": None},
                        {"id": "numeric_mm1czbyg",   "text": "",  "value": None},
                        {"id": "formula_mm1cv57q",   "text": "",  "value": None},
                        {"id": "formula_mm1c7nen",   "text": "",  "value": None},
                        {"id": "color_mm1cjcmg",     "text": "",  "value": None},
                        {"id": "color_mm1cnfsb",     "text": "",  "value": None},
                    ],
                },
            ],
        }

    def test_claims_board_item_to_normalized_orders(self):
        """Should extract normalized orders from Claims Board item."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders
        item = self.get_mock_claims_board_item()
        orders = claims_board_item_to_normalized_orders(item)

        # Should produce 2 orders (sub_3 has no HCPC, so skipped)
        assert len(orders) == 2

        # First order should be Insulin Pump
        pump_order = orders[0]
        assert pump_order["pre_computed_hcpc"] == "E0784"
        assert pump_order["pre_computed_units"] == "1"      # from claim_qty formula
        assert pump_order["pre_computed_charge"] == "2500.00"  # from est_pay formula
        assert pump_order["member_id"] == "TEST123456"

        # Second order should be CGM Sensors
        sensor_order = orders[1]
        assert sensor_order["pre_computed_hcpc"] == "A4239"
        assert sensor_order["pre_computed_units"] == "3"     # from claim_qty formula
        # No modifiers column on Claims Board — always empty list
        assert sensor_order["pre_computed_modifiers"] == []

    def test_claims_board_extracts_patient_name(self):
        """Should strip payer name from patient name."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders
        item = self.get_mock_claims_board_item()
        orders = claims_board_item_to_normalized_orders(item)

        assert orders[0]["patient_full_name"] == "John TestPatient"
        assert orders[0]["patient_first_name"] == "John"
        assert orders[0]["patient_last_name"] == "TestPatient"

    def test_claims_board_skips_empty_subitems(self):
        """Subitems without HCPC code should be skipped."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders
        item = self.get_mock_claims_board_item()

        # Only first 2 subitems have HCPC codes
        orders = claims_board_item_to_normalized_orders(item)
        assert len(orders) == 2

    def test_claims_board_no_subitems(self):
        """Should return empty list when no subitems."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders
        item = {"id": "test", "name": "Test", "column_values": [], "subitems": []}
        orders = claims_board_item_to_normalized_orders(item)
        assert orders == []

    def test_build_claims_from_claims_board_item(self):
        """Should produce valid Stedi JSON from Claims Board item."""
        from services.claim_builder_service import build_claims_from_claims_board_item
        item = self.get_mock_claims_board_item()
        payloads = build_claims_from_claims_board_item(item)

        assert len(payloads) >= 1

        payload = payloads[0]
        assert "claimInformation" in payload
        assert "serviceLines" in payload["claimInformation"]

        service_lines = payload["claimInformation"]["serviceLines"]
        assert len(service_lines) == 2  # Pump + Sensors

        # Verify pre-computed HCPC codes are in the payload
        hcpc_codes = [
            sl["professionalService"]["procedureCode"]
            for sl in service_lines
        ]
        assert "E0784" in hcpc_codes
        assert "A4239" in hcpc_codes


# ============================================================
# 4. monday_service.py — new functions
# ============================================================

class TestMondayServiceNewFunctions:
    """Test new Monday service functions in mock mode."""

    def test_get_new_order_item_mock(self):
        """Should return mock data when no API token."""
        from services.monday_service import get_new_order_item
        item = get_new_order_item("12345")
        assert item["id"] == "12345"
        assert item["name"] == "John TestPatient"
        # NOB was duplicated from Order Board — has subitems (same structure)
        assert "subitems" in item
        assert len(item["subitems"]) >= 1

    def test_get_claims_board_item_mock(self):
        """Should return mock Claims Board data in mock mode."""
        from services.monday_service import get_claims_board_item
        item = get_claims_board_item("cb_123")
        assert item["id"] == "cb_123"
        assert "subitems" in item
        assert len(item["subitems"]) >= 2

    def test_create_claims_board_parent_mock(self):
        """Should return mock ID in mock mode."""
        from services.monday_service import create_claims_board_parent
        result = create_claims_board_parent(
            patient_name="Test Patient",
            payer_name="Test Payer",
            normalized_order={"member_id": "123", "doctor_npi": "456"},
        )
        assert result.startswith("mock_cb_")

    def test_populate_claims_board_subitems_mock(self):
        """Should complete without error in mock mode."""
        from services.monday_service import populate_claims_board_subitems
        products = [
            {"product_name": "CGM Sensors", "hcpc_code": "A4239",
             "claim_qty": "6", "units": "3", "modifiers": ["KS"],
             "charge_amount": "450.00", "est_pay": "450.00"},
        ]
        # Should not raise
        populate_claims_board_subitems("mock_123", products)

    def test_update_claims_board_277_mock(self):
        """Should complete without error in mock mode."""
        from services.monday_service import update_claims_board_277
        # Should not raise
        update_claims_board_277("mock_123", "Accepted")
        update_claims_board_277("mock_123", "Rejected", "Invalid member ID")

    def test_update_claims_board_workflow_mock(self):
        """Should complete without error in mock mode."""
        from services.monday_service import update_claims_board_workflow
        # Should not raise
        update_claims_board_workflow("mock_123", "Submitted")
        update_claims_board_workflow("mock_123", "Paid")

    def test_update_existing_claims_subitems_mock(self):
        """Should complete without error in mock mode."""
        from services.monday_service import update_existing_claims_subitems
        children = [
            {"HCPC Code": "A4239", "Primary Paid": 300.00},
        ]
        # Should not raise
        update_existing_claims_subitems("mock_123", children)


# ============================================================
# 5. routes/order_to_claims.py — migration endpoint
# ============================================================

class TestOrderToClaimsRoute:
    """Test the New Order Board → Claims Board migration endpoint."""

    def test_compute_product_subitem_data(self):
        """Should compute valid subitem data for a product."""
        from routes.order_to_claims import compute_product_subitem_data
        from claims_board_config import PRODUCT_CATEGORIES

        normalized = {
            "primary_insurance_name": "Anthem BCBS Commercial",
            "cgm_coverage": "Insulin",
        }
        order_cols = {
            "cgm_sensor_qty": "6",
            "cgm_type": "Dexcom G7",
        }

        # Find CGM Sensors category
        cgm_cat = next(c for c in PRODUCT_CATEGORIES if c["name"] == "CGM Sensors")
        result = compute_product_subitem_data(normalized, order_cols, cgm_cat)

        assert result is not None
        assert result["product_name"] == "CGM Sensors"
        assert result["hcpc_code"] == "A4239"
        assert result["claim_qty"] == "6"
        assert float(result["charge_amount"]) > 0

    def test_compute_product_subitem_zero_qty(self):
        """Should return None when quantity is 0."""
        from routes.order_to_claims import compute_product_subitem_data
        from claims_board_config import PRODUCT_CATEGORIES

        normalized = {"primary_insurance_name": "Anthem BCBS Commercial"}
        order_cols = {"cgm_sensor_qty": "0"}

        cgm_cat = next(c for c in PRODUCT_CATEGORIES if c["name"] == "CGM Sensors")
        result = compute_product_subitem_data(normalized, order_cols, cgm_cat)
        assert result is None

    def test_compute_product_subitem_missing_qty(self):
        """Should return None when quantity is missing."""
        from routes.order_to_claims import compute_product_subitem_data
        from claims_board_config import PRODUCT_CATEGORIES

        normalized = {"primary_insurance_name": "Anthem BCBS Commercial"}
        order_cols = {}

        cgm_cat = next(c for c in PRODUCT_CATEGORIES if c["name"] == "CGM Sensors")
        result = compute_product_subitem_data(normalized, order_cols, cgm_cat)
        assert result is None

    def test_compute_all_product_subitems(self):
        """Should compute subitems only for products with qty > 0."""
        from routes.order_to_claims import compute_all_product_subitems

        normalized = {
            "primary_insurance_name": "Anthem BCBS Commercial",
            "cgm_coverage": "Insulin",
        }
        order_cols = {
            "pump_qty": "1",
            "infusion_set_qty": "0",
            "cartridge_qty": "0",
            "cgm_sensor_qty": "6",
            "cgm_monitor_qty": "0",
            "pump_type": "t:slim X2",
            "cgm_type": "Dexcom G7",
        }

        results = compute_all_product_subitems(normalized, order_cols)

        # Only pump and sensors have qty > 0
        assert len(results) == 2
        names = [r["product_name"] for r in results]
        assert "Insulin Pump" in names
        assert "CGM Sensors" in names


# ============================================================
# 6. monday_webhook.py — dual routing
# ============================================================

class TestDualRouting:
    """Test that the webhook correctly routes based on SUBMISSION_SOURCE."""

    def test_default_is_order_board_mode(self):
        """Default routing should go to Order Board handler."""
        os.environ.pop("SUBMISSION_SOURCE", None)
        from claims_board_config import is_claims_board_mode
        assert is_claims_board_mode() is False

    def test_claims_board_mode_routing(self):
        """Setting SUBMISSION_SOURCE=claims_board should trigger CB mode."""
        os.environ["SUBMISSION_SOURCE"] = "claims_board"
        from claims_board_config import is_claims_board_mode
        assert is_claims_board_mode() is True
        os.environ.pop("SUBMISSION_SOURCE", None)


# ============================================================
# 7. stedi_webhook.py — Claims Board mode
# ============================================================

class TestStediWebhookClaimsBoardMode:
    """Test 277/835 handlers respect Claims Board mode."""

    def test_277_handler_exists(self):
        """handle_277_event should be importable."""
        from routes.stedi_webhook import handle_277_event
        assert callable(handle_277_event)

    def test_835_handler_exists(self):
        """handle_835_event should be importable."""
        from routes.stedi_webhook import handle_835_event
        assert callable(handle_835_event)

    def test_parse_277_status(self):
        """parse_277_status should extract status from 277 report."""
        from routes.stedi_webhook import parse_277_status

        # Test Accepted
        report_accepted = {
            "transactions": [{
                "payers": [{
                    "claimStatusTransactions": [{
                        "claimStatusDetails": [{
                            "patientClaimStatusDetails": [{
                                "claims": [{
                                    "patientAccountNumber": "PCN123",
                                    "claimStatus": {
                                        "informationClaimStatuses": [{
                                            "informationStatuses": [{
                                                "healthCareClaimStatusCategoryCode": "A1",
                                                "statusCodeValue": ""
                                            }]
                                        }]
                                    }
                                }]
                            }]
                        }]
                    }]
                }]
            }]
        }

        status, reason, pcn = parse_277_status(report_accepted)
        assert status == "Accepted"
        assert pcn == "PCN123"

    def test_parse_277_status_rejected(self):
        """parse_277_status should handle rejection."""
        from routes.stedi_webhook import parse_277_status

        report_rejected = {
            "transactions": [{
                "payers": [{
                    "claimStatusTransactions": [{
                        "claimStatusDetails": [{
                            "patientClaimStatusDetails": [{
                                "claims": [{
                                    "patientAccountNumber": "PCN456",
                                    "claimStatus": {
                                        "informationClaimStatuses": [{
                                            "informationStatuses": [{
                                                "healthCareClaimStatusCategoryCode": "A3",
                                                "statusCodeValue": "Invalid member ID"
                                            }]
                                        }]
                                    }
                                }]
                            }]
                        }]
                    }]
                }]
            }]
        }

        status, reason, pcn = parse_277_status(report_rejected)
        assert status == "Rejected"
        assert reason == "Invalid member ID"


# ============================================================
# 8. Integration tests — full Claims Board flow
# ============================================================

class TestIntegrationClaimsBoard:
    """Integration tests through FastAPI for Claims Board endpoints."""

    @pytest.fixture
    def transport(self):
        from httpx import ASGITransport
        from main import app
        return ASGITransport(app=app)

    @pytest.mark.asyncio
    async def test_order_to_claims_preview(self, transport):
        """POST /order-to-claims/preview should return computed products."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/order-to-claims/preview", json={"item_id": "12345"})
            assert r.status_code == 200
            data = r.json()
            assert data["status"] == "preview"
            assert "products" in data
            assert len(data["products"]) > 0

    @pytest.mark.asyncio
    async def test_order_to_claims_migrate(self, transport):
        """POST /order-to-claims/migrate should create Claims Board items."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/order-to-claims/migrate", json={"item_id": "12345"})
            assert r.status_code == 200
            data = r.json()
            assert data["status"] == "migrated"
            assert data["claims_item_id"].startswith("mock_cb_")
            assert len(data["products"]) > 0
            assert float(data["total_charge"]) > 0

    @pytest.mark.asyncio
    async def test_order_to_claims_no_item_id(self, transport):
        """POST /order-to-claims/migrate with no item_id should fail."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/order-to-claims/migrate", json={})
            assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_monday_webhook_claims_board_mode(self, transport):
        """Monday webhook in claims_board mode should use CB handler."""
        os.environ["SUBMISSION_SOURCE"] = "claims_board"
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/monday/webhook", json={
                "event": {
                    "pulseId": "cb_99999",
                    "value": {"label": {"text": "Submit Claim"}}
                }
            })
            assert r.status_code == 200
            assert r.json()["status"] == "received"
        os.environ.pop("SUBMISSION_SOURCE", None)

    @pytest.mark.asyncio
    async def test_monday_webhook_order_board_mode(self, transport):
        """Monday webhook in order_board mode should use legacy handler."""
        os.environ.pop("SUBMISSION_SOURCE", None)
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/monday/webhook", json={
                "event": {
                    "pulseId": "99999",
                    "value": {"label": {"text": "Submit Claim"}}
                }
            })
            assert r.status_code == 200
            assert r.json()["status"] == "received"

    @pytest.mark.asyncio
    async def test_health_endpoint(self, transport):
        """Health check should still work."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/health")
            assert r.status_code == 200
            assert r.json()["status"] == "ok"


# ============================================================
# 9. Edge case tests
# ============================================================

class TestEdgeCases:
    """Test edge cases that could arise during integration."""

    def test_no_modifiers_column_on_claims_board(self):
        """No modifiers column exists on Claims Board — always empty list."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders

        item = {
            "id": "test",
            "name": "Test Patient",
            "column_values": [
                {"id": "text_mktat89m", "text": "MEM123", "value": None},
                {"id": "text_mkp3y5ax", "text": "01/01/1990", "value": None},
                {"id": "text_mkxr2r9b", "text": "NPI123", "value": None},
                {"id": "text_mkxrh4a4", "text": "Dr. Test", "value": None},
                {"id": "date_mkwr7spz", "text": "2026-03-15", "value": None},
            ],
            "subitems": [{
                "id": "sub_1",
                "name": "CGM Sensors",
                "column_values": [
                    {"id": "color_mm1cdvq8",   "text": "A4239", "value": None},   # HCPC Code (STATUS)
                    {"id": "numeric_mm1czbyg",  "text": "6", "value": None},       # Order Qty
                    {"id": "formula_mm1cv57q",  "text": "3", "value": None},       # Claim Qty (formula)
                    {"id": "formula_mm1c7nen",  "text": "450.00", "value": None},  # Est. Pay (formula)
                ],
            }],
        }

        orders = claims_board_item_to_normalized_orders(item)
        assert len(orders) == 1
        # No modifiers column on Claims Board — always empty
        assert orders[0]["pre_computed_modifiers"] == []

    def test_formula_fields_used_for_units_and_charge(self):
        """claim_qty (formula) should be used for units, est_pay for charge."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders

        item = {
            "id": "test",
            "name": "Test",
            "column_values": [
                {"id": "text_mktat89m", "text": "MEM123", "value": None},
                {"id": "text_mkp3y5ax", "text": "01/01/1990", "value": None},
                {"id": "text_mkxr2r9b", "text": "NPI123", "value": None},
                {"id": "text_mkxrh4a4", "text": "Dr. Test", "value": None},
                {"id": "date_mkwr7spz", "text": "2026-03-15", "value": None},
            ],
            "subitems": [{
                "id": "sub_1",
                "name": "Pump",
                "column_values": [
                    {"id": "color_mm1cdvq8",   "text": "E0784", "value": None},
                    {"id": "numeric_mm1czbyg",  "text": "1", "value": None},        # Order Qty
                    {"id": "formula_mm1cv57q",  "text": "1", "value": None},        # Claim Qty (formula)
                    {"id": "formula_mm1c7nen",  "text": "2500.00", "value": None},  # Est. Pay (formula)
                ],
            }],
        }

        orders = claims_board_item_to_normalized_orders(item)
        assert orders[0]["pre_computed_units"] == "1"       # from claim_qty formula
        assert orders[0]["pre_computed_charge"] == "2500.00"  # from est_pay formula
        assert orders[0]["pre_computed_modifiers"] == []

    def test_patient_name_with_no_payer_suffix(self):
        """Claims Board item name without ' - payer' should work."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders

        item = {
            "id": "test",
            "name": "Solo Patient Name",
            "column_values": [
                {"id": "text_mktat89m", "text": "MEM123", "value": None},
                {"id": "text_mkp3y5ax", "text": "01/01/1990", "value": None},
                {"id": "text_mkxr2r9b", "text": "NPI123", "value": None},
                {"id": "text_mkxrh4a4", "text": "Dr. Test", "value": None},
                {"id": "date_mkwr7spz", "text": "2026-03-15", "value": None},
            ],
            "subitems": [{
                "id": "sub_1",
                "name": "Test Product",
                "column_values": [
                    {"id": "color_mm1cdvq8",   "text": "E0784", "value": None},
                    {"id": "numeric_mm1czbyg",  "text": "1", "value": None},
                    {"id": "formula_mm1cv57q",  "text": "1", "value": None},
                    {"id": "formula_mm1c7nen",  "text": "2500.00", "value": None},
                ],
            }],
        }

        orders = claims_board_item_to_normalized_orders(item)
        assert orders[0]["patient_full_name"] == "Solo Patient Name"
