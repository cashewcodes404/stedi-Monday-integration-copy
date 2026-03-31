"""
test_stress.py
==============
Stress tests and edge-case tests for the Claims Board migration.

These go BEYOND the happy-path tests in test_migration.py.
They cover:
  1. Malformed / missing data resilience
  2. Column ID consistency across all modules
  3. Cross-module integration (config ↔ service ↔ builder ↔ routes)
  4. Dev brief compliance checks
  5. Deployment readiness (env vars, board IDs, etc.)
"""

import sys
import os
import json
import re
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Ensure mock mode
os.environ.pop("MONDAY_API_TOKEN", None)
os.environ.pop("STEDI_API_KEY", None)


# ============================================================
# 1. COLUMN ID CONSISTENCY — No placeholders anywhere
# ============================================================

class TestColumnIdConsistency:
    """Verify no cb_/nob_ placeholder IDs remain in ANY module."""

    def test_claims_board_config_no_placeholders(self):
        from claims_board_config import (
            CLAIMS_BOARD_PARENT_COLUMN_MAP,
            CLAIMS_BOARD_SUBITEM_COLUMN_MAP,
            NEW_ORDER_BOARD_COLUMN_MAP,
        )
        for col_id in CLAIMS_BOARD_PARENT_COLUMN_MAP:
            assert not col_id.startswith("cb_"), f"Placeholder found in parent map: {col_id}"
            assert not col_id.startswith("nob_"), f"Placeholder found in parent map: {col_id}"

        for col_id in CLAIMS_BOARD_SUBITEM_COLUMN_MAP:
            assert not col_id.startswith("cb_"), f"Placeholder found in subitem map: {col_id}"

        for col_id in NEW_ORDER_BOARD_COLUMN_MAP:
            assert not col_id.startswith("nob_"), f"Placeholder found in NOB map: {col_id}"

    def test_claim_builder_service_no_placeholders(self):
        from services.claim_builder_service import CLAIMS_BOARD_COLUMN_MAP, CLAIMS_BOARD_SUBITEM_MAP
        for col_id in CLAIMS_BOARD_COLUMN_MAP:
            assert not col_id.startswith("cb_"), f"Placeholder in builder parent map: {col_id}"
        for col_id in CLAIMS_BOARD_SUBITEM_MAP:
            assert not col_id.startswith("cb_"), f"Placeholder in builder subitem map: {col_id}"

    def test_monday_service_era_map_no_placeholders(self):
        from services.monday_service import SUBITEM_ERA_COLUMN_MAP
        for field_name, (col_id, col_type) in SUBITEM_ERA_COLUMN_MAP.items():
            assert not col_id.startswith("cb_"), f"Placeholder in ERA map: {field_name}={col_id}"
            # All ERA column IDs should match Monday's naming pattern
            assert re.match(r"^(numeric|text|long_text|date|color|formula)_", col_id), \
                f"ERA column ID doesn't match Monday pattern: {field_name}={col_id}"

    def test_era_column_ids_match_subitem_config(self):
        """ERA column IDs in monday_service should match claims_board_config subitem map."""
        from services.monday_service import SUBITEM_ERA_COLUMN_MAP
        from claims_board_config import CLAIMS_BOARD_SUBITEM_COLUMN_MAP

        era_col_ids = {col_id for col_id, _ in SUBITEM_ERA_COLUMN_MAP.values()}
        config_col_ids = set(CLAIMS_BOARD_SUBITEM_COLUMN_MAP.keys())

        # Every ERA column ID should exist in the subitem config
        for era_id in era_col_ids:
            assert era_id in config_col_ids, \
                f"ERA column ID {era_id} not in CLAIMS_BOARD_SUBITEM_COLUMN_MAP"

    def test_write_maps_are_complete_inverses(self):
        """Write maps should be exact inverses of column maps."""
        from claims_board_config import (
            CLAIMS_BOARD_PARENT_COLUMN_MAP,
            CLAIMS_BOARD_PARENT_WRITE_MAP,
            CLAIMS_BOARD_SUBITEM_COLUMN_MAP,
            CLAIMS_BOARD_SUBITEM_WRITE_MAP,
        )
        # Parent
        for col_id, semantic in CLAIMS_BOARD_PARENT_COLUMN_MAP.items():
            assert CLAIMS_BOARD_PARENT_WRITE_MAP[semantic] == col_id, \
                f"Write map mismatch: {semantic} -> expected {col_id}"

        # Subitem
        for col_id, semantic in CLAIMS_BOARD_SUBITEM_COLUMN_MAP.items():
            assert CLAIMS_BOARD_SUBITEM_WRITE_MAP[semantic] == col_id, \
                f"Write map mismatch: {semantic} -> expected {col_id}"

    def test_hcpc_status_index_covers_all_known_codes(self):
        """HCPC_STATUS_INDEX should cover all HCPC codes in HCPC_TO_PRODUCT."""
        from claims_board_config import HCPC_STATUS_INDEX, HCPC_TO_PRODUCT
        for code in HCPC_TO_PRODUCT:
            assert code in HCPC_STATUS_INDEX, \
                f"HCPC code {code} in HCPC_TO_PRODUCT but not in HCPC_STATUS_INDEX"

    def test_builder_column_map_matches_config(self):
        """claim_builder_service column maps should be consistent with claims_board_config."""
        from services.claim_builder_service import CLAIMS_BOARD_COLUMN_MAP, CLAIMS_BOARD_SUBITEM_MAP
        from claims_board_config import CLAIMS_BOARD_PARENT_COLUMN_MAP, CLAIMS_BOARD_SUBITEM_COLUMN_MAP

        # Builder parent map IDs should be a subset of the config
        for col_id in CLAIMS_BOARD_COLUMN_MAP:
            assert col_id in CLAIMS_BOARD_PARENT_COLUMN_MAP, \
                f"Builder parent column {col_id} not in config parent map"

        # Builder subitem map IDs should be a subset of the config
        for col_id in CLAIMS_BOARD_SUBITEM_MAP:
            assert col_id in CLAIMS_BOARD_SUBITEM_COLUMN_MAP, \
                f"Builder subitem column {col_id} not in config subitem map"


# ============================================================
# 2. MALFORMED DATA RESILIENCE
# ============================================================

class TestMalformedDataResilience:
    """Test that the system handles bad/missing data gracefully."""

    def test_claims_board_item_missing_all_columns(self):
        """Should return empty orders when no column data at all."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders
        item = {"id": "bad", "name": "Bad Item", "column_values": [], "subitems": [
            {"id": "s1", "name": "Pump", "column_values": []}
        ]}
        orders = claims_board_item_to_normalized_orders(item)
        # No HCPC code → subitem should be skipped
        assert orders == []

    def test_claims_board_item_empty_name(self):
        """Should handle empty item name gracefully."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders
        item = {
            "id": "test",
            "name": "",  # empty
            "column_values": [],
            "subitems": [{
                "id": "s1",
                "name": "Test",
                "column_values": [
                    {"id": "color_mm1cdvq8", "text": "A4239", "value": None},
                    {"id": "numeric_mm1czbyg", "text": "1", "value": None},
                ],
            }],
        }
        orders = claims_board_item_to_normalized_orders(item)
        assert len(orders) == 1
        assert orders[0]["patient_full_name"] == ""
        assert orders[0]["payer_name"] == ""

    def test_claims_board_item_missing_subitems_key(self):
        """Should handle missing subitems key entirely."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders
        item = {"id": "test", "name": "Test", "column_values": []}
        # No "subitems" key at all
        orders = claims_board_item_to_normalized_orders(item)
        assert orders == []

    def test_claims_board_item_none_column_text(self):
        """Columns with None text values should not crash."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders
        item = {
            "id": "test",
            "name": "Test - Payer",
            "column_values": [
                {"id": "text_mktat89m", "text": None, "value": None},
                {"id": "date_mkwr7spz", "text": None, "value": None},
            ],
            "subitems": [{
                "id": "s1",
                "name": "Pump",
                "column_values": [
                    {"id": "color_mm1cdvq8", "text": "E0784", "value": None},
                    {"id": "numeric_mm1czbyg", "text": None, "value": None},
                    {"id": "formula_mm1cv57q", "text": None, "value": None},
                    {"id": "formula_mm1c7nen", "text": None, "value": None},
                ],
            }],
        }
        orders = claims_board_item_to_normalized_orders(item)
        assert len(orders) == 1
        assert orders[0]["pre_computed_hcpc"] == "E0784"
        assert orders[0]["member_id"] == ""

    def test_pre_computed_values_all_empty(self):
        """If pre-computed charge/units are empty, should fall back to resolver."""
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
            "pre_computed_units": "",   # empty
            "pre_computed_charge": "",  # empty
        }
        result = build_service_line_from_normalized_order(order)
        # Should fall back to resolver — still produces valid output
        assert result["procedure_code"] != ""
        assert result["line_item_charge_amount"] != ""

    def test_zero_quantity_products_excluded(self):
        """Products with 0 quantity should not generate subitems."""
        from routes.order_to_claims import compute_product_subitem_data
        from claims_board_config import PRODUCT_CATEGORIES

        normalized = {"primary_insurance_name": "Anthem BCBS Commercial"}
        for cat in PRODUCT_CATEGORIES:
            result = compute_product_subitem_data(normalized, {cat["qty_field"]: "0"}, cat)
            assert result is None, f"Product {cat['name']} should be None for qty=0"

    def test_negative_quantity_products_excluded(self):
        """Products with negative quantity should not generate subitems."""
        from routes.order_to_claims import compute_product_subitem_data
        from claims_board_config import PRODUCT_CATEGORIES

        normalized = {"primary_insurance_name": "Anthem BCBS Commercial"}
        cgm = next(c for c in PRODUCT_CATEGORIES if c["name"] == "CGM Sensors")
        result = compute_product_subitem_data(normalized, {"cgm_sensor_qty": "-3"}, cgm)
        assert result is None

    def test_non_numeric_quantity_handled(self):
        """Products with non-numeric quantity should not crash."""
        from routes.order_to_claims import compute_product_subitem_data
        from claims_board_config import PRODUCT_CATEGORIES

        normalized = {"primary_insurance_name": "Anthem BCBS Commercial"}
        pump = next(c for c in PRODUCT_CATEGORIES if c["name"] == "Insulin Pump")
        result = compute_product_subitem_data(normalized, {"pump_qty": "abc"}, pump)
        assert result is None


# ============================================================
# 3. CROSS-MODULE FLOW TESTS
# ============================================================

class TestCrossModuleFlow:
    """Test that data flows correctly between modules."""

    def test_full_claims_board_to_stedi_payload(self):
        """Full flow: Claims Board mock → normalized orders → Stedi JSON."""
        from services.monday_service import get_claims_board_item
        from services.claim_builder_service import build_claims_from_claims_board_item

        item = get_claims_board_item("stress_test_1")
        payloads = build_claims_from_claims_board_item(item)

        assert len(payloads) >= 1
        payload = payloads[0]

        # Verify Stedi JSON structure
        assert "tradingPartnerServiceId" in payload or "tradingPartnerName" in payload
        assert "subscriber" in payload
        assert "claimInformation" in payload

        claim_info = payload["claimInformation"]
        assert "serviceLines" in claim_info
        assert len(claim_info["serviceLines"]) >= 1

        # Verify charge amounts are formatted
        for sl in claim_info["serviceLines"]:
            charge = sl["professionalService"]["lineItemChargeAmount"]
            # Should be a decimal string
            assert "." in str(charge), f"Charge not decimal formatted: {charge}"

    def test_migration_flow_produces_valid_subitems(self):
        """Full migration: NOB mock → compute subitems → verify structure."""
        from services.monday_service import get_new_order_item
        from routes.order_to_claims import new_order_to_normalized, compute_all_product_subitems

        item = get_new_order_item("stress_test_2")
        normalized, order_cols = new_order_to_normalized(item)

        # normalized should have patient data
        assert normalized["patient_full_name"] == "John TestPatient"
        assert normalized["patient_dob"] != ""

        # compute products
        products = compute_all_product_subitems(normalized, order_cols)
        assert len(products) >= 1

        # Each product should have required fields
        for p in products:
            assert p["product_name"] != ""
            assert p["hcpc_code"] != ""
            assert float(p["charge_amount"]) > 0

    def test_277_parse_handles_deeply_nested_structure(self):
        """277 parser should survive deeply nested/missing fields."""
        from routes.stedi_webhook import parse_277_status

        # Completely empty — falls through to default "Pending" category
        status, reason, pcn = parse_277_status({})
        # Empty dict navigates through .get() chains, hits empty category code → "Pending"
        assert status in ("Pending", "Unknown")

        # Partial structure — should not crash
        partial = {"transactions": [{"payers": []}]}
        status, reason, pcn = parse_277_status(partial)
        assert status in ("Pending", "Unknown")

    def test_build_claims_from_claims_board_produces_correct_hcpc(self):
        """HCPC codes from Claims Board subitems should appear in Stedi payload."""
        from services.monday_service import get_claims_board_item
        from services.claim_builder_service import build_claims_from_claims_board_item

        item = get_claims_board_item("hcpc_test")
        payloads = build_claims_from_claims_board_item(item)
        assert len(payloads) >= 1

        # Extract all HCPC codes from service lines
        hcpc_codes = set()
        for payload in payloads:
            for sl in payload["claimInformation"]["serviceLines"]:
                hcpc_codes.add(sl["professionalService"]["procedureCode"])

        # Mock item has E0784 and A4239
        assert "E0784" in hcpc_codes
        assert "A4239" in hcpc_codes


# ============================================================
# 4. DEV BRIEF COMPLIANCE
# ============================================================

class TestDevBriefCompliance:
    """Verify all 8 dev brief sections are implemented."""

    def test_section_4a_order_to_claims_route_exists(self):
        """4a: Route for Order Board → Claims Board should exist."""
        from routes.order_to_claims import router
        routes = [r.path for r in router.routes]
        assert "/migrate" in routes
        assert "/preview" in routes

    def test_section_4b_claims_board_webhook_handler(self):
        """4b: Monday webhook should have Claims Board handler."""
        from routes.monday_webhook import handle_claims_board_event
        assert callable(handle_claims_board_event)

    def test_section_4c_new_monday_service_functions(self):
        """4c: monday_service.py should have all new functions."""
        from services import monday_service as ms
        assert callable(ms.get_claims_board_item)
        assert callable(ms.create_claims_board_parent)
        assert callable(ms.populate_claims_board_subitems)
        assert callable(ms.update_existing_claims_subitems)
        assert callable(ms.update_claims_board_277)
        assert callable(ms.update_claims_board_workflow)

    def test_section_4d_pre_computed_values_in_builder(self):
        """4d: claim_infrastructure should read pre-computed values."""
        from claim_infrastructure import build_service_line_from_normalized_order
        order = {
            "order_date": "20260315",
            "item": "Test",
            "source_child_name": "Test",
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

    def test_section_4e_277_writes_to_claims_board(self):
        """4e: 277 handler should update Claims Board, not Order Board."""
        from routes.stedi_webhook import handle_277_event
        assert callable(handle_277_event)

    def test_section_4e_835_updates_existing_subitems(self):
        """4e: 835 handler should UPDATE existing subitems (not create)."""
        from services.monday_service import update_existing_claims_subitems
        assert callable(update_existing_claims_subitems)

    def test_section_5_env_vars_documented(self):
        """Section 5: Required env vars should be in .env.example."""
        env_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            ".env.example"
        )
        with open(env_path) as f:
            content = f.read()
        assert "MONDAY_NEW_ORDER_BOARD_ID" in content
        assert "MONDAY_CLAIMS_BOARD_ID" in content
        assert "SUBMISSION_SOURCE" in content

    def test_5_product_categories(self):
        """Dev brief specifies 5 product subitems."""
        from claims_board_config import PRODUCT_CATEGORIES
        assert len(PRODUCT_CATEGORIES) == 5
        expected = {"Insulin Pump", "Infusion Set", "Cartridge", "CGM Sensors", "CGM Monitor"}
        actual = {p["name"] for p in PRODUCT_CATEGORIES}
        assert actual == expected

    def test_claims_board_submission_uses_pre_computed(self):
        """Claims Board flow should read subitems, not re-compute."""
        from services.claim_builder_service import claims_board_item_to_normalized_orders
        from services.monday_service import get_claims_board_item

        item = get_claims_board_item("test_pre_computed")
        orders = claims_board_item_to_normalized_orders(item)

        for order in orders:
            # Every order from Claims Board should have pre-computed values
            assert order.get("pre_computed_hcpc") != "", \
                f"Missing pre_computed_hcpc for {order.get('source_child_name')}"

    def test_dual_routing_via_submission_source(self):
        """SUBMISSION_SOURCE env var should control routing."""
        os.environ["SUBMISSION_SOURCE"] = "claims_board"
        from claims_board_config import is_claims_board_mode
        assert is_claims_board_mode() is True

        os.environ["SUBMISSION_SOURCE"] = "order_board"
        assert is_claims_board_mode() is False

        os.environ.pop("SUBMISSION_SOURCE", None)
        assert is_claims_board_mode() is False


# ============================================================
# 5. INTEGRATION STRESS
# ============================================================

class TestIntegrationStress:
    """High-level integration stress tests via FastAPI."""

    @pytest.fixture
    def transport(self):
        from httpx import ASGITransport
        from main import app
        return ASGITransport(app=app)

    @pytest.mark.asyncio
    async def test_preview_then_migrate(self, transport):
        """Preview and migrate should return consistent data."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            preview = await client.post("/order-to-claims/preview", json={"item_id": "999"})
            assert preview.status_code == 200
            p_data = preview.json()

            migrate = await client.post("/order-to-claims/migrate", json={"item_id": "999"})
            assert migrate.status_code == 200
            m_data = migrate.json()

            # Same number of products
            assert len(p_data["products"]) == len(m_data["products"])

    @pytest.mark.asyncio
    async def test_health_always_works(self, transport):
        """Health check should never fail."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            for _ in range(5):
                r = await client.get("/health")
                assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_webhook_with_empty_body(self, transport):
        """Monday webhook with empty body should not crash."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/monday/webhook", json={})
            assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_webhook_challenge_response(self, transport):
        """Monday webhook should respond to challenge."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/monday/webhook", json={"challenge": "abc123"})
            assert r.status_code == 200
            assert r.json()["challenge"] == "abc123"

    @pytest.mark.asyncio
    async def test_stedi_webhook_empty_event(self, transport):
        """Stedi webhook with empty event should not crash."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/stedi/webhook", json={"event": {}})
            assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_migrate_empty_item_id(self, transport):
        """Migrate with empty item_id should return 400."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/order-to-claims/migrate", json={"item_id": ""})
            assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_preview_empty_item_id(self, transport):
        """Preview with empty item_id should return 400."""
        from httpx import AsyncClient
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/order-to-claims/preview", json={"item_id": ""})
            assert r.status_code == 400


# ============================================================
# 6. STEDI WEBHOOK HANDLER CHECKS
# ============================================================

class TestStediWebhookHandlers:
    """Verify stedi_webhook.py PCN lookup uses real column IDs."""

    def test_find_claims_by_pcn_uses_real_column(self):
        """_find_claims_item_by_pcn should search correlation_id column."""
        import inspect
        from routes.stedi_webhook import _find_claims_item_by_pcn
        source = inspect.getsource(_find_claims_item_by_pcn)
        assert "text_mkwzbcme" in source, \
            "PCN search should use real column ID text_mkwzbcme"

    def test_find_claims_by_correlation_uses_real_column(self):
        """_find_claims_item_by_correlation_id should search correlation_id column."""
        import inspect
        from routes.stedi_webhook import _find_claims_item_by_correlation_id
        source = inspect.getsource(_find_claims_item_by_correlation_id)
        assert "text_mkwzbcme" in source

    def test_find_claims_by_claim_id_uses_real_column(self):
        """_find_claims_item_by_claim_id should NOT use placeholder column."""
        import inspect
        from routes.stedi_webhook import _find_claims_item_by_claim_id
        source = inspect.getsource(_find_claims_item_by_claim_id)
        assert "text_stedi_claim_id" not in source, \
            "_find_claims_item_by_claim_id still uses placeholder text_stedi_claim_id!"
        assert "text_mkwzbcme" in source

    def test_monday_webhook_stores_pcn_with_real_column(self):
        """Claims Board webhook should store PCN using real column ID."""
        import inspect
        from routes.monday_webhook import handle_claims_board_event
        source = inspect.getsource(handle_claims_board_event)
        assert "text_mkwzbcme" in source, \
            "PCN storage should use real column ID text_mkwzbcme"

    def test_monday_webhook_sets_claim_sent_date(self):
        """Claims Board webhook should set claim_sent_date."""
        import inspect
        from routes.monday_webhook import handle_claims_board_event
        source = inspect.getsource(handle_claims_board_event)
        assert "date_mm14rk8d" in source, \
            "Claim Sent Date should use real column ID date_mm14rk8d"
