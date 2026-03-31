"""Tests for claim_assumptions.py — the billing rules source of truth."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from claim_assumptions import (
    generate_patient_control_number,
    generate_provider_control_number,
    resolve_payer_name,
    resolve_payer_id,
    resolve_claim_filing_code,
    resolve_place_of_service_code,
    resolve_procedure_code,
    resolve_service_unit_count,
    resolve_procedure_modifiers,
    resolve_line_item_charge_amount,
    resolve_bcbs_routed_payer_name_and_pos,
    resolve_cgm_service_unit_count,
    sum_claim_charge_amount,
    safe_str,
    normalize_spaces,
    parse_int,
    parse_yyyymmdd,
    add_days_to_yyyymmdd,
    PAYER_ID_MAP,
    CLAIM_FILING_CODE_MAP,
    PAYER_RATE_SCHEDULE,
)


# ─── PCN / Provider Control Number Generation ──────────────────

class TestControlNumbers:
    def test_pcn_length(self):
        pcn = generate_patient_control_number()
        assert len(pcn) == 17

    def test_pcn_alphanumeric(self):
        pcn = generate_patient_control_number()
        assert pcn.isalnum()

    def test_pcn_custom_length(self):
        pcn = generate_patient_control_number(length=10)
        assert len(pcn) == 10

    def test_provider_control_number_length(self):
        pcn = generate_provider_control_number()
        assert len(pcn) == 12

    def test_uniqueness(self):
        """Two generated PCNs should not collide."""
        pcns = {generate_patient_control_number() for _ in range(100)}
        assert len(pcns) == 100


# ─── Payer Resolution ──────────────────────────────────────────

class TestPayerResolution:
    def test_resolve_payer_id_known(self):
        assert resolve_payer_id("Anthem BCBS Commercial") == "803"
        assert resolve_payer_id("United Commercial") == "87726"
        assert resolve_payer_id("Aetna Medicare") == "60054"
        assert resolve_payer_id("Medicaid") == "MCDNY"

    def test_resolve_payer_id_unknown(self):
        assert resolve_payer_id("NonExistentPayer") == ""

    def test_resolve_claim_filing_code(self):
        assert resolve_claim_filing_code("Anthem BCBS Commercial") == "CI"
        assert resolve_claim_filing_code("Anthem BCBS Medicare") == "MB"
        assert resolve_claim_filing_code("Fidelis Medicaid") == "MC"
        assert resolve_claim_filing_code("Unknown") == "CI"  # default

    def test_all_payers_have_filing_code(self):
        for payer in PAYER_ID_MAP:
            code = resolve_claim_filing_code(payer)
            assert code in ("CI", "MB", "MC"), f"{payer} has unexpected filing code: {code}"

    def test_all_payers_have_id(self):
        for payer in PAYER_ID_MAP:
            pid = resolve_payer_id(payer)
            assert pid, f"{payer} has no payer ID"


# ─── BCBS Routing ──────────────────────────────────────────────

class TestBCBSRouting:
    def test_ny_patient_routes_to_anthem(self):
        name, pos = resolve_bcbs_routed_payer_name_and_pos("Anthem BCBS Commercial", "NY")
        assert name == "Anthem BCBS Commercial"
        assert pos == "12"

    def test_nj_patient_routes_to_horizon(self):
        name, pos = resolve_bcbs_routed_payer_name_and_pos("Anthem BCBS Commercial", "NJ")
        assert name == "Horizon BCBS"
        assert pos == "12"

    def test_fl_patient_routes_to_bcbs_fl(self):
        name, pos = resolve_bcbs_routed_payer_name_and_pos("Anthem BCBS Commercial", "FL")
        assert name == "BCBS FL"
        assert pos == "12"

    def test_tn_patient_routes_to_bcbs_tn(self):
        name, pos = resolve_bcbs_routed_payer_name_and_pos("Anthem BCBS Commercial", "TN")
        assert name == "BCBS TN"
        assert pos == "12"

    def test_out_of_state_uses_pos_11(self):
        name, pos = resolve_bcbs_routed_payer_name_and_pos("Anthem BCBS Commercial", "CA")
        assert name == "Anthem BCBS Commercial"
        assert pos == "11"

    def test_non_bcbs_payer_unchanged(self):
        name, pos = resolve_bcbs_routed_payer_name_and_pos("United Commercial", "CA")
        assert name == "United Commercial"
        assert pos == "12"

    def test_resolve_payer_name_from_order(self):
        order = {"payer_name": "Anthem BCBS Commercial", "patient_state": "NJ"}
        assert resolve_payer_name(order) == "Horizon BCBS"

    def test_resolve_payer_name_falls_back_to_insurance(self):
        order = {"primary_insurance_name": "Cigna", "patient_state": "NY"}
        assert resolve_payer_name(order) == "Cigna"


# ─── Procedure Code Resolution ─────────────────────────────────

class TestProcedureCodes:
    def test_fixed_codes(self):
        assert resolve_procedure_code("AnyPayer", "Insulin Pump") == "E0784"
        assert resolve_procedure_code("AnyPayer", "CGM Monitor") == "E2103"
        assert resolve_procedure_code("AnyPayer", "CGM Sensors") == "A4239"

    def test_payer_specific_infusion_set(self):
        assert resolve_procedure_code("Anthem BCBS Commercial", "Infusion Set 1") == "A4230"
        assert resolve_procedure_code("Anthem BCBS Medicare", "Infusion Set 1") == "A4224"
        assert resolve_procedure_code("Medicare A&B", "Infusion Set 2") == "A4224"

    def test_payer_specific_cartridge(self):
        assert resolve_procedure_code("Anthem BCBS Commercial", "Cartridge") == "A4232"
        assert resolve_procedure_code("Anthem BCBS Medicare", "Cartridge") == "A4225"

    def test_unknown_item_returns_empty(self):
        assert resolve_procedure_code("Anthem BCBS Commercial", "Unknown Widget") == ""


# ─── Service Unit Count ────────────────────────────────────────

class TestServiceUnitCount:
    def test_pump_always_1(self):
        assert resolve_service_unit_count("AnyPayer", "Insulin Pump", "", "1", "E0784") == "1"

    def test_cgm_monitor_always_1(self):
        assert resolve_service_unit_count("AnyPayer", "CGM Monitor", "", "1", "E2103") == "1"

    def test_cgm_sensors_dexcom_g7(self):
        # Dexcom G7 divisor = 3, quantity 6 → 2 units
        assert resolve_cgm_service_unit_count("Dexcom G7", "6") == "2"

    def test_cgm_sensors_dexcom_g6(self):
        assert resolve_cgm_service_unit_count("Dexcom G6", "9") == "3"

    def test_cgm_sensors_freestyle_libre(self):
        # FreeStyle Libre divisor = 2, quantity 4 → 2 units
        assert resolve_cgm_service_unit_count("Freestyle Libre 2 Plus", "4") == "2"

    def test_cgm_sensors_guardian_4(self):
        assert resolve_cgm_service_unit_count("Guardian 4", "8") == "2"

    def test_cgm_sensors_zero_quantity(self):
        assert resolve_cgm_service_unit_count("Dexcom G7", "0") == ""

    def test_infusion_set_a4224_fixed(self):
        count = resolve_service_unit_count("Anthem BCBS Medicare", "Infusion Set 1", "", "3", "A4224")
        assert count == "14"

    def test_cartridge_quantity_based(self):
        # A4232 is quantity-based: qty * 10
        count = resolve_service_unit_count("Anthem BCBS Commercial", "Cartridge", "", "3", "A4232")
        assert count == "30"


# ─── Modifiers ─────────────────────────────────────────────────

class TestModifiers:
    def test_pump_modifiers(self):
        mods = resolve_procedure_modifiers("AnyPayer", "E0784", "")
        assert mods == ["NU", "KX"]

    def test_cgm_sensor_insulin_coverage(self):
        mods = resolve_procedure_modifiers("Anthem BCBS Commercial", "A4239", "Insulin")
        assert "KX" in mods
        assert "KF" in mods
        assert "CG" in mods

    def test_cgm_sensor_hypo_coverage(self):
        mods = resolve_procedure_modifiers("Anthem BCBS Commercial", "A4239", "Hypo")
        assert "KS" in mods

    def test_united_gets_nu_on_sensors(self):
        mods = resolve_procedure_modifiers("United Commercial", "A4239", "Insulin")
        assert "NU" in mods

    def test_non_united_no_nu_on_sensors(self):
        mods = resolve_procedure_modifiers("Anthem BCBS Commercial", "A4239", "Insulin")
        assert "NU" not in mods

    def test_e2103_modifiers(self):
        mods = resolve_procedure_modifiers("AnyPayer", "E2103", "Insulin")
        assert "KF" in mods
        assert "CG" in mods
        assert "NU" in mods
        assert "KX" in mods


# ─── Charge Amount / Rates ─────────────────────────────────────

class TestChargeAmounts:
    def test_pump_rate_anthem(self):
        amount = resolve_line_item_charge_amount("Anthem BCBS Commercial", "E0784", "1")
        assert amount == "4200.00"

    def test_sensor_rate_multiplied_by_units(self):
        # Anthem sensor_rate=375.0, units=2 → 750.00
        amount = resolve_line_item_charge_amount("Anthem BCBS Commercial", "A4239", "2")
        assert amount == "750.00"

    def test_fallback_to_legacy_rate(self):
        # BCBS FL has None rates, should fall back to legacy
        amount = resolve_line_item_charge_amount("BCBS FL", "E0784", "1")
        assert amount == "6000"  # legacy hardcoded

    def test_sum_claim_charge_amount(self):
        lines = [
            {"line_item_charge_amount": "100.50"},
            {"line_item_charge_amount": "200.75"},
            {"line_item_charge_amount": ""},
        ]
        assert sum_claim_charge_amount(lines) == "301.25"

    def test_sum_empty_lines(self):
        assert sum_claim_charge_amount([]) == "0.00"


# ─── Helpers ───────────────────────────────────────────────────

class TestHelpers:
    def test_safe_str(self):
        assert safe_str(None) == ""
        assert safe_str("  hello  ") == "hello"
        assert safe_str(123) == "123"

    def test_normalize_spaces(self):
        assert normalize_spaces("  hello   world  ") == "hello world"

    def test_parse_int(self):
        assert parse_int("5") == 5
        assert parse_int("5.7") == 5
        assert parse_int("") == 0
        assert parse_int(None) == 0

    def test_parse_yyyymmdd(self):
        dt = parse_yyyymmdd("20260315")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 3
        assert dt.day == 15

    def test_parse_yyyymmdd_invalid(self):
        assert parse_yyyymmdd("bad") is None
        assert parse_yyyymmdd("") is None

    def test_add_days(self):
        assert add_days_to_yyyymmdd("20260315", 1) == "20260316"
        assert add_days_to_yyyymmdd("20260331", 1) == "20260401"
        assert add_days_to_yyyymmdd("", 1) == ""
