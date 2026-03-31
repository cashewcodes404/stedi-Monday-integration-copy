"""Tests for claim_infrastructure.py — normalization, address parsing, claim building."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from claim_infrastructure import (
    parse_address,
    normalize_date,
    normalize_gender,
    split_full_name,
    safe_str,
    build_normalized_order_template,
    group_normalized_orders_into_claims,
    build_stedi_claim_json,
    build_service_line_from_normalized_order,
    validate_grouped_claim,
    validate_stedi_claim_json,
)
import pytest


# ─── Address Parsing ───────────────────────────────────────────

class TestAddressParsing:
    def test_standard_address(self):
        result = parse_address("123 Main St, Brooklyn, NY 11221")
        assert result["address1"] == "123 Main St"
        assert result["city"] == "Brooklyn"
        assert result["state"] == "NY"
        assert result["postal_code"] == "11221"

    def test_address_with_zip_plus_4(self):
        result = parse_address("456 Oak Ave, New York, NY 10001-1234")
        assert result["state"] == "NY"
        assert result["postal_code"] == "10001"

    def test_address_with_apartment(self):
        result = parse_address("789 Elm Rd Apt 5B, Queens, NY 11101")
        assert result["address1"] != ""
        assert result["state"] == "NY"

    def test_address_with_usa_suffix(self):
        result = parse_address("100 Park Ave, New York, NY 10017, USA")
        assert result["state"] == "NY"
        assert "USA" not in result["address1"]

    def test_full_state_name(self):
        result = parse_address("200 Broadway, New York, New York 10007")
        assert result["state"] == "NY"

    def test_empty_address(self):
        result = parse_address("")
        assert result["address1"] == ""
        assert result["city"] == ""
        assert result["state"] == ""

    def test_city_title_cased(self):
        result = parse_address("123 Main St, BROOKLYN, NY 11221")
        assert result["city"] == "Brooklyn"


# ─── Date Normalization ───────────────────────────────────────

class TestDateNormalization:
    def test_slash_format_short_year(self):
        assert normalize_date("3/6/26") == "20260306"

    def test_slash_format_long_year(self):
        assert normalize_date("03/06/2026") == "20260306"

    def test_iso_format(self):
        assert normalize_date("2026-03-06") == "20260306"

    def test_empty(self):
        assert normalize_date("") == ""

    def test_invalid(self):
        assert normalize_date("not-a-date") == ""


# ─── Gender Normalization ──────────────────────────────────────

class TestGender:
    def test_male(self):
        assert normalize_gender("Male") == "M"
        assert normalize_gender("M") == "M"
        assert normalize_gender("male") == "M"

    def test_female(self):
        assert normalize_gender("Female") == "F"
        assert normalize_gender("F") == "F"

    def test_unknown(self):
        assert normalize_gender("") == "U"
        assert normalize_gender("Other") == "U"


# ─── Name Splitting ────────────────────────────────────────────

class TestNameSplitting:
    def test_simple_name(self):
        first, last = split_full_name("John Smith")
        assert first == "John"
        assert last == "Smith"

    def test_three_part_name(self):
        first, last = split_full_name("Mary Jane Watson")
        assert first == "Mary"
        assert last == "Jane Watson"

    def test_single_name(self):
        first, last = split_full_name("Cher")
        assert first == "Cher"
        assert last == ""

    def test_empty(self):
        first, last = split_full_name("")
        assert first == ""
        assert last == ""


# ─── Claim Grouping ───────────────────────────────────────────

class TestClaimGrouping:
    def _make_order(self, patient="John Smith", member_id="M001", date="20260315",
                    item="CGM Sensors", payer="Anthem BCBS Commercial", quantity="6",
                    variant="Dexcom G7"):
        order = build_normalized_order_template()
        order["patient_full_name"] = patient
        order["patient_first_name"] = "John"
        order["patient_last_name"] = "Smith"
        order["patient_dob"] = "19800101"
        order["patient_gender"] = "M"
        order["patient_address_1"] = "123 Test St"
        order["patient_city"] = "Brooklyn"
        order["patient_state"] = "NY"
        order["patient_postal_code"] = "11221"
        order["member_id"] = member_id
        order["primary_insurance_name"] = payer
        order["payer_name"] = payer
        order["order_date"] = date
        order["item"] = item
        order["variant"] = variant
        order["quantity"] = quantity
        order["diagnosis_code"] = "E10.65"
        order["cgm_coverage"] = "Insulin"
        order["doctor_name"] = "Jane Doctor"
        order["doctor_first_name"] = "Jane"
        order["doctor_last_name"] = "Doctor"
        order["doctor_npi"] = "1234567890"
        order["doctor_address_1"] = "456 Medical Ave"
        order["doctor_city"] = "New York"
        order["doctor_state"] = "NY"
        order["doctor_postal_code"] = "10001"
        return order

    def test_single_order_single_claim(self):
        orders = [self._make_order()]
        claims = group_normalized_orders_into_claims(orders)
        assert len(claims) == 1
        assert len(claims[0]["service_lines"]) == 1

    def test_same_patient_grouped(self):
        orders = [
            self._make_order(item="CGM Sensors"),
            self._make_order(item="CGM Monitor"),
        ]
        claims = group_normalized_orders_into_claims(orders)
        assert len(claims) == 1
        assert len(claims[0]["service_lines"]) == 2

    def test_different_patients_split(self):
        orders = [
            self._make_order(patient="John Smith", member_id="M001"),
            self._make_order(patient="Jane Doe", member_id="M002"),
        ]
        # Different patient+member combos → different claim keys
        claims = group_normalized_orders_into_claims(orders)
        assert len(claims) == 2

    def test_claim_charge_is_sum(self):
        orders = [
            self._make_order(item="CGM Sensors"),
            self._make_order(item="CGM Monitor"),
        ]
        claims = group_normalized_orders_into_claims(orders)
        charge = float(claims[0]["claim_charge_amount"])
        line_sum = sum(float(l["line_item_charge_amount"]) for l in claims[0]["service_lines"])
        assert abs(charge - line_sum) < 0.01


# ─── Stedi JSON Building ──────────────────────────────────────

class TestStediJsonBuilding:
    def _make_grouped_claim(self):
        order = build_normalized_order_template()
        order["patient_full_name"] = "John Smith"
        order["patient_first_name"] = "John"
        order["patient_last_name"] = "Smith"
        order["patient_dob"] = "19800101"
        order["patient_gender"] = "M"
        order["patient_address_1"] = "123 Test St"
        order["patient_city"] = "Brooklyn"
        order["patient_state"] = "NY"
        order["patient_postal_code"] = "11221"
        order["member_id"] = "TEST123"
        order["primary_insurance_name"] = "Anthem BCBS Commercial"
        order["payer_name"] = "Anthem BCBS Commercial"
        order["order_date"] = "20260315"
        order["item"] = "CGM Sensors"
        order["variant"] = "Dexcom G7"
        order["quantity"] = "6"
        order["diagnosis_code"] = "E10.65"
        order["cgm_coverage"] = "Insulin"
        order["doctor_name"] = "Jane Doctor"
        order["doctor_first_name"] = "Jane"
        order["doctor_last_name"] = "Doctor"
        order["doctor_npi"] = "1234567890"
        order["doctor_address_1"] = "456 Medical Ave"
        order["doctor_city"] = "New York"
        order["doctor_state"] = "NY"
        order["doctor_postal_code"] = "10001"

        claims = group_normalized_orders_into_claims([order])
        return claims[0]

    def test_builds_valid_json(self):
        claim = self._make_grouped_claim()
        payload = build_stedi_claim_json(claim)
        assert payload["tradingPartnerServiceId"] == "803"
        assert payload["subscriber"]["memberId"] == "TEST123"
        assert payload["subscriber"]["firstName"] == "John"
        assert payload["subscriber"]["lastName"] == "Smith"

    def test_has_diagnosis_code(self):
        claim = self._make_grouped_claim()
        payload = build_stedi_claim_json(claim)
        diag = payload["claimInformation"]["healthCareCodeInformation"][0]["diagnosisCode"]
        assert diag == "E10.65"

    def test_has_service_lines(self):
        claim = self._make_grouped_claim()
        payload = build_stedi_claim_json(claim)
        lines = payload["claimInformation"]["serviceLines"]
        assert len(lines) >= 1
        assert lines[0]["professionalService"]["procedureCode"] == "A4239"

    def test_has_referring_provider(self):
        claim = self._make_grouped_claim()
        payload = build_stedi_claim_json(claim)
        assert "referring" in payload
        assert payload["referring"]["npi"] == "1234567890"
        assert payload["referring"]["lastName"] == "Doctor"

    def test_has_billing_provider(self):
        claim = self._make_grouped_claim()
        payload = build_stedi_claim_json(claim)
        assert payload["billing"]["npi"] == "1023042348"
        assert payload["billing"]["organizationName"] == "Mid-Island Medical Supply Company"

    def test_json_serializable(self):
        claim = self._make_grouped_claim()
        payload = build_stedi_claim_json(claim)
        serialized = json.dumps(payload)
        assert len(serialized) > 100

    def test_charge_amounts_are_strings(self):
        claim = self._make_grouped_claim()
        payload = build_stedi_claim_json(claim)
        assert isinstance(payload["claimInformation"]["claimChargeAmount"], str)
        for line in payload["claimInformation"]["serviceLines"]:
            assert isinstance(line["professionalService"]["lineItemChargeAmount"], str)


# ─── Validation ────────────────────────────────────────────────

class TestValidation:
    def test_missing_payer_raises(self):
        claim = {
            "claim_key": "test",
            "payer_name": "",
            "payer_id": "803",
            "claim_filing_code": "CI",
            "member_id": "M001",
            "diagnosis_code": "E10.65",
            "place_of_service_code": "12",
            "patient_control_number": "PCN123",
            "claim_charge_amount": "500",
            "service_lines": [{"service_date": "20260315", "procedure_code": "A4239",
                               "service_unit_count": "2", "line_item_charge_amount": "500",
                               "provider_control_number": "PROV1"}],
        }
        with pytest.raises(ValueError, match="payer_name"):
            validate_grouped_claim(claim)

    def test_missing_service_lines_raises(self):
        claim = {
            "claim_key": "test",
            "payer_name": "Anthem",
            "payer_id": "803",
            "claim_filing_code": "CI",
            "member_id": "M001",
            "diagnosis_code": "E10.65",
            "place_of_service_code": "12",
            "patient_control_number": "PCN123",
            "claim_charge_amount": "500",
            "service_lines": [],
        }
        with pytest.raises(ValueError, match="no service lines"):
            validate_grouped_claim(claim)
