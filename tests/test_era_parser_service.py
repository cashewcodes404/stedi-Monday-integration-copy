"""Tests for era_parser_service.py — ERA JSON parsing."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from services.era_parser_service import (
    parse_era_json,
    parse_era_from_string,
    summarize_era_row_for_monday,
    match_era_rows_to_claim_item,
    parse_service_adjustments,
    parse_remark_codes,
    format_stedi_date,
)


# ─── Date Formatting ──────────────────────────────────────────

class TestDateFormatting:
    def test_valid_date(self):
        assert format_stedi_date("20260315") == "2026-03-15"

    def test_empty_date(self):
        assert format_stedi_date("") == ""

    def test_short_date(self):
        assert format_stedi_date("2026") == "2026"

    def test_invalid_date(self):
        assert format_stedi_date("NOTADATE") == "NOTADATE"


# ─── Service Adjustments ──────────────────────────────────────

class TestServiceAdjustments:
    def test_pr_deductible(self):
        adjs = [{"claimAdjustmentGroupCode": "PR", "adjustmentReasonCode1": "1",
                 "adjustmentAmount1": "50.00", "adjustmentReason1": "Deductible"}]
        result = parse_service_adjustments(adjs)
        assert result["Parsed PR Amount"] == 50.0
        assert result["Parsed Deductible Amount"] == 50.0

    def test_pr_coinsurance(self):
        adjs = [{"claimAdjustmentGroupCode": "PR", "adjustmentReasonCode1": "2",
                 "adjustmentAmount1": "30.00", "adjustmentReason1": "Coinsurance"}]
        result = parse_service_adjustments(adjs)
        assert result["Parsed PR Amount"] == 30.0
        assert result["Parsed Coinsurance Amount"] == 30.0

    def test_co_45_contractual(self):
        adjs = [{"claimAdjustmentGroupCode": "CO", "adjustmentReasonCode1": "45",
                 "adjustmentAmount1": "100.00", "adjustmentReason1": "Contractual"}]
        result = parse_service_adjustments(adjs)
        assert result["Parsed CO Amount"] == 100.0
        assert result["Parsed CO-45 Amount"] == 100.0

    def test_co_253_sequestration(self):
        adjs = [{"claimAdjustmentGroupCode": "CO", "adjustmentReasonCode1": "253",
                 "adjustmentAmount1": "8.50", "adjustmentReason1": "Sequestration"}]
        result = parse_service_adjustments(adjs)
        assert result["Parsed CO-253 Amount"] == 8.50

    def test_multiple_adjustments(self):
        adjs = [
            {"claimAdjustmentGroupCode": "CO", "adjustmentReasonCode1": "45",
             "adjustmentAmount1": "100.00", "adjustmentReason1": "Contractual"},
            {"claimAdjustmentGroupCode": "PR", "adjustmentReasonCode1": "1",
             "adjustmentAmount1": "25.00", "adjustmentReason1": "Deductible"},
        ]
        result = parse_service_adjustments(adjs)
        assert result["Parsed CO Amount"] == 100.0
        assert result["Parsed PR Amount"] == 25.0
        assert "CO-45" in result["Parsed Adjustment Codes"]
        assert "PR-1" in result["Parsed Adjustment Codes"]

    def test_empty_adjustments(self):
        result = parse_service_adjustments([])
        assert result["Parsed PR Amount"] == 0.0
        assert result["Parsed CO Amount"] == 0.0


# ─── Remark Codes ──────────────────────────────────────────────

class TestRemarkCodes:
    def test_single_remark(self):
        remarks = [{"remarkCode": "N790", "remark": "Missing auth number"}]
        result = parse_remark_codes(remarks)
        assert result["Parsed Remark Codes"] == "N790"
        assert "Missing auth" in result["Parsed Remark Text"]

    def test_multiple_remarks(self):
        remarks = [
            {"remarkCode": "N790", "remark": "Missing auth"},
            {"remarkCode": "MA130", "remark": "Incomplete info"},
        ]
        result = parse_remark_codes(remarks)
        assert "N790" in result["Parsed Remark Codes"]
        assert "MA130" in result["Parsed Remark Codes"]

    def test_empty_remarks(self):
        result = parse_remark_codes([])
        assert result["Parsed Remark Codes"] == ""


# ─── Single Claim ERA Parsing ─────────────────────────────────

class TestParseEraJson:
    def _sample_era(self):
        return {
            "claimPaymentInfo": {
                "patientControlNumber": "PCN_TEST_001",
                "claimStatusCode": "1",
                "claimPaymentAmount": "450.00",
                "patientResponsibilityAmount": "50.00",
                "totalClaimChargeAmount": "500.00",
                "payerClaimControlNumber": "PAYER_001",
            },
            "serviceLines": [
                {
                    "servicePaymentInformation": {
                        "adjudicatedProcedureCode": "A4239",
                        "lineItemProviderPaymentAmount": "450.00",
                        "lineItemChargeAmount": "500.00",
                    },
                    "serviceSupplementalAmounts": {"allowedActual": "500.00"},
                    "serviceDate": "20260316",
                    "lineItemControlNumber": "LINE001",
                    "serviceAdjustments": [
                        {
                            "claimAdjustmentGroupCode": "CO",
                            "adjustmentReasonCode1": "45",
                            "adjustmentAmount1": "50.00",
                            "adjustmentReason1": "Contractual",
                        }
                    ],
                    "healthCareCheckRemarkCodes": [],
                }
            ],
        }

    def test_parent_fields(self):
        result = parse_era_json(self._sample_era())
        parent = result["parent"]
        assert parent["raw_patient_control_num"] == "PCN_TEST_001"
        assert parent["primary_paid"] == 450.0
        assert parent["pr_amount"] == 50.0
        assert parent["primary_status"] == "1"

    def test_children_count(self):
        result = parse_era_json(self._sample_era())
        assert len(result["children"]) == 1

    def test_child_fields(self):
        result = parse_era_json(self._sample_era())
        child = result["children"][0]
        assert child["HCPC Code"] == "A4239"
        assert child["Primary Paid"] == 450.0
        assert child["Raw Allowed Actual"] == 500.0
        assert child["Raw Service Date"] == "2026-03-16"
        assert child["Parsed CO Amount"] == 50.0
        assert child["Parsed CO-45 Amount"] == 50.0


# ─── Full 835 String Parsing ──────────────────────────────────

class TestParseEraFromString:
    def test_flat_format(self):
        era = {
            "claimPaymentInfo": {
                "patientControlNumber": "FLAT_001",
                "claimStatusCode": "1",
                "claimPaymentAmount": "100.00",
                "patientResponsibilityAmount": "0",
                "totalClaimChargeAmount": "100.00",
            },
            "serviceLines": [],
        }
        rows = parse_era_from_string(json.dumps(era))
        assert len(rows) == 1
        assert rows[0]["parent"]["raw_patient_control_num"] == "FLAT_001"

    def test_stedi_api_format(self):
        era = {
            "transactions": [{
                "financialInformation": {"checkIssueOrEFTEffectiveDate": "20260315"},
                "paymentAndRemitReassociationDetails": {"checkOrEFTTraceNumber": "CHK001"},
                "detailInfo": [{
                    "paymentInfo": [{
                        "claimPaymentInfo": {
                            "patientControlNumber": "API_001",
                            "claimStatusCode": "1",
                            "claimPaymentAmount": "200.00",
                            "patientResponsibilityAmount": "25.00",
                            "totalClaimChargeAmount": "225.00",
                        },
                        "serviceLines": [{
                            "servicePaymentInformation": {
                                "adjudicatedProcedureCode": "E0784",
                                "lineItemProviderPaymentAmount": "200.00",
                                "lineItemChargeAmount": "225.00",
                            },
                            "serviceSupplementalAmounts": {"allowedActual": "225.00"},
                            "serviceDate": "20260316",
                            "lineItemControlNumber": "L001",
                            "serviceAdjustments": [],
                            "healthCareCheckRemarkCodes": [],
                        }],
                        "patientName": {"firstName": "John", "lastName": "Test"},
                    }]
                }]
            }]
        }
        rows = parse_era_from_string(json.dumps(era))
        assert len(rows) == 1
        assert rows[0]["parent"]["raw_patient_control_num"] == "API_001"
        assert rows[0]["parent"]["primary_paid"] == 200.0

    def test_invalid_json(self):
        rows = parse_era_from_string("not json at all")
        assert rows == []

    def test_empty_string(self):
        rows = parse_era_from_string("")
        assert rows == []

    def test_unknown_format(self):
        rows = parse_era_from_string(json.dumps({"random": "data"}))
        assert rows == []


# ─── Summarize for Monday ─────────────────────────────────────

class TestSummarize:
    def test_summarize(self):
        era_row = {
            "parent": {
                "primary_paid": 450.0,
                "pr_amount": 50.0,
                "paid_date": "2026-03-15",
                "primary_status": "1",
                "raw_patient_control_num": "PCN001",
                "raw_payer_claim_control": "PAYER001",
                "check_number": "CHK001",
            },
            "children": [{"HCPC Code": "A4239"}],
        }
        summary = summarize_era_row_for_monday(era_row)
        assert summary["primary_paid"] == 450.0
        assert summary["pr_amount"] == 50.0
        assert summary["check_number"] == "CHK001"
        assert len(summary["children"]) == 1


# ─── PCN Matching ─────────────────────────────────────────────

class TestMatching:
    def test_match_by_pcn(self):
        rows = [
            {"parent": {"raw_patient_control_num": "PCN_A"}},
            {"parent": {"raw_patient_control_num": "PCN_B"}},
        ]
        matched = match_era_rows_to_claim_item(rows, "PCN_A")
        assert len(matched) == 1
        assert matched[0]["parent"]["raw_patient_control_num"] == "PCN_A"

    def test_no_match(self):
        rows = [{"parent": {"raw_patient_control_num": "PCN_A"}}]
        matched = match_era_rows_to_claim_item(rows, "PCN_NOPE")
        assert len(matched) == 0

    def test_empty_pcn_returns_all(self):
        rows = [{"parent": {"raw_patient_control_num": "A"}}, {"parent": {"raw_patient_control_num": "B"}}]
        matched = match_era_rows_to_claim_item(rows, "")
        assert len(matched) == 2
