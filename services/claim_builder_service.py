"""
services/claim_builder_service.py
Converts Monday API order data into Stedi claim JSON payloads.
"""

import logging
import sys
import os
from copy import deepcopy
from services.stedi_service import lookup_payer_name, lookup_payer_name_by_internal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from claim_infrastructure import (
    build_normalized_order_template,
    group_normalized_orders_into_claims,
    build_stedi_claim_json,
    parse_address,
    normalize_date,
    normalize_gender,
    split_full_name,
    safe_str,
    build_service_line_from_normalized_order,
)

logger = logging.getLogger(__name__)

# ── Parent item column IDs ──────────────────────────────────────────────────────
COLUMN_MAP = {
    "status":               "claim_status",
    "text_mm18zjmz":        "gender",
    "text_mm187t6a":        "dob",
    "phone_mm18rr9v":       "phone",
    "location_mm187v29":    "patient_address",
    "color_mm189t0b":       "diagnosis_code",
    "color_mm18ds28":       "cgm_coverage",
    "text_mm18w2y4":        "doctor_name",
    "text_mm18x1kj":        "doctor_npi",
    "location_mm18qfed":    "doctor_address",
    "phone_mm18t5ct":       "doctor_phone",
    "color_mm18jhq5":       "primary_insurance",
    "text_mm18s3fe":        "member_id",
    "color_mm18h6yn":       "pr_payor",
    "text_mm18c6z4":        "secondary_id",
    "color_mm18h05q":       "subscription_type",
    "color_mm1bx9az":       "status_277",
    "text_mm1b56xa":        "rejected_reason_277",
}

# ── Subitem column IDs ──────────────────────────────────────────────────────────
SUBITEM_COLUMN_MAP = {
    "status":               "order_status",
    "date0":                "order_date",
    "color_mm18p9f4":       "primary_insurance",
    "text_mm18k1x8":        "plan_name",
    "text_mm18zcs4":        "member_id",
    "color_mm18fzt5":       "secondary_payor",
    "text_mm18qg5j":        "secondary_id",
    "numeric_mm18mwna":     "coinsurance_pct",
    "numeric_mm18mdhg":     "deductible",
    "numeric_mm18bvg0":     "deductible_remaining",
    "numeric_mm1879ha":     "oop_max",
    "numeric_mm18c79g":     "oop_max_remaining",
    "numeric_mm18t2q9":     "quantity",
    "color_mm185yjy":       "cgm_type",
    "color_mm18e5yq":       "pump_type",
    "color_mm18pj26":       "infusion_set",
    "text_mm18dsxx":        "auth_id",
}


def extract_columns(column_values: list) -> dict:
    """Convert Monday column_values list into a simple dict"""
    result = {}
    for col in column_values:
        col_id = col.get("id", "")
        field_name = COLUMN_MAP.get(col_id, col_id)
        result[field_name] = col.get("text", "") or ""
    return result


def extract_subitem_columns(column_values: list) -> dict:
    """Convert subitem column_values into a simple dict"""
    result = {}
    for col in column_values:
        col_id = col.get("id", "")
        field_name = SUBITEM_COLUMN_MAP.get(col_id, col_id)
        result[field_name] = col.get("text", "") or ""
    return result


def monday_item_to_normalized_orders(monday_item: dict) -> list[dict]:
    """
    Convert Monday order item into normalized order dicts
    for Brandon's pipeline. One order per subitem.
    """
    parent_cols = extract_columns(monday_item.get("column_values", []))

    patient_full_name = monday_item.get("name", "")
    patient_first, patient_last = split_full_name(patient_full_name)

    patient_addr = parse_address(parent_cols.get("patient_address", ""))
    doctor_full_name = parent_cols.get("doctor_name", "")
    doctor_first, doctor_last = split_full_name(doctor_full_name)
    doctor_addr = parse_address(parent_cols.get("doctor_address", ""))

    subitems = monday_item.get("subitems", [])
    if not subitems:
        logger.warning(f"No subitems for item {monday_item.get('id')} — cannot build claim")
        return []

    normalized_orders = []

    for subitem in subitems:
        sub_cols = extract_subitem_columns(subitem.get("column_values", []))

        order = build_normalized_order_template()

        # ── Patient ────────────────────────────────────────────
        order["source_parent_name"]     = patient_full_name
        order["source_child_name"]      = subitem.get("name", "")
        order["patient_full_name"]      = patient_full_name
        order["patient_first_name"]     = patient_first
        order["patient_last_name"]      = patient_last
        order["patient_dob"]            = normalize_date(parent_cols.get("dob", ""))
        order["patient_gender"]         = normalize_gender(parent_cols.get("gender", ""))
        order["patient_phone"]          = parent_cols.get("phone", "")
        order["patient_address_1"]      = patient_addr.get("address1", "")
        order["patient_address_2"]      = patient_addr.get("address2", "")
        order["patient_city"]           = patient_addr.get("city", "")
        order["patient_state"]          = patient_addr.get("state", "")
        order["patient_postal_code"]    = patient_addr.get("postal_code", "")

        # ── Insurance ──────────────────────────────────────────
        # Use subitem values first, fall back to parent

        # order["primary_insurance_name"] = (
        #     sub_cols.get("primary_insurance") or
        #     parent_cols.get("primary_insurance", "")
        # )

        subitem_payer_id = sub_cols.get("payer_id", "")
        subitem_insurance = (
                sub_cols.get("primary_insurance") or
                parent_cols.get("primary_insurance", "")
        )

        official_payer_name = ""
        if subitem_payer_id:
            official_payer_name = lookup_payer_name(subitem_payer_id)

        order["primary_insurance_name"] = (
                official_payer_name or
                subitem_insurance or
                ""
        )

        order["member_id"] = (
                sub_cols.get("member_id") or
                parent_cols.get("member_id", "")
        )

        order["secondary_member_id"]    = (
            sub_cols.get("secondary_id") or
            parent_cols.get("secondary_id", "")
        )
        order["subscription_type"]      = parent_cols.get("subscription_type", "")
        order["diagnosis_code"]         = parent_cols.get("diagnosis_code", "")
        order["cgm_coverage"]           = parent_cols.get("cgm_coverage", "")

        order["group_number"] = ""
        order["subscriber_group_name"] = ""

        # ── Doctor ─────────────────────────────────────────────
        order["doctor_name"]            = doctor_full_name
        order["doctor_first_name"]      = doctor_first
        order["doctor_last_name"]       = doctor_last
        order["doctor_npi"]             = parent_cols.get("doctor_npi", "")
        order["doctor_address_1"]       = doctor_addr.get("address1", "")
        order["doctor_address_2"]       = doctor_addr.get("address2", "")
        order["doctor_city"]            = doctor_addr.get("city", "")
        order["doctor_state"]           = doctor_addr.get("state", "")
        order["doctor_postal_code"]     = doctor_addr.get("postal_code", "")
        order["doctor_phone"]           = parent_cols.get("doctor_phone", "")

        # ── Service Line ───────────────────────────────────────
        order["order_status"]           = sub_cols.get("order_status", "")
        order["order_date"]             = normalize_date(sub_cols.get("order_date", ""))
        order["service_date"]           = normalize_date(sub_cols.get("order_date", ""))
        order["quantity"]               = sub_cols.get("quantity", "")
        order["auth_id"]                = sub_cols.get("auth_id", "")
        order["item"]                   = subitem.get("name", "")

        # ── Product variant ────────────────────────────────────
        cgm_type     = sub_cols.get("cgm_type", "")
        pump_type    = sub_cols.get("pump_type", "")
        infusion_set = sub_cols.get("infusion_set", "")
        order["variant"] = cgm_type or pump_type or infusion_set or ""

        logger.info(
            f"Normalized: {patient_full_name} | "
            f"subitem={subitem.get('name')} | "
            f"insurance={order['primary_insurance_name']} | "
            f"member_id={order['member_id']} | "
            f"service_date={order['service_date']} | "
            f"quantity={order['quantity']}"
        )

        normalized_orders.append(order)

    return normalized_orders

# ── Claims Board column IDs ────────────────────────────────────────────────────
# Parent columns on Claims Board that contain claim-level data
# VERIFIED against live Monday board (2026-03-31)
CLAIMS_BOARD_COLUMN_MAP = {
    "text_mktat89m":     "member_id",
    "text_mkp3y5ax":     "dob",
    "text_mkxr2r9b":     "doctor_npi",
    "text_mkxrh4a4":     "doctor_name",
    "text_mkwzbcme":     "correlation_id",       # Customer Order / Stedi PCN
    "text_mm1gkf40":     "raw_pcn",              # Raw Patient Control Number
    "date_mkwr7spz":     "dos",
    "date_mm14rk8d":     "claim_sent_date",
    "color_mky2gpz5":    "diagnosis_code",        # Status type (ICD-10)
    "location_mkxxpesw": "patient_address",        # Location type
    "location_mkxr251b": "doctor_address",         # Location type
    "color_mky1qvcf":    "subscription_type",      # Status type
    "color_mkxmmm77":    "insurance_type",          # Status (Commercial/Medicaid/Medicare)
    "color_mkxmywtb":    "primary_status",          # Primary claim status
    "color_mkxmhypt":    "pr_payor",               # Primary Payor (e.g. "Anthem BCBS Commercial")
    "color_mm1zy5f2":    "gender",                 # Gender (Male/Female)
    "color_mm1ze7b4":    "cgm_coverage",           # CGM Coverage (Insulin/Hypo)
    "phone_mm1znnww":    "patient_phone",           # Patient Phone
    "phone_mm1zy789":    "doctor_phone",            # Doctor Phone
    "text_mkxwcqfy":     "secondary_id",            # Secondary ID
    "color_mkxq1a2p":    "secondary_payer",         # Secondary Payer (status)
    "numeric_mm15t7ed":  "frequency_number",        # Frequency number (e.g. 90)
    "numeric_mky1xhgp":  "total_infusion_qty",      # Total Infusion Qty
}

# VERIFIED against live Monday board (2026-03-31)
# Subitem columns on Claims Board (pre-computed product data)
CLAIMS_BOARD_SUBITEM_MAP = {
    "color_mm1cdvq8":       "hcpc_code",         # HCPC Code (STATUS type — text returns code)
    "numeric_mm1czbyg":     "order_qty",          # Order Quantity (writable number)
    "formula_mm1cv57q":     "claim_qty",          # Claim Quantity (FORMULA — read only)
    "formula_mm1c7nen":     "est_pay_formula",    # Est. Pay formula (FORMULA — read only, may be empty via API)
    "numeric_mm1zspsy":     "est_pay",            # Est Pay (writable numeric — written by handler)
    "numeric_mm1za8v5":     "charge_amount",      # Charge Amount (writable numeric — written by handler)
    "color_mm1cjcmg":       "primary_insurance",  # Primary Insurance (status)
    "color_mm1cnfsb":       "order_frequency",    # Order Frequency (status)
}


def extract_claims_board_columns(column_values: list) -> dict:
    """Convert Claims Board parent column_values into a semantic dict."""
    result = {}
    for col in column_values:
        col_id = col.get("id", "")
        field_name = CLAIMS_BOARD_COLUMN_MAP.get(col_id, col_id)
        result[field_name] = col.get("text", "") or ""
    return result


def extract_claims_board_subitem_columns(column_values: list) -> dict:
    """Convert Claims Board subitem column_values into a semantic dict."""
    result = {}
    for col in column_values:
        col_id = col.get("id", "")
        field_name = CLAIMS_BOARD_SUBITEM_MAP.get(col_id, col_id)
        result[field_name] = col.get("text", "") or ""
    return result


def claims_board_item_to_normalized_orders(claims_item: dict) -> list[dict]:
    """
    Convert a Claims Board item (parent + 5 product subitems) into
    normalized order dicts for the claim builder pipeline.

    Key difference from Order Board flow: subitems have PRE-COMPUTED
    HCPC codes, units, modifiers, and charge amounts. These are read
    directly and passed through to the service line builder, which
    respects them instead of re-computing.
    """
    parent_cols = extract_claims_board_columns(claims_item.get("column_values", []))

    # Claims Board parent has limited patient info — get what's available
    raw_name = claims_item.get("name", "")
    patient_full_name = raw_name
    payer_from_name = ""
    # Name format on Claims Board: "Patient Name - Payer" — extract both
    if " - " in raw_name:
        parts = raw_name.split(" - ", 1)
        patient_full_name = parts[0].strip()
        payer_from_name = parts[1].strip()

    # Primary Payor column is the authoritative source for payer name
    # Falls back to item name suffix if the column is empty
    payer_name = parent_cols.get("pr_payor", "") or payer_from_name

    patient_first, patient_last = split_full_name(patient_full_name)

    doctor_full_name = parent_cols.get("doctor_name", "")
    doctor_first, doctor_last = split_full_name(doctor_full_name)

    subitems = claims_item.get("subitems", [])
    if not subitems:
        logger.warning(f"No subitems for Claims Board item {claims_item.get('id')} — cannot build claim")
        return []

    normalized_orders = []

    for subitem in subitems:
        sub_cols = extract_claims_board_subitem_columns(subitem.get("column_values", []))

        # Skip subitems with no HCPC code (product not ordered)
        # HCPC Code is a STATUS column — Monday returns the label text (e.g. "E0784")
        hcpc_code = sub_cols.get("hcpc_code", "").strip()
        if not hcpc_code:
            continue

        order = build_normalized_order_template()

        # Patient info from parent
        order["source_parent_name"]     = claims_item.get("name", "")
        order["source_child_name"]      = subitem.get("name", "")
        order["patient_full_name"]      = patient_full_name
        order["patient_first_name"]     = patient_first
        order["patient_last_name"]      = patient_last
        order["patient_dob"]            = normalize_date(parent_cols.get("dob", ""))
        gender_raw = parent_cols.get("gender", "")
        order["patient_gender"]         = normalize_gender(gender_raw) if gender_raw else "U"
        order["patient_phone"]          = parent_cols.get("patient_phone", "")

        # Patient address
        patient_addr_raw = parent_cols.get("patient_address", "")
        if patient_addr_raw:
            patient_addr = parse_address(patient_addr_raw)
            order["patient_address_1"]   = patient_addr.get("address1", "")
            order["patient_address_2"]   = patient_addr.get("address2", "")
            order["patient_city"]        = patient_addr.get("city", "")
            order["patient_state"]       = patient_addr.get("state", "")
            order["patient_postal_code"] = patient_addr.get("postal_code", "")

        # Insurance — payer from Primary Payor column (or item name suffix fallback)
        order["member_id"] = parent_cols.get("member_id", "")
        order["primary_insurance_name"] = payer_name
        order["payer_name"] = payer_name
        order["diagnosis_code"]     = parent_cols.get("diagnosis_code", "")
        order["cgm_coverage"]       = parent_cols.get("cgm_coverage", "")
        order["subscription_type"]  = parent_cols.get("subscription_type", "")
        order["secondary_member_id"] = parent_cols.get("secondary_id", "")

        # Doctor — doctor_phone now comes from phone column on Claims Board
        order["doctor_name"]        = doctor_full_name
        order["doctor_first_name"]  = doctor_first
        order["doctor_last_name"]   = doctor_last
        order["doctor_npi"]         = parent_cols.get("doctor_npi", "")
        order["doctor_phone"]       = parent_cols.get("doctor_phone", "")  # phone_mm1zy789

        # Doctor address
        doctor_addr_raw = parent_cols.get("doctor_address", "")
        if doctor_addr_raw:
            doctor_addr = parse_address(doctor_addr_raw, drop_units=True)
            order["doctor_address_1"]    = doctor_addr.get("address1", "")
            order["doctor_address_2"]    = doctor_addr.get("address2", "")
            order["doctor_city"]         = doctor_addr.get("city", "")
            order["doctor_state"]        = doctor_addr.get("state", "")
            order["doctor_postal_code"]  = doctor_addr.get("postal_code", "")

        # Service date from parent DOS
        dos = parent_cols.get("dos", "")
        order["order_date"]    = normalize_date(dos)
        order["service_date"]  = normalize_date(dos)

        # Product info from subitem
        order["item"]     = subitem.get("name", "")
        # Use claim_qty (formula) if available, fall back to order_qty
        order["quantity"] = sub_cols.get("claim_qty", "") or sub_cols.get("order_qty", "")

        # PRE-COMPUTED values — these bypass the resolver functions
        # HCPC Code comes from status column (text contains the code like "E0784")
        order["pre_computed_hcpc"]      = hcpc_code

        # Units: prefer claim_qty (formula — may be empty via API in production),
        # fall back to order_qty (writable — always populated by handler)
        pre_units = sub_cols.get("claim_qty", "") or sub_cols.get("order_qty", "")
        order["pre_computed_units"]     = pre_units

        # Charge: prefer writable charge_amount column, then est_pay (writable),
        # then formula est_pay_formula (may be empty via API)
        pre_charge = (
            sub_cols.get("charge_amount", "") or
            sub_cols.get("est_pay", "") or
            sub_cols.get("est_pay_formula", "")
        )
        order["pre_computed_charge"]    = pre_charge

        # NOTE: No modifiers column exists on Claims Board subitems.
        # Modifiers will be computed by resolver functions as fallback.
        order["pre_computed_modifiers"] = []

        logger.info(
            f"Claims Board normalized: {patient_full_name} | "
            f"subitem={subitem.get('name')} | "
            f"hcpc={hcpc_code} | "
            f"payer={payer_name} | "
            f"units={pre_units} | "
            f"charge={pre_charge}"
        )

        normalized_orders.append(order)

    return normalized_orders


def build_claims_from_claims_board_item(claims_item: dict) -> list[dict]:
    """
    Main entry point for Claims Board flow.
    Claims Board item → Stedi claim JSON payloads.

    Uses pre-computed HCPC/units/modifiers/charges from subitems.
    """
    item_id = claims_item.get("id")
    item_name = claims_item.get("name")

    logger.info(f"Building claims from Claims Board: {item_name} (id={item_id})")

    normalized_orders = claims_board_item_to_normalized_orders(claims_item)
    if not normalized_orders:
        logger.warning(f"No normalized orders for Claims Board item {item_id}")
        return []

    logger.info(f"Normalized {len(normalized_orders)} service lines from Claims Board")

    grouped_claims = group_normalized_orders_into_claims(normalized_orders)
    logger.info(f"Grouped into {len(grouped_claims)} claim(s)")

    stedi_payloads = []
    for claim in grouped_claims:
        try:
            payload = build_stedi_claim_json(claim)

            # Remove groupNumber and subscriberGroupName
            subscriber = payload.get("subscriber", {})
            subscriber.pop("groupNumber", None)
            subscriber.pop("subscriberGroupName", None)

            # Format charge amounts
            payload = format_charge_amounts(payload)

            # Replace tradingPartnerName with official Stedi name
            payer_id = payload.get("tradingPartnerServiceId", "")
            official_name = get_official_payer_name(payer_id)
            if official_name:
                payload["tradingPartnerName"] = official_name
                payload["receiver"] = {"organizationName": official_name}

            stedi_payloads.append(payload)
            logger.info(f"Built Claims Board payload: {claim.get('claim_key')}")

        except Exception as e:
            logger.error(f"Failed to build Stedi JSON from Claims Board: {e}", exc_info=True)

    logger.info(f"Total Claims Board payloads: {len(stedi_payloads)}")
    return stedi_payloads


def build_claims_from_monday_item(monday_item: dict) -> list[dict]:
    """Main entry point. Monday item → Stedi claim JSON payloads."""
    item_id = monday_item.get("id")
    patient_name = monday_item.get("name")

    logger.info(f"Building claims for: {patient_name} (id={item_id})")

    normalized_orders = monday_item_to_normalized_orders(monday_item)
    if not normalized_orders:
        logger.warning(f"No normalized orders for item {item_id}")
        return []

    logger.info(f"Normalized {len(normalized_orders)} service lines")

    grouped_claims = group_normalized_orders_into_claims(normalized_orders)
    logger.info(f"Grouped into {len(grouped_claims)} claim(s)")

    stedi_payloads = []
    for claim in grouped_claims:
        try:
            payload = build_stedi_claim_json(claim)

            # Remove groupNumber and subscriberGroupName
            subscriber = payload.get("subscriber", {})
            subscriber.pop("groupNumber", None)
            subscriber.pop("subscriberGroupName", None)

            # Format all charge amounts to 2 decimal places
            payload = format_charge_amounts(payload)

            # Replace tradingPartnerName with official Stedi name
            # using the hardcoded mapping from claim_assumptions.py
            payer_id = payload.get("tradingPartnerServiceId", "")
            official_name = get_official_payer_name(payer_id)
            if official_name:
                payload["tradingPartnerName"] = official_name
                payload["receiver"] = {"organizationName": official_name}
                logger.info(f"tradingPartnerName: payer_id={payer_id} → '{official_name}'")

            stedi_payloads.append(payload)
            logger.info(f"Built payload: {claim.get('claim_key')}")

        except Exception as e:
            logger.error(f"Failed to build Stedi JSON: {e}", exc_info=True)

    logger.info(f"Total payloads: {len(stedi_payloads)}")
    return stedi_payloads


def get_official_payer_name(payer_id: str) -> str:
    """
    Get official Stedi tradingPartnerName from payer ID.
    Uses hardcoded mapping from claim_assumptions.py.
    """
    try:
        from claim_assumptions import STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID
        name = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(payer_id, "")
        if not name:
            logger.warning(f"No official name for payer_id={payer_id} — using internal name")
        return name
    except Exception as e:
        logger.warning(f"Could not load STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID: {e}")
        return ""


def format_charge_amounts(payload: dict) -> dict:
    """Ensure all charge amounts are formatted with 2 decimal places."""
    claim_info = payload.get("claimInformation", {})
    if "claimChargeAmount" in claim_info:
        try:
            claim_info["claimChargeAmount"] = f"{float(claim_info['claimChargeAmount']):.2f}"
        except (ValueError, TypeError):
            pass
    for line in claim_info.get("serviceLines", []):
        svc = line.get("professionalService", {})
        if "lineItemChargeAmount" in svc:
            try:
                svc["lineItemChargeAmount"] = f"{float(svc['lineItemChargeAmount']):.2f}"
            except (ValueError, TypeError):
                pass
    return payload
