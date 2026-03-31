"""
claims_board_config.py
======================
Centralized configuration for the Claims Board migration.

Contains:
- New Order Board column ID mappings
- Claims Board parent column ID mappings
- Claims Board subitem column ID mappings
- Product category definitions (5 product subitems)
- Status label indexes for Claims Board
- Reverse write maps for ERA → Monday
- SUBMISSION_SOURCE routing helper

COLUMN ID CONVENTION:
  - Columns prefixed with "cb_" are Claims Board placeholders
  - Columns prefixed with "nob_" are New Order Board placeholders
  - Replace these with real Monday column IDs after board setup
  - Real column IDs look like "text_mkwzbcme" or "numeric_mm115q76"
"""

import os
import logging

logger = logging.getLogger(__name__)


# ============================================================
# SUBMISSION SOURCE ROUTING
# ============================================================

def get_submission_source() -> str:
    """
    Determine where claims originate from.
    Returns "claims_board" or "order_board".
    Default: "order_board" (legacy behavior).
    """
    return os.getenv("SUBMISSION_SOURCE", "order_board").lower().strip()


def is_claims_board_mode() -> bool:
    """True when the system should use the new Claims Board flow."""
    return get_submission_source() == "claims_board"


# ============================================================
# BOARD IDS
# ============================================================

def get_order_board_id() -> str:
    return os.getenv("MONDAY_ORDER_BOARD_ID", "")

def get_claims_board_id() -> str:
    return os.getenv("MONDAY_CLAIMS_BOARD_ID", "")

def get_new_order_board_id() -> str:
    return os.getenv("MONDAY_NEW_ORDER_BOARD_ID", "")


# ============================================================
# NEW ORDER BOARD — COLUMN MAPPINGS
# ============================================================
# The New Order Board is FLAT (no subitems).
# Product quantities and types are parent-level columns.

NEW_ORDER_BOARD_COLUMN_MAP = {
    # Patient info
    "nob_patient_name":       "patient_name",        # Item name (from monday_item["name"])
    "nob_gender":             "gender",
    "nob_dob":                "dob",
    "nob_phone":              "phone",
    "nob_patient_address":    "patient_address",
    "nob_diagnosis_code":     "diagnosis_code",
    "nob_cgm_coverage":       "cgm_coverage",

    # Doctor info
    "nob_doctor_name":        "doctor_name",
    "nob_doctor_npi":         "doctor_npi",
    "nob_doctor_address":     "doctor_address",
    "nob_doctor_phone":       "doctor_phone",

    # Insurance
    "nob_primary_insurance":  "primary_insurance",
    "nob_member_id":          "member_id",
    "nob_secondary_id":       "secondary_id",
    "nob_subscription_type":  "subscription_type",

    # Product quantities (flat — no subitems)
    "nob_pump_qty":           "pump_qty",
    "nob_infusion_set_qty":   "infusion_set_qty",
    "nob_cartridge_qty":      "cartridge_qty",
    "nob_cgm_sensor_qty":     "cgm_sensor_qty",
    "nob_cgm_monitor_qty":    "cgm_monitor_qty",

    # Product variants
    "nob_pump_type":          "pump_type",
    "nob_cgm_type":           "cgm_type",
    "nob_infusion_set_type":  "infusion_set_type",

    # Order metadata
    "nob_order_date":         "order_date",
    "nob_order_status":       "order_status",
    "nob_auth_id":            "auth_id",
}


# ============================================================
# CLAIMS BOARD — PARENT COLUMN MAPPINGS
# ============================================================
# These are the columns on the Claims Board parent item.

CLAIMS_BOARD_PARENT_COLUMN_MAP = {
    # Patient
    "text_mktat89m":   "member_id",
    "text_mkp3y5ax":   "dob",
    "text_mkxr2r9b":   "doctor_npi",
    "text_mkxrh4a4":   "doctor_name",
    "text_mkwzbcme":   "correlation_id",       # Stedi correlation ID / PCN

    # Additional patient/claim fields needed for Stedi submission
    "cb_gender":           "gender",            # placeholder
    "cb_patient_address":  "patient_address",   # placeholder
    "cb_diagnosis_code":   "diagnosis_code",    # placeholder
    "cb_cgm_coverage":     "cgm_coverage",      # placeholder
    "cb_doctor_address":   "doctor_address",    # placeholder
    "cb_doctor_phone":     "doctor_phone",      # placeholder
    "cb_subscription_type":"subscription_type",  # placeholder

    # Dates
    "date_mkwr7spz":   "dos",                  # Date of Service
    "date_mm14rk8d":   "claim_sent_date",

    # ERA parent fields (populated by 835 handler)
    "numeric_mm115q76": "primary_paid",         # Primary Paid (A)
    "numeric_mkxmc2rh": "pr_amount",            # PR Amount (C)
    "date_mm11zg2f":    "paid_date",            # Primary Paid Date
    "text_mm11m3fh":    "check_number",         # Check #
    "text_mkzck8tw":    "primary_status",       # Primary Status
    "text_mm0fa4vk":    "raw_pcn",              # Raw Patient Control #

    # 277 status fields (populated by 277 handler)
    "cb_277_status":    "status_277",           # placeholder — needs real column ID
    "cb_277_reason":    "rejection_reason_277",  # placeholder

    # Workflow / claim status
    "cb_claim_status":  "claim_status",         # placeholder — needs real column ID
}

# Reverse map: semantic name → column ID for writing
CLAIMS_BOARD_PARENT_WRITE_MAP = {v: k for k, v in CLAIMS_BOARD_PARENT_COLUMN_MAP.items()}


# ============================================================
# CLAIMS BOARD — SUBITEM COLUMN MAPPINGS
# ============================================================
# These columns exist on each of the 5 pre-populated product subitems.

CLAIMS_BOARD_SUBITEM_COLUMN_MAP = {
    # Pre-computed claim fields (populated during order → claims migration)
    "cb_sub_hcpc_code":       "hcpc_code",
    "cb_sub_claim_qty":       "claim_qty",
    "cb_sub_modifiers":       "modifiers",
    "cb_sub_est_pay":         "est_pay",
    "cb_sub_charge_amount":   "charge_amount",
    "cb_sub_units":           "units",

    # ERA fields (populated by 835 handler — UPDATE, not create)
    "numeric_mm1czbyg":       "era_primary_paid",
    "date_mm11hscn":          "era_service_date",
    "numeric_mm11v6th":       "era_charge_amount",
    "text_mm16qhea":          "era_patient_control_num",
    "text_mm1gzsan":          "era_claim_status_code",
    "text_mm1g4yd9":          "era_line_control_num",
    "numeric_mm1gg3pj":       "era_allowed_actual",
    "numeric_mm1gtdts":       "era_pr_amount",
    "numeric_mm1gredn":       "era_deductible",
    "numeric_mm1g3nvh":       "era_coinsurance",
    "numeric_mm11aqr1":       "era_copay",
    "numeric_mm1gtd3e":       "era_other_pr",
    "numeric_mm1g48c":        "era_co_amount",
    "numeric_mm1gken":        "era_co_45",
    "numeric_mm1gt3ky":       "era_co_253",
    "numeric_mm1g3vgp":       "era_other_co",
    "numeric_mm1grbc3":       "era_oa_amount",
    "numeric_mm1gh22d":       "era_pi_amount",
    "text_mm1g6tw3":          "era_remark_codes",
    "long_text_mm1ggyz6":     "era_remark_text",
    "text_mm1gt1dh":          "era_adj_codes",
    "long_text_mm1g7xmy":     "era_adj_reasons",
}

# Reverse map: semantic name → column ID for writing
CLAIMS_BOARD_SUBITEM_WRITE_MAP = {v: k for k, v in CLAIMS_BOARD_SUBITEM_COLUMN_MAP.items()}


# ============================================================
# 5 PRODUCT CATEGORIES
# ============================================================
# The Claims Board has 5 pre-populated subitems per parent.
# Each subitem represents one product category.

PRODUCT_CATEGORIES = [
    {
        "name": "Insulin Pump",
        "fixed_hcpc": "E0784",
        "item_names": ["Insulin Pump"],
        "qty_field": "pump_qty",
        "variant_field": "pump_type",
    },
    {
        "name": "Infusion Set",
        "fixed_hcpc": None,  # Payer-dependent: A4224, A4230, A4231
        "item_names": ["Infusion Set 1", "Infusion Set"],
        "qty_field": "infusion_set_qty",
        "variant_field": "infusion_set_type",
    },
    {
        "name": "Cartridge",
        "fixed_hcpc": None,  # Payer-dependent: A4225, A4232
        "item_names": ["Cartridge", "Cartridges"],
        "qty_field": "cartridge_qty",
        "variant_field": None,
    },
    {
        "name": "CGM Sensors",
        "fixed_hcpc": "A4239",
        "item_names": ["CGM Sensors"],
        "qty_field": "cgm_sensor_qty",
        "variant_field": "cgm_type",
    },
    {
        "name": "CGM Monitor",
        "fixed_hcpc": "E2103",
        "item_names": ["CGM Monitor"],
        "qty_field": "cgm_monitor_qty",
        "variant_field": None,
    },
]

# Quick lookup: HCPC code → product category name
HCPC_TO_PRODUCT = {}
for cat in PRODUCT_CATEGORIES:
    if cat["fixed_hcpc"]:
        HCPC_TO_PRODUCT[cat["fixed_hcpc"]] = cat["name"]

# Payer-dependent HCPC codes also map to products
PAYER_DEPENDENT_HCPCS = {
    "A4224": "Infusion Set",
    "A4230": "Infusion Set",
    "A4231": "Infusion Set",
    "A4225": "Cartridge",
    "A4232": "Cartridge",
}
HCPC_TO_PRODUCT.update(PAYER_DEPENDENT_HCPCS)


# ============================================================
# CLAIMS BOARD STATUS INDEXES
# ============================================================
# Label index → status text for the Claims Board status column.
# These need to be verified against the actual Monday board settings.

CLAIMS_BOARD_STATUS_TO_INDEX = {
    "Pending":       "0",
    "Submitted":     "1",
    "Accepted":      "2",
    "Rejected":      "3",
    "Paid":          "4",
    "Denied":        "5",
}

CLAIMS_BOARD_277_STATUS_TO_INDEX = {
    "Pending":       "0",
    "Accepted":      "1",
    "Rejected":      "2",
}


# ============================================================
# ORDER BOARD → CLAIMS BOARD FIELD MAPPING
# ============================================================
# Maps Order Board column IDs to Claims Board column IDs
# Used when migrating data from old Order Board to Claims Board.

ORDER_TO_CLAIMS_FIELD_MAP = {
    "text_mm18s3fe":  "text_mktat89m",   # Member ID
    "text_mm187t6a":  "text_mkp3y5ax",   # DOB
    "text_mm18x1kj":  "text_mkxr2r9b",   # NPI
    "text_mm18w2y4":  "text_mkxrh4a4",   # Doctor Name
}


# ============================================================
# INFUSION SET 2 LOGIC
# ============================================================

def needs_infusion_set_2(infusion_set_qty: int) -> bool:
    """
    Determine if a second Infusion Set subitem is needed.
    Some payers require a separate line for certain infusion sets.
    This is a placeholder — actual logic depends on business rules.
    """
    # For now, only needed if explicitly flagged
    # TODO: Implement payer-specific infusion set 2 rules
    return False


# ============================================================
# VALIDATION HELPERS
# ============================================================

def validate_claims_board_config():
    """
    Validate that all required env vars are set for Claims Board mode.
    Returns list of missing/placeholder items.
    """
    issues = []

    if not get_claims_board_id():
        issues.append("MONDAY_CLAIMS_BOARD_ID not set")

    if is_claims_board_mode() and not get_new_order_board_id():
        issues.append("MONDAY_NEW_ORDER_BOARD_ID not set (required in claims_board mode)")

    # Check for placeholder column IDs still in use
    placeholder_columns = []
    for col_id in CLAIMS_BOARD_PARENT_COLUMN_MAP:
        if col_id.startswith("cb_") or col_id.startswith("nob_"):
            placeholder_columns.append(col_id)

    for col_id in CLAIMS_BOARD_SUBITEM_COLUMN_MAP:
        if col_id.startswith("cb_") or col_id.startswith("nob_"):
            placeholder_columns.append(col_id)

    for col_id in NEW_ORDER_BOARD_COLUMN_MAP:
        if col_id.startswith("nob_"):
            placeholder_columns.append(col_id)

    if placeholder_columns:
        issues.append(
            f"{len(placeholder_columns)} placeholder column IDs remain "
            f"(prefixed cb_/nob_). Replace with real Monday column IDs."
        )

    if issues:
        logger.warning(f"Claims Board config issues: {issues}")

    return issues
