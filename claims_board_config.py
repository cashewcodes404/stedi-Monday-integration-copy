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

# VERIFIED against live Monday board (2026-03-31)
# New Order Board (18403054769) was duplicated from Order Board — shares column IDs.
# It has subitems (same structure as Order Board), NOT flat.
NEW_ORDER_BOARD_COLUMN_MAP = {
    # Patient info
    "status":               "claim_status",
    "color_mm1svmyk":       "gender",              # Status type (Male/Female)
    "text_mm187t6a":        "dob",
    "phone_mm18rr9v":       "phone",
    "location_mm187v29":    "patient_address",
    "color_mm189t0b":       "diagnosis_code",       # Status type
    "color_mm18ds28":       "cgm_coverage",         # Status type (Hypo/Insulin)

    # Doctor info
    "text_mm18w2y4":        "doctor_name",
    "text_mm18x1kj":        "doctor_npi",
    "location_mm18qfed":    "doctor_address",
    "phone_mm18t5ct":       "doctor_phone",

    # Insurance
    "color_mm18jhq5":       "primary_insurance",    # Status type
    "text_mm18s3fe":        "member_id",
    "color_mm18h6yn":       "pr_payor",
    "text_mm18c6z4":        "secondary_id",
    "color_mm18h05q":       "subscription_type",    # Status type

    # 277 / Claim tracking
    "color_mm1bx9az":       "status_277",
    "text_mm1b56xa":        "rejection_reason_277",
    "text_mm1ra2v1":        "claim_id",

    # Order metadata
    "date_mm1ssf5g":        "dos",                  # Date of Service
}


# ============================================================
# CLAIMS BOARD — PARENT COLUMN MAPPINGS
# ============================================================
# These are the columns on the Claims Board parent item.

# VERIFIED against live Monday board (2026-03-31)
# Claims Board (18245429780)
CLAIMS_BOARD_PARENT_COLUMN_MAP = {
    # Patient / Insurance
    "text_mktat89m":    "member_id",
    "text_mkp3y5ax":    "dob",
    "color_mkxmhypt":   "primary_payor",        # Status type (18 payer labels)
    "text_mm1gcz3y":    "pr_payor_id",
    "color_mky2gpz5":   "diagnosis_code",        # Status type (ICD-10 codes)
    "location_mkxxpesw":"patient_address",        # Location type
    "color_mky1qvcf":   "subscription_type",      # Status type (CGM/Pump/Both)
    "color_mkxmmm77":   "insurance_type",         # Status type (Commercial/Medicaid/Medicare)

    # Doctor
    "text_mkxrh4a4":    "doctor_name",
    "text_mkxr2r9b":    "doctor_npi",
    "location_mkxr251b":"doctor_address",

    # Correlation / tracking
    "text_mkwzbcme":    "correlation_id",         # Customer Order (Stedi correlation ID / PCN)
    "text_mm1gkf40":    "raw_pcn",                # Raw Patient Control Number
    "text_mm1gefbz":    "raw_payer_claim_control", # Raw Payer Claim Control Number

    # Dates
    "date_mkwr7spz":    "dos",                    # Date of Service
    "date_mm14rk8d":    "claim_sent_date",

    # ERA parent fields (populated by 835 handler)
    "numeric_mm115q76": "primary_paid",           # Primary Paid (A)
    "numeric_mkxmc2rh": "pr_amount",              # PR Amount (C)
    "date_mm11zg2f":    "paid_date",              # Primary Paid Date (D)
    "text_mm11m3fh":    "check_number",           # Check #
    "text_mkzck8tw":    "primary_status_text",    # Primary --> (text field)
    "color_mkxmywtb":   "primary_status",         # Primary (status: Outstanding/Paid/Denied/etc.)

    # ERA amounts
    "numeric_mm1ghydj": "raw_claim_charge_amount",
    "text_mm1gz8ss":    "raw_remittance_trace",
    "numeric_mm1gdpjq": "raw_remittance_total",

    # Product unit columns (on parent)
    "numeric_mkwz41cr": "e0784_units",            # Pump units
    "numeric_mkwzb2f4": "e2103_units",            # Monitor units
    "numeric_mkwz251j": "a4239_units",            # Sensor units
    "numeric_mkwz4zkt": "pump_qty",
    "numeric_mkwz337y": "infusion_1_qty",
    "numeric_mkwz9g9f": "infusion_2_qty",
    "numeric_mkwzr5js": "monitor_qty",

    # Workflow / claim status
    "color_mm11nnfy":   "primary_era_status",     # Primary ERA --> (Working on it/Primary ERA)
    "color_mm1j794n":   "workflow",               # Workflow --> section header

    # NOTE: No dedicated 277 Status column exists on Claims Board.
    # 277 status is tracked via primary_status (color_mkxmywtb).
    # No Gender column, No CGM Coverage column, No Doctor Phone column.
}

# Reverse map: semantic name → column ID for writing
CLAIMS_BOARD_PARENT_WRITE_MAP = {v: k for k, v in CLAIMS_BOARD_PARENT_COLUMN_MAP.items()}


# ============================================================
# CLAIMS BOARD — SUBITEM COLUMN MAPPINGS
# ============================================================
# These columns exist on each of the 5 pre-populated product subitems.

# VERIFIED against live Monday board (2026-03-31)
# Claims Board Subitems (board 18245429979)
# NOTE: HCPC Code is a STATUS column (dropdown), not text.
#       Claim Quantity and Est. Pay are FORMULAS (read-only).
#       No Modifiers column exists on the board.
CLAIMS_BOARD_SUBITEM_COLUMN_MAP = {
    # Pre-populated product fields
    "color_mm1cdvq8":         "hcpc_code",          # HCPC Code (STATUS type — see HCPC_STATUS_INDEX)
    "numeric_mm1czbyg":       "order_qty",           # Order Quantity (writable)
    "formula_mm1cv57q":       "claim_qty",           # Claim Quantity (FORMULA — read only!)
    "formula_mm1c7nen":       "est_pay",             # Est. Pay (FORMULA — read only!)
    "color_mm1cjcmg":         "primary_insurance",   # Primary Insurance (status)
    "color_mm1cnfsb":         "order_frequency",     # Order Frequency (status)
    "color_mm1148h5":         "primary_status",      # Primary (status: Outstanding/Paid/Denied/Underpaid)

    # ERA fields (populated by 835 handler — UPDATE existing subitems)
    "numeric_mm11v6th":       "era_primary_paid",     # Primary Paid
    "text_mm1ge9yn":          "era_service_date",     # Raw Service Date
    "numeric_mm1gg3pj":       "era_charge_amount",    # Raw Line Item Charge Amount
    "text_mm1gzsan":          "era_patient_control",  # Patient Control #
    "text_mm1g4yd9":          "era_claim_status_code",# Claim Status Code
    "text_mm1gat8c":          "era_line_control",     # Raw Line Item Control Number
    "numeric_mm1gtdts":       "era_allowed_actual",   # Raw Allowed Actual
    "numeric_mm1gredn":       "era_pr_amount",        # Parsed PR Amount
    "numeric_mm1g3nvh":       "era_deductible",       # Parsed Deductible Amount
    "numeric_mm11aqr1":       "era_coinsurance",      # Parsed Coinsurance Amount
    "numeric_mm1gtd3e":       "era_copay",            # Parsed Copay Amount
    "numeric_mm1g48c":        "era_other_pr",         # Parsed Other PR Amount
    "numeric_mm1gken":        "era_co_amount",        # Parsed CO Amount
    "numeric_mm1gt3ky":       "era_co_45",            # Parsed CO-45 Amount
    "numeric_mm1g3vgp":       "era_co_253",           # Parsed CO-253 Amount
    "numeric_mm1grbc3":       "era_other_co",         # Parsed Other CO Amount
    "numeric_mm1gh22d":       "era_oa_amount",        # Parsed OA Amount
    "numeric_mm1gqkvz":       "era_pi_amount",        # Parsed PI Amount
    "text_mm1g6tw3":          "era_remark_codes",     # Parsed Remark Codes
    "long_text_mm1ggyz6":     "era_remark_text",      # Parsed Remark Text
    "text_mm1gt1dh":          "era_adj_codes",        # Parsed Adjustment Codes
    "long_text_mm1g7xmy":     "era_adj_reasons",      # Parsed Adjustment Reasons
}

# HCPC Code status column label indexes
# Used to SET hcpc_code via status index (not text)
HCPC_STATUS_INDEX = {
    "E0784": "0",   # Insulin Pump
    "A4224": "1",   # Infusion Set (payer-dependent)
    "A4225": "2",   # Cartridge (payer-dependent)
    "E2103": "3",   # CGM Monitor
    "A4239": "4",   # CGM Sensors
    "A4232": "6",   # Cartridge (payer-dependent)
    "A4230": "7",   # Infusion Set (payer-dependent)
    "A4231": "8",   # Infusion Set (payer-dependent)
}

# Claims Board subitem Primary status indexes
SUBITEM_PRIMARY_STATUS_INDEX = {
    "Outstanding":  "0",
    "Paid":         "1",
    "Denied":       "2",
    "Underpaid":    "3",
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
