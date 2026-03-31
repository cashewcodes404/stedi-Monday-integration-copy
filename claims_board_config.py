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

COLUMN IDS:
  All column IDs have been verified against the live Monday boards (2026-03-31).
  Real column IDs look like "text_mkwzbcme" or "numeric_mm115q76".
  No cb_/nob_ placeholders remain.
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
# ORDER BOARD (18204609819) — COLUMN MAPPINGS
# ============================================================
# The original Order Board. Product quantities are on parent-level columns.
# No subitems needed for reading — all data is on the parent.

ORDER_BOARD_COLUMN_MAP = {
    # Patient info
    "text_mkp3y5ax":        "dob",
    "phone_mkwrkc73":       "phone",
    "email_mkwrdzzw":       "email",
    "location_mkwr5c7w":    "patient_address",        # Location type

    # Doctor info
    "text_mkxnky7q":        "doctor_name",
    "text_mkxnfg9w":        "doctor_npi",
    "location_mkxnk6jb":    "doctor_address",         # Location type
    "phone_mkxnbspk":       "doctor_phone",

    # Insurance
    "color_mkxnbypc":       "primary_insurance",      # Status: Fidelis, Anthem BCBS, Aetna, etc.
    "color_mkxn5k1a":       "insurance_type",         # Status: Commercial, Medicaid, Medicare
    "text_mktat89m":        "member_id",
    "text_mkwrwrpc":        "medicaid_id",
    "color_mkxsjqjb":       "auth_status",            # Status: Active, No Auth Needed, etc.
    "text_mkwrb2t9":        "authorization",

    # Order metadata
    "color_mkwtxw9r":       "order_status",           # Status: where we add "Process Claim"
    "date_mkwr7spz":        "dos",                    # Order Date (= Date of Service)
    "color_mkxnxfp0":       "subscription_type",      # Status: Insulin Pump & CGM, etc.
    "color_mkxn8fwb":       "frequency",              # Status: 60-Day, 90-Day
    "color_mky4gs76":       "diagnosis_code",         # Status: ICD-10 codes

    # Product quantities (all parent-level)
    "color_mkxnnhfp":       "pump_type",              # Status: t:slim, Mobi, etc.
    "numeric_mkwydqmn":     "pump_qty",
    "color_mkxny0k1":       "infusion_set_1_type",    # Status: AutoSoft 90, TruSteel, etc.
    "numeric_mktaqs2b":     "infusion_1_qty",
    "color_mkxnzhdd":       "infusion_set_2_type",    # Status
    "numeric_mktafnnp":     "infusion_2_qty",
    "numeric_mkwyatkx":     "monitor_qty",
    "color_mkxnhjqj":       "cgm_type",               # Status: Dexcom G7, FreeStyle Libre, etc.

    # Tracking
    "numeric_mkxs3ehj":     "customer_order",
    "text_mm1g99yk":        "stedi_claim_id",
}


# ============================================================
# NEW ORDER BOARD (18405457690) — COLUMN MAPPINGS
# ============================================================
# The New Order Board is FLAT (no subitems).
# Product quantities and types are all parent-level columns.
# Primary Insurance uses COMBINED labels (e.g. "Anthem BCBS Commercial")
# matching the original repo's payer naming convention.
#
# VERIFIED against live Monday board (2026-03-31)

NEW_ORDER_BOARD_COLUMN_MAP = {
    # Patient info
    "color_mm1svmyk":       "gender",               # Status: Male, Female
    "text_mm187t6a":        "dob",
    "phone_mm18rr9v":       "phone",
    "location_mm187v29":    "patient_address",       # Location type
    "pulse_id_mm18spqf":    "customer_id",           # Item ID column

    # Doctor info
    "text_mm18w2y4":        "doctor_name",
    "text_mm18x1kj":        "doctor_npi",
    "location_mm18qfed":    "doctor_address",        # Location type
    "phone_mm18t5ct":       "doctor_phone",

    # Insurance — Primary Insurance is ALREADY combined (e.g. "Anthem BCBS Commercial")
    "color_mm18jhq5":       "primary_insurance",     # Status: Anthem BCBS Commercial, United Commercial, etc.
    "text_mm18s3fe":        "member_id",
    "color_mm18h6yn":       "secondary_insurance",   # Status: NY Medicaid, Patient, Medicare Supplement
    "text_mm18c6z4":        "secondary_id",

    # Order metadata
    "status":               "order_status",          # Status: Order, Ordered, Stuck + Process Claim, Claim Sent to Review
    "date_mm1ssf5g":        "dos",                   # Order Date / Date of Service
    "color_mm18h05q":       "subscription_type",     # Status: CGM, Insulin Pump & CGM, Supplies, etc.
    "color_mm1s96z2":       "order_type",            # Status: First Order, Reorder
    "color_mm1s8tz0":       "frequency",             # Status: 60-Days, 90-Days
    "color_mm189t0b":       "diagnosis_code",        # Status: E11.65, E10.65
    "color_mm18ds28":       "cgm_coverage",          # Status: Hypo, Insulin
    "text_mm1snsw3":        "auth_id",
    "color_mm1seak5":       "referral",              # Status: Working on it, Done, Stuck

    # Product — Insulin Pump
    "color_mm1stny0":       "pump_brand",            # Status: Tandem, Beta Bionics
    "color_mm1s45wm":       "pump_type",             # Status: Mobi, iLet, t:slim
    "numeric_mm1smjyx":     "pump_qty",

    # Product — Infusion Sets
    "color_mm1saxyg":       "infusion_set_1_type",   # Status: AutoSoft XC 9mm 23"
    "numeric_mm1shc1v":     "infusion_1_qty",
    "color_mm1sp64":        "infusion_set_2_type",   # Status
    "numeric_mm1svn8d":     "infusion_2_qty",

    # Product — Cartridge
    "color_mm1szdck":       "cartridge_type",        # Status: Mobi, iLet, t:slim
    "numeric_mm1s9qxd":     "cartridge_qty",

    # Product — CGM
    "color_mm1sjy4y":       "cgm_type",              # Status: FreeStyle Libre 3 Plus, Dexcom G7
    "numeric_mm1s49bj":     "cgm_sensor_qty",
    "numeric_mm1s431c":     "cgm_monitor_qty",
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

    # Order metadata
    "color_mky4mb3y":   "frequency",              # Frequency (status: 60-Day/90-Day/30-Day)

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
    "dropdown_mm1z7je9":      "modifiers",           # Modifiers (dropdown)
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

# Subitem Order Frequency status indexes
SUBITEM_ORDER_FREQUENCY_INDEX = {
    "60-Days": "0",
    "90-Days": "1",
}

# ── Subitem Primary Insurance label mapping ──
# The subitem "Primary Insurance" (color_mm1cjcmg) uses combined payer+type labels
# that match the Est. Pay / Claim Quantity formulas.
# Known indexes (already on the board):
SUBITEM_PRIMARY_INSURANCE_INDEX = {
    "Medicare A & B":             "0",
    "Cigna Commercial":           "1",
    "Fidelis CHP":                "2",
    "Anthem BCBS Commercial":     "3",
    "Humana":                     "4",
    "Aetna Commercial":           "6",
}

# Full mapping: (parent_payor, insurance_type) → subitem label text.
# If a combo isn't in SUBITEM_PRIMARY_INSURANCE_INDEX, we'll use
# {"label": "<text>"} format which auto-creates the label on Monday.
# The label text MUST match what the Est. Pay formula references in its SWITCH.
PARENT_PAYOR_TO_SUBITEM_INSURANCE = {
    # Anthem BCBS
    ("Anthem BCBS", "Commercial"):  "Anthem BCBS Commercial",
    ("Anthem BCBS", "Medicare"):    "Anthem BCBS Medicare",
    ("Anthem BCBS", "Medicaid"):    "Anthem BCBS Medicaid",
    # Fidelis
    ("Fidelis", "Commercial"):      "Fidelis Commercial",
    ("Fidelis", "Medicaid"):        "Fidelis Medicaid",
    ("Fidelis", "CHP"):             "Fidelis CHP",
    # Cigna
    ("Cigna", "Commercial"):        "Cigna Commercial",
    ("Cigna", "Medicare"):          "Cigna Medicare",
    # Aetna
    ("Aetna", "Commercial"):        "Aetna Commercial",
    ("Aetna", "Medicare"):          "Aetna Medicare",
    # United Healthcare
    ("United Healthcare", "Commercial"): "United Commercial",
    ("United Healthcare", "Medicare"):   "United Medicare",
    ("United Healthcare", "Medicaid"):   "United Medicaid",
    # Medicare A & B (standalone — no insurance type needed)
    ("Medicare A & B", ""):          "Medicare A & B",
    ("Medicare A & B", "Medicare"):  "Medicare A & B",
    # Others (insurance_type usually empty for these)
    ("Wellcare", ""):               "Wellcare",
    ("Humana", ""):                 "Humana",
    ("Humana", "Medicare"):         "Humana",
    ("health first", ""):           "health first",
    ("NYSHIP Empire", ""):          "NYSHIP Empire",
    ("NYSHIP Empire", "Commercial"):"NYSHIP Empire",
    ("Medicaid", ""):               "Medicaid",
    ("Medicaid", "Medicaid"):       "Medicaid",
    ("BCBS Wyoming", ""):           "BCBS Wyoming",
    ("MagnaCare", ""):              "MagnaCare",
    ("Midlands Choice", ""):        "Midlands Choice",
    ("UMR", ""):                    "UMR",
    ("1199", ""):                   "1199",
    ("BCBS NJ (Horizon)", ""):      "Horizon BCBS",
    ("Horizon BCBS", ""):           "Horizon BCBS",
    ("MetroPlus", ""):              "MetroPlus",
}


def resolve_subitem_insurance_label(parent_payor: str, insurance_type: str) -> str:
    """
    Given the parent Claims Board's Primary Payor + Insurance Type,
    return the subitem Primary Insurance label text that matches the
    Est. Pay formula's SWITCH statement.

    Falls back to combining payer + type if no explicit mapping exists.
    """
    # Try exact match
    label = PARENT_PAYOR_TO_SUBITEM_INSURANCE.get((parent_payor, insurance_type))
    if label:
        return label

    # Try with empty insurance type (for standalone payers like Medicare A & B)
    label = PARENT_PAYOR_TO_SUBITEM_INSURANCE.get((parent_payor, ""))
    if label:
        return label

    # Fallback: combine payer + type (e.g., "MetroPlus Commercial")
    if insurance_type:
        return f"{parent_payor} {insurance_type}"
    return parent_payor


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
