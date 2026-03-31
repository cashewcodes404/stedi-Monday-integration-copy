import os
import logging
import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

MONDAY_API_URL = "https://api.monday.com/v2"


def is_mock_mode() -> bool:
    """Check if running without real API keys (mock mode)."""
    return not os.getenv("MONDAY_API_TOKEN")


def get_headers() -> dict:
    token = os.getenv("MONDAY_API_TOKEN")
    if not token:
        raise ValueError("MONDAY_API_TOKEN not set in .env")
    return {
        "Authorization": token,
        "Content-Type": "application/json",
        "API-Version": "2024-01",
    }


def run_query(query: str, variables: dict = None) -> dict:
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    response = requests.post(
        MONDAY_API_URL,
        json=payload,
        headers=get_headers(),
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()

    if "errors" in result:
        raise ValueError(f"Monday API error: {result['errors']}")

    return result


def search_board_items(board_id: str, column_id: str, match_value: str) -> str:
    """
    Search all items on a board for a column value match.
    Uses cursor-based pagination to handle boards with >200 items.
    Returns the item ID if found, or empty string.
    """
    if not board_id or not match_value:
        return ""

    query = """
    query FindItem($boardId: ID!, $cursor: String) {
      boards(ids: [$boardId]) {
        items_page(limit: 200, cursor: $cursor) {
          cursor
          items {
            id
            name
            column_values { id text }
          }
        }
      }
    }
    """
    cursor = None

    while True:
        variables = {"boardId": board_id}
        if cursor:
            variables["cursor"] = cursor

        try:
            result = run_query(query, variables)
            page = (
                result.get("data", {})
                .get("boards", [{}])[0]
                .get("items_page", {})
            )
            items = page.get("items", [])

            for item in items:
                for col in item.get("column_values", []):
                    if col.get("id") == column_id:
                        stored = col.get("text") or ""
                        # Support comma-separated values (multi-claim orders)
                        stored_values = [v.strip() for v in stored.split(",")]
                        if match_value in stored_values:
                            return item["id"]
                        # Also check exact match for non-comma cases
                        if stored == match_value:
                            return item["id"]

            cursor = page.get("cursor")
            if not cursor or not items:
                break

        except Exception as e:
            logger.error(f"Board search failed: {e}")
            break

    return ""

def get_order_item(item_id: str) -> dict:
    """Fetch order item with all column values. Returns mock data in mock mode."""
    if is_mock_mode():
        logger.info(f"MOCK MODE: Returning sample order for item_id={item_id}")
        return _get_mock_order_item(item_id)

    query = """
    query GetOrderItem($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        name
        column_values {
          id
          text
          value
        }
        subitems {
          id
          name
          column_values {
            id
            text
            value
          }
        }
      }
    }
    """
    result = run_query(query, {"itemId": item_id})
    items = result.get("data", {}).get("items", [])
    if not items:
        raise ValueError(f"No item found for item_id={item_id}")
    logger.info(f"Fetched item: {items[0].get('name')}")
    return items[0]


def _get_mock_order_item(item_id: str) -> dict:
    """Return a realistic mock order item for testing without Monday API."""
    return {
        "id": item_id,
        "name": "John TestPatient",
        "column_values": [
            {"id": "status",            "text": "Submit Claim", "value": None},
            {"id": "text_mm18zjmz",     "text": "Male",        "value": None},
            {"id": "text_mm187t6a",     "text": "01/15/1980",  "value": None},
            {"id": "phone_mm18rr9v",    "text": "555-123-4567", "value": None},
            {"id": "location_mm187v29", "text": "123 Test St, Brooklyn, NY 11221", "value": None},
            {"id": "color_mm189t0b",    "text": "E10.65",      "value": None},
            {"id": "color_mm18ds28",    "text": "Insulin",     "value": None},
            {"id": "text_mm18w2y4",     "text": "Jane Doctor", "value": None},
            {"id": "text_mm18x1kj",     "text": "1234567890",  "value": None},
            {"id": "location_mm18qfed", "text": "456 Medical Ave, New York, NY 10001", "value": None},
            {"id": "phone_mm18t5ct",    "text": "555-987-6543", "value": None},
            {"id": "color_mm18jhq5",    "text": "Anthem BCBS Commercial", "value": None},
            {"id": "text_mm18s3fe",     "text": "TEST123456",  "value": None},
            {"id": "color_mm18h6yn",    "text": "Anthem BCBS Commercial", "value": None},
            {"id": "text_mm18c6z4",     "text": "",            "value": None},
            {"id": "color_mm18h05q",    "text": "Individual",  "value": None},
        ],
        "subitems": [
            {
                "id": "mock_sub_1",
                "name": "CGM Sensors",
                "column_values": [
                    {"id": "status",            "text": "Ready",      "value": None},
                    {"id": "date0",             "text": "2026-03-15", "value": None},
                    {"id": "color_mm18p9f4",    "text": "Anthem BCBS Commercial", "value": None},
                    {"id": "text_mm18k1x8",     "text": "",           "value": None},
                    {"id": "text_mm18zcs4",     "text": "TEST123456", "value": None},
                    {"id": "numeric_mm18t2q9",  "text": "6",          "value": None},
                    {"id": "color_mm185yjy",    "text": "Dexcom G7",  "value": None},
                    {"id": "color_mm18e5yq",    "text": "",           "value": None},
                    {"id": "color_mm18pj26",    "text": "",           "value": None},
                    {"id": "text_mm18dsxx",     "text": "",           "value": None},
                ],
            },
        ],
    }

STATUS_TO_INDEX = {
    "Accepted":       "1",
    "Rejected":       "0",   # Payer Rejected
    "Stedi Rejected": "2",
}

def _mock_mutation(description: str, **kwargs) -> None:
    """Log a mutation that would have been sent in live mode."""
    logger.info(f"MOCK MODE: Would {description} — {kwargs}")


def update_277_status(item_id: str, status: str, rejection_reason: str = "") -> None:
    if is_mock_mode():
        _mock_mutation("update 277 status", item_id=item_id, status=status)
        return

    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    # Update 277 status with correct index
    label_index = STATUS_TO_INDEX.get(status, "1")
    status_value = '{"index": ' + label_index + '}'

    try:
        run_query(mutation, {
            "itemId": str(item_id),
            "boardId": str(board_id),
            "columnId": "color_mm1bx9az",
            "value": status_value,
        })
        logger.info(f"Updated 277 status: {status}")
    except Exception as e:
        logger.warning(f"Failed to update 277 status: {e}")

    # Only store rejection reason when actually rejected
    if status != "Accepted" and rejection_reason:
        try:
            run_query(mutation, {
                "itemId": str(item_id),
                "boardId": str(board_id),
                "columnId": "text_mm1b56xa",
                "value": f'"{rejection_reason}"',
            })
        except Exception as e:
            logger.warning(f"Failed to store rejection reason: {e}")

CLAIM_STATUS_TO_INDEX = {
    "Submit Claim": "0",
    "Submitted":    "1",
    "Rejected":     "2",
    "Test Claim Submitted":  "3",
}

def update_claim_status(item_id: str, status: str) -> None:
    if is_mock_mode():
        _mock_mutation("update claim status", item_id=item_id, status=status)
        return

    board_id     = os.getenv("MONDAY_ORDER_BOARD_ID")
    label_index  = CLAIM_STATUS_TO_INDEX.get(status, "1")
    status_value = '{"index": ' + label_index + '}'

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    try:
        run_query(mutation, {
            "itemId":   str(item_id),
            "boardId":  str(board_id),
            "columnId": "status",
            "value":    status_value,
        })
        logger.info(f"Claim Status column → {status}")
    except Exception as e:
        logger.warning(f"Failed to update Claim Status column: {e}")
        raise

def create_claims_board_item(order_item: dict, claim_id: str, payer_name: str = "") -> str:
    """
    Create new item in Claims Board after claim is submitted.
    Populates as many fields as possible from the order data.
    """
    if is_mock_mode():
        import uuid
        mock_id = f"mock_{uuid.uuid4().hex[:8]}"
        patient_name = order_item.get("name", "Unknown")
        _mock_mutation("create Claims Board item", patient=patient_name, claim_id=claim_id)
        return mock_id

    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
    if not claims_board_id:
        logger.warning("MONDAY_CLAIMS_BOARD_ID not set — skipping")
        return ""

    patient_name = order_item.get("name", "Unknown")
    item_name = f"{patient_name} - {payer_name}" if payer_name else patient_name

    # Step 1: Create the item
    mutation = """
    mutation CreateItem($boardId: ID!, $itemName: String!) {
      create_item(board_id: $boardId, item_name: $itemName) { id }
    }
    """
    result = run_query(mutation, {
        "boardId": claims_board_id,
        "itemName": item_name,
    })
    new_item_id = result.get("data", {}).get("create_item", {}).get("id", "")
    if not new_item_id:
        logger.warning("Failed to create Claims Board item")
        return ""

    logger.info(f"Created Claims Board item {new_item_id}: {item_name}")

    # Step 2: Populate columns from order data
    col_values = {col.get("id"): col.get("text", "") for col in order_item.get("column_values", [])}

    # Map of column_id → value to set
    # Based on Claims Board columns logged earlier
    fields_to_set = {
        "text_mktat89m":   col_values.get("text_mm18s3fe", ""),    # Member ID
        "text_mkp3y5ax":   col_values.get("text_mm187t6a", ""),    # DOB
        "text_mkxr2r9b":   col_values.get("text_mm18x1kj", ""),    # NPI
        "text_mkxrh4a4":   col_values.get("text_mm18w2y4", ""),    # Doctor
        "text_mkwzbcme":   claim_id,                                # Stedi Correlation ID / Customer Order ref
    }

    # DOS from subitem order_date
    subitems = order_item.get("subitems", [])
    if subitems:
        sub_cols = {c.get("id"): c.get("text", "") for c in subitems[0].get("column_values", [])}
        dos_raw = sub_cols.get("date0", "")
        if dos_raw:
            fields_to_set["date_mkwr7spz"] = dos_raw  # DOS

    # Claim Sent Date = today
    from datetime import date
    today = date.today().isoformat()
    fields_to_set["date_mm14rk8d"] = today  # Claim Sent Date

    update_mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    for col_id, value in fields_to_set.items():
        if not value:
            continue
        try:
            # Date columns need JSON format
            if col_id.startswith("date_"):
                formatted = '{"date": "' + str(value) + '"}'
            else:
                formatted = f'"{value}"'

            run_query(update_mutation, {
                "itemId":   str(new_item_id),
                "boardId":  str(claims_board_id),
                "columnId": col_id,
                "value":    formatted,
            })
            logger.info(f"Claims Board: set {col_id} = {value}")
        except Exception as e:
            logger.warning(f"Claims Board: failed to set {col_id}: {e}")

    return new_item_id

def populate_era_data_on_claims_item(claims_item_id: str, era_data: dict) -> None:
    """
    Populate ERA payment data onto a Claims Board item.
    Parent row fields + service line subitems.
    """
    if is_mock_mode():
        _mock_mutation("populate ERA data", claims_item_id=claims_item_id,
                       paid=era_data.get("primary_paid"), pr=era_data.get("pr_amount"))
        return

    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    # Phase 1 parent fields — from claimsvisualizer.py
    field_to_column = {
        "primary_paid":            ("numeric_mm115q76", "number"),  # Primary Paid (A)
        "pr_amount":               ("numeric_mkxmc2rh", "number"),  # PR Amount (C)
        "paid_date":               ("date_mm11zg2f",    "date"),    # Primary Paid Date (D)
        "check_number":            ("text_mm11m3fh",    "text"),    # Check #
        "primary_status":          ("text_mkzck8tw",    "text"),    # Primary -->
        "raw_patient_control_num": ("text_mm0fa4vk",    "text"),    # Raw PCN
    }

    for field, (column_id, col_type) in field_to_column.items():
        value = era_data.get(field, "")
        if value is None or value == "":
            continue
        try:
            if col_type == "number":
                formatted = str(value)
            elif col_type == "date":
                formatted = '{"date": "' + str(value) + '"}'
            else:
                formatted = f'"{value}"'

            run_query(mutation, {
                "itemId":   str(claims_item_id),
                "boardId":  str(claims_board_id),
                "columnId": column_id,
                "value":    formatted,
            })
            logger.info(f"ERA parent: set {field} → {column_id} = {value}")
        except Exception as e:
            logger.warning(f"ERA parent: failed {field}: {e}")

    # Populate service line subitems
    children = era_data.get("children", [])
    if children:
        populate_era_service_line_subitems(claims_item_id, children)


# Subitem column ID mapping
# Based on Claims Board subitem columns fetched
SUBITEM_ERA_COLUMN_MAP = {
    # ERA field name         → (column_id,              type)
    "Primary Paid":          ("numeric_mm1czbyg",       "number"),  # Primary Paid
    "Raw Service Date":      ("date_mm11hscn",          "date"),    # Service Date
    "Raw Line Item Charge":  ("numeric_mm11v6th",       "number"),  # Charge Amount
    "Patient Control #":     ("text_mm16qhea",          "text"),    # Patient Control #
    "Claim Status Code":     ("text_mm1gzsan",          "text"),    # Claim Status Code
    "Raw Line Control #":    ("text_mm1g4yd9",          "text"),    # Line Item Control #
    "Raw Allowed Actual":    ("numeric_mm1gg3pj",       "number"),  # Allowed Actual
    "Parsed PR Amount":      ("numeric_mm1gtdts",       "number"),  # Parsed PR Amount
    "Parsed Deductible":     ("numeric_mm1gredn",       "number"),  # Parsed Deductible
    "Parsed Coinsurance":    ("numeric_mm1g3nvh",       "number"),  # Parsed Coinsurance
    "Parsed Copay":          ("numeric_mm11aqr1",       "number"),  # Parsed Copay
    "Parsed Other PR":       ("numeric_mm1gtd3e",       "number"),  # Parsed Other PR
    "Parsed CO Amount":      ("numeric_mm1g48c",        "number"),  # Parsed CO Amount
    "Parsed CO-45":          ("numeric_mm1gken",        "number"),  # Parsed CO-45
    "Parsed CO-253":         ("numeric_mm1gt3ky",       "number"),  # Parsed CO-253
    "Parsed Other CO":       ("numeric_mm1g3vgp",       "number"),  # Parsed Other CO
    "Parsed OA Amount":      ("numeric_mm1grbc3",       "number"),  # Parsed OA
    "Parsed PI Amount":      ("numeric_mm1gh22d",       "number"),  # Parsed PI
    "Parsed Remark Codes":   ("text_mm1g6tw3",          "text"),    # Remark Codes
    "Parsed Remark Text":    ("long_text_mm1ggyz6",     "long_text"), # Remark Text
    "Parsed Adj Codes":      ("text_mm1gt1dh",          "text"),    # Adjustment Codes
    "Parsed Adj Reasons":    ("long_text_mm1g7xmy",     "long_text"), # Adjustment Reasons
}

def _get_column_value(item_id: str, column_id: str) -> str:
    """Read a single column value from an Order Board item"""
    query = """
    query GetItem($itemId: ID!) {
      items(ids: [$itemId]) {
        column_values { id text }
      }
    }
    """
    try:
        result = run_query(query, {"itemId": item_id})
        cols = result.get("data", {}).get("items", [{}])[0].get("column_values", [])
        for col in cols:
            if col.get("id") == column_id:
                return col.get("text", "") or ""
    except Exception:
        pass
    return ""

import threading

_pcn_lock = threading.Lock()


def store_claim_pcn(item_id: str, pcn: str, claim_id: str) -> None:
    """
    Store patientControlNumber on Order Board item.
    Uses a lock to prevent race conditions when multiple claims
    for the same order submit concurrently.
    """
    if is_mock_mode():
        _mock_mutation("store PCN", item_id=item_id, pcn=pcn)
        return

    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    with _pcn_lock:
        existing = _get_column_value(item_id, "text_mm1ra2v1")
        if existing and pcn not in existing:
            new_value = f"{existing},{pcn}"
        else:
            new_value = pcn

        try:
            run_query(mutation, {
                "itemId":   str(item_id),
                "boardId":  str(board_id),
                "columnId": "text_mm1ra2v1",
                "value":    f'"{new_value}"',
            })
            logger.info(f"Stored pcn={pcn} on order item {item_id} (full: {new_value})")
        except Exception as e:
            logger.warning(f"Failed to store pcn: {e}")

def post_claim_update_to_monday(
    item_id: str,
    submitted_claims: list,
    is_test: bool = False,
) -> None:
    if is_mock_mode():
        _mock_mutation("post claim update", item_id=item_id, claims=len(submitted_claims))
        return

    import json
    from datetime import datetime
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    mode_tag = "🧪 TEST CLAIM" if is_test else "✅ LIVE CLAIM"

    lines = [f"{mode_tag} submitted to Stedi — {now}\n"]

    for i, c in enumerate(submitted_claims, 1):
        payload_json = json.dumps(c["payload"], indent=2)
        lines.append(
            f"-- Claim #{i} --\n"
            f"Payer: {c['payer']}\n"
            f"Claim ID: {c['claim_id']}\n"
            f"Patient Control #: {c['pcn']}\n"
            f"Payload:\n{payload_json}\n"
        )

    message = "\n".join(lines)

    mutation = """
    mutation PostUpdate($itemId: ID!, $body: String!) {
      create_update(item_id: $itemId, body: $body) { id }
    }
    """
    try:
        run_query(mutation, {"itemId": str(item_id), "body": message})
        logger.info(f"Posted combined claim update to Monday item {item_id}")
    except Exception as e:
        logger.warning(f"Failed to post Monday update: {e}")


def populate_era_service_line_subitems(claims_item_id: str, children: list) -> None:
    """
    Create subitems on Claims Board item for each ERA service line.
    One subitem per HCPCS code with all parsed ERA fields.
    """
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    create_mutation = """
    mutation CreateSubitem($parentId: ID!, $itemName: String!) {
      create_subitem(parent_item_id: $parentId, item_name: $itemName) {
        id
        board { id }
      }
    }
    """

    update_mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    for child in children:
        hcpc_code = child.get("HCPC Code", "Unknown")

        try:
            # Create subitem named after HCPCS code
            result = run_query(create_mutation, {
                "parentId": str(claims_item_id),
                "itemName": hcpc_code,
            })

            subitem_id = (
                result.get("data", {})
                .get("create_subitem", {})
                .get("id", "")
            )
            subitem_board_id = (
                result.get("data", {})
                .get("create_subitem", {})
                .get("board", {})
                .get("id", "")
            )

            if not subitem_id or not subitem_board_id:
                logger.warning(f"Failed to create subitem for {hcpc_code}")
                continue

            logger.info(f"Created subitem {subitem_id} for {hcpc_code}")

            # Map child ERA fields to subitem columns
            fields = {
                "Primary Paid":         child.get("Primary Paid"),
                "Raw Service Date":     child.get("Raw Service Date"),
                "Raw Line Item Charge": child.get("Raw Line Item Charge Amount"),
                "Patient Control #":    child.get("Patient Control #"),
                "Claim Status Code":    child.get("Claim Status Code"),
                "Raw Line Control #":   child.get("Raw Line Item Control Number"),
                "Raw Allowed Actual":   child.get("Raw Allowed Actual"),
                "Parsed PR Amount":     child.get("Parsed PR Amount"),
                "Parsed Deductible":    child.get("Parsed Deductible Amount"),
                "Parsed Coinsurance":   child.get("Parsed Coinsurance Amount"),
                "Parsed Copay":         child.get("Parsed Copay Amount"),
                "Parsed Other PR":      child.get("Parsed Other PR Amount"),
                "Parsed CO Amount":     child.get("Parsed CO Amount"),
                "Parsed CO-45":         child.get("Parsed CO-45 Amount"),
                "Parsed CO-253":        child.get("Parsed CO-253 Amount"),
                "Parsed Other CO":      child.get("Parsed Other CO Amount"),
                "Parsed OA Amount":     child.get("Parsed OA Amount"),
                "Parsed PI Amount":     child.get("Parsed PI Amount"),
                "Parsed Remark Codes":  child.get("Parsed Remark Codes"),
                "Parsed Remark Text":   child.get("Parsed Remark Text"),
                "Parsed Adj Codes":     child.get("Parsed Adjustment Codes"),
                "Parsed Adj Reasons":   child.get("Parsed Adjustment Reasons"),
            }

            for field_name, value in fields.items():
                if value is None or value == "" or value == 0.0:
                    continue

                col_id, col_type = SUBITEM_ERA_COLUMN_MAP.get(field_name, (None, None))
                if not col_id:
                    continue

                try:
                    if col_type == "number":
                        formatted = str(value)
                    elif col_type == "date":
                        formatted = '{"date": "' + str(value) + '"}'
                    elif col_type == "long_text":
                        formatted = '{"text": "' + str(value).replace('"', "'") + '"}'
                    else:
                        formatted = f'"{str(value)}"'

                    run_query(update_mutation, {
                        "itemId":   str(subitem_id),
                        "boardId":  str(subitem_board_id),
                        "columnId": col_id,
                        "value":    formatted,
                    })
                    logger.info(f"  Subitem {hcpc_code}: set {field_name} = {value}")

                except Exception as e:
                    logger.warning(f"  Subitem {hcpc_code}: failed {field_name}: {e}")

        except Exception as e:
            logger.warning(f"Failed to create subitem for {hcpc_code}: {e}")

# ============================================================
# NEW ORDER BOARD FUNCTIONS
# ============================================================

def get_new_order_item(item_id: str) -> dict:
    """
    Fetch item from the New Order Board (flat, no subitems).
    Returns mock data in mock mode.
    """
    if is_mock_mode():
        logger.info(f"MOCK MODE: Returning sample new order for item_id={item_id}")
        return _get_mock_new_order_item(item_id)

    query = """
    query GetNewOrderItem($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        name
        column_values {
          id
          text
          value
        }
      }
    }
    """
    result = run_query(query, {"itemId": item_id})
    items = result.get("data", {}).get("items", [])
    if not items:
        raise ValueError(f"No item found for item_id={item_id} on New Order Board")
    logger.info(f"Fetched New Order Board item: {items[0].get('name')}")
    return items[0]


def _get_mock_new_order_item(item_id: str) -> dict:
    """Return a realistic mock New Order Board item (flat, no subitems)."""
    return {
        "id": item_id,
        "name": "John TestPatient",
        "column_values": [
            {"id": "nob_gender",            "text": "Male",        "value": None},
            {"id": "nob_dob",               "text": "01/15/1980",  "value": None},
            {"id": "nob_phone",             "text": "555-123-4567", "value": None},
            {"id": "nob_patient_address",   "text": "123 Test St, Brooklyn, NY 11221", "value": None},
            {"id": "nob_diagnosis_code",    "text": "E10.65",      "value": None},
            {"id": "nob_cgm_coverage",      "text": "Insulin",     "value": None},
            {"id": "nob_doctor_name",       "text": "Jane Doctor", "value": None},
            {"id": "nob_doctor_npi",        "text": "1234567890",  "value": None},
            {"id": "nob_doctor_address",    "text": "456 Medical Ave, New York, NY 10001", "value": None},
            {"id": "nob_doctor_phone",      "text": "555-987-6543", "value": None},
            {"id": "nob_primary_insurance", "text": "Anthem BCBS Commercial", "value": None},
            {"id": "nob_member_id",         "text": "TEST123456",  "value": None},
            {"id": "nob_secondary_id",      "text": "",            "value": None},
            {"id": "nob_subscription_type", "text": "Individual",  "value": None},
            {"id": "nob_pump_qty",          "text": "1",           "value": None},
            {"id": "nob_infusion_set_qty",  "text": "10",          "value": None},
            {"id": "nob_cartridge_qty",     "text": "10",          "value": None},
            {"id": "nob_cgm_sensor_qty",    "text": "6",           "value": None},
            {"id": "nob_cgm_monitor_qty",   "text": "1",           "value": None},
            {"id": "nob_pump_type",         "text": "t:slim X2",   "value": None},
            {"id": "nob_cgm_type",          "text": "Dexcom G7",   "value": None},
            {"id": "nob_order_date",        "text": "2026-03-15",  "value": None},
            {"id": "nob_order_status",      "text": "Ready",       "value": None},
            {"id": "nob_auth_id",           "text": "",            "value": None},
        ],
    }


# ============================================================
# CLAIMS BOARD — PARENT CREATION
# ============================================================

def get_claims_board_item(item_id: str) -> dict:
    """
    Fetch Claims Board item with all column values and subitems.
    Used when submitting a claim from the Claims Board.
    """
    if is_mock_mode():
        logger.info(f"MOCK MODE: Returning sample Claims Board item for item_id={item_id}")
        return _get_mock_claims_board_item(item_id)

    query = """
    query GetClaimsBoardItem($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        name
        column_values {
          id
          text
          value
        }
        subitems {
          id
          name
          column_values {
            id
            text
            value
          }
        }
      }
    }
    """
    result = run_query(query, {"itemId": item_id})
    items = result.get("data", {}).get("items", [])
    if not items:
        raise ValueError(f"No Claims Board item found for item_id={item_id}")
    logger.info(f"Fetched Claims Board item: {items[0].get('name')}")
    return items[0]


def _get_mock_claims_board_item(item_id: str) -> dict:
    """Return a realistic mock Claims Board item with 5 product subitems."""
    return {
        "id": item_id,
        "name": "John TestPatient - Anthem BCBS Commercial",
        "column_values": [
            {"id": "text_mktat89m",     "text": "TEST123456",     "value": None},
            {"id": "text_mkp3y5ax",     "text": "01/15/1980",     "value": None},
            {"id": "text_mkxr2r9b",     "text": "1234567890",     "value": None},
            {"id": "text_mkxrh4a4",     "text": "Jane Doctor",    "value": None},
            {"id": "text_mkwzbcme",     "text": "",               "value": None},
            {"id": "date_mkwr7spz",     "text": "2026-03-15",     "value": None},
            {"id": "date_mm14rk8d",     "text": "",               "value": None},
        ],
        "subitems": [
            {
                "id": "mock_cb_sub_1",
                "name": "Insulin Pump",
                "column_values": [
                    {"id": "cb_sub_hcpc_code",     "text": "E0784",  "value": None},
                    {"id": "cb_sub_claim_qty",     "text": "1",      "value": None},
                    {"id": "cb_sub_units",         "text": "1",      "value": None},
                    {"id": "cb_sub_modifiers",     "text": "",       "value": None},
                    {"id": "cb_sub_charge_amount", "text": "2500.00","value": None},
                    {"id": "cb_sub_est_pay",       "text": "2500.00","value": None},
                ],
            },
            {
                "id": "mock_cb_sub_2",
                "name": "CGM Sensors",
                "column_values": [
                    {"id": "cb_sub_hcpc_code",     "text": "A4239",  "value": None},
                    {"id": "cb_sub_claim_qty",     "text": "6",      "value": None},
                    {"id": "cb_sub_units",         "text": "3",      "value": None},
                    {"id": "cb_sub_modifiers",     "text": "KS",     "value": None},
                    {"id": "cb_sub_charge_amount", "text": "450.00", "value": None},
                    {"id": "cb_sub_est_pay",       "text": "450.00", "value": None},
                ],
            },
        ],
    }


def create_claims_board_parent(
    patient_name: str,
    payer_name: str,
    normalized_order: dict,
) -> str:
    """
    Create a Claims Board parent item with patient/order data.
    This is the NEW flow — creates the parent BEFORE submission.
    """
    if is_mock_mode():
        import uuid
        mock_id = f"mock_cb_{uuid.uuid4().hex[:8]}"
        _mock_mutation("create Claims Board parent", patient=patient_name, payer=payer_name)
        return mock_id

    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
    if not claims_board_id:
        logger.warning("MONDAY_CLAIMS_BOARD_ID not set — skipping")
        return ""

    item_name = f"{patient_name} - {payer_name}" if payer_name else patient_name

    # Step 1: Create the item
    mutation = """
    mutation CreateItem($boardId: ID!, $itemName: String!) {
      create_item(board_id: $boardId, item_name: $itemName) { id }
    }
    """
    result = run_query(mutation, {
        "boardId": claims_board_id,
        "itemName": item_name,
    })
    new_item_id = result.get("data", {}).get("create_item", {}).get("id", "")
    if not new_item_id:
        logger.warning("Failed to create Claims Board parent")
        return ""

    logger.info(f"Created Claims Board parent {new_item_id}: {item_name}")

    # Step 2: Populate fields from normalized order
    from claim_infrastructure import normalize_date
    from datetime import date

    fields_to_set = {
        "text_mktat89m": normalized_order.get("member_id", ""),
        "text_mkp3y5ax": normalized_order.get("patient_dob", ""),
        "text_mkxr2r9b": normalized_order.get("doctor_npi", ""),
        "text_mkxrh4a4": normalized_order.get("doctor_name", ""),
    }

    # DOS from order date
    order_date = normalized_order.get("order_date", "")
    if order_date:
        # Normalize to YYYY-MM-DD for Monday date column
        if len(order_date) == 8:  # YYYYMMDD
            order_date = f"{order_date[:4]}-{order_date[4:6]}-{order_date[6:8]}"
        fields_to_set["date_mkwr7spz"] = order_date

    update_mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    for col_id, value in fields_to_set.items():
        if not value:
            continue
        try:
            if col_id.startswith("date_"):
                formatted = '{"date": "' + str(value) + '"}'
            else:
                formatted = f'"{value}"'

            run_query(update_mutation, {
                "itemId":   str(new_item_id),
                "boardId":  str(claims_board_id),
                "columnId": col_id,
                "value":    formatted,
            })
            logger.info(f"Claims Board parent: set {col_id} = {value}")
        except Exception as e:
            logger.warning(f"Claims Board parent: failed to set {col_id}: {e}")

    return new_item_id


def populate_claims_board_subitems(claims_item_id: str, product_subitems: list) -> None:
    """
    Populate the 5 pre-created product subitems on a Claims Board item.
    Creates subitems named after each product and fills in pre-computed values.
    """
    if is_mock_mode():
        _mock_mutation("populate Claims Board subitems",
                       claims_item_id=claims_item_id,
                       products=len(product_subitems))
        return

    from claims_board_config import CLAIMS_BOARD_SUBITEM_WRITE_MAP

    create_mutation = """
    mutation CreateSubitem($parentId: ID!, $itemName: String!) {
      create_subitem(parent_item_id: $parentId, item_name: $itemName) {
        id
        board { id }
      }
    }
    """

    update_mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    for product in product_subitems:
        product_name = product.get("product_name", "Unknown")

        try:
            # Create subitem
            result = run_query(create_mutation, {
                "parentId": str(claims_item_id),
                "itemName": product_name,
            })

            subitem_id = (
                result.get("data", {})
                .get("create_subitem", {})
                .get("id", "")
            )
            subitem_board_id = (
                result.get("data", {})
                .get("create_subitem", {})
                .get("board", {})
                .get("id", "")
            )

            if not subitem_id or not subitem_board_id:
                logger.warning(f"Failed to create subitem for {product_name}")
                continue

            logger.info(f"Created Claims Board subitem {subitem_id}: {product_name}")

            # Write pre-computed values
            field_map = {
                "hcpc_code":     product.get("hcpc_code", ""),
                "claim_qty":     product.get("claim_qty", ""),
                "units":         product.get("units", ""),
                "modifiers":     ",".join(product.get("modifiers", [])),
                "charge_amount": product.get("charge_amount", ""),
                "est_pay":       product.get("est_pay", ""),
            }

            for field_name, value in field_map.items():
                if not value:
                    continue

                col_id = CLAIMS_BOARD_SUBITEM_WRITE_MAP.get(field_name)
                if not col_id:
                    logger.warning(f"No column ID mapped for subitem field: {field_name}")
                    continue

                try:
                    if col_id.startswith("numeric_"):
                        formatted = str(value)
                    else:
                        formatted = f'"{value}"'

                    run_query(update_mutation, {
                        "itemId":   str(subitem_id),
                        "boardId":  str(subitem_board_id),
                        "columnId": col_id,
                        "value":    formatted,
                    })
                    logger.info(f"  Subitem {product_name}: set {field_name} = {value}")
                except Exception as e:
                    logger.warning(f"  Subitem {product_name}: failed {field_name}: {e}")

        except Exception as e:
            logger.warning(f"Failed to create subitem for {product_name}: {e}")


# ============================================================
# CLAIMS BOARD — 277 STATUS UPDATE
# ============================================================

def update_claims_board_277(claims_item_id: str, status: str, rejection_reason: str = "") -> None:
    """
    Update 277 acknowledgement status on a Claims Board parent item.
    In the new flow, 277 writes to Claims Board instead of Order Board.
    """
    if is_mock_mode():
        _mock_mutation("update Claims Board 277", claims_item_id=claims_item_id, status=status)
        return

    from claims_board_config import (
        CLAIMS_BOARD_277_STATUS_TO_INDEX,
        CLAIMS_BOARD_PARENT_WRITE_MAP,
    )

    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    # Update 277 status
    col_id = CLAIMS_BOARD_PARENT_WRITE_MAP.get("status_277", "cb_277_status")
    label_index = CLAIMS_BOARD_277_STATUS_TO_INDEX.get(status, "0")
    status_value = '{"index": ' + label_index + '}'

    try:
        run_query(mutation, {
            "itemId": str(claims_item_id),
            "boardId": str(claims_board_id),
            "columnId": col_id,
            "value": status_value,
        })
        logger.info(f"Claims Board 277 status → {status}")
    except Exception as e:
        logger.warning(f"Failed to update Claims Board 277 status: {e}")

    # Store rejection reason if rejected
    if status != "Accepted" and rejection_reason:
        reason_col_id = CLAIMS_BOARD_PARENT_WRITE_MAP.get("rejection_reason_277", "cb_277_reason")
        try:
            run_query(mutation, {
                "itemId": str(claims_item_id),
                "boardId": str(claims_board_id),
                "columnId": reason_col_id,
                "value": f'"{rejection_reason}"',
            })
        except Exception as e:
            logger.warning(f"Failed to store Claims Board rejection reason: {e}")


# ============================================================
# CLAIMS BOARD — WORKFLOW STATUS UPDATE
# ============================================================

def update_claims_board_workflow(claims_item_id: str, status: str) -> None:
    """
    Update the workflow/claim status on a Claims Board parent item.
    (e.g., Submitted, Accepted, Paid, etc.)
    """
    if is_mock_mode():
        _mock_mutation("update Claims Board workflow", claims_item_id=claims_item_id, status=status)
        return

    from claims_board_config import (
        CLAIMS_BOARD_STATUS_TO_INDEX,
        CLAIMS_BOARD_PARENT_WRITE_MAP,
    )

    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    col_id = CLAIMS_BOARD_PARENT_WRITE_MAP.get("claim_status", "cb_claim_status")
    label_index = CLAIMS_BOARD_STATUS_TO_INDEX.get(status, "1")
    status_value = '{"index": ' + label_index + '}'

    try:
        run_query(mutation, {
            "itemId": str(claims_item_id),
            "boardId": str(claims_board_id),
            "columnId": col_id,
            "value": status_value,
        })
        logger.info(f"Claims Board workflow status → {status}")
    except Exception as e:
        logger.warning(f"Failed to update Claims Board workflow status: {e}")


# ============================================================
# CLAIMS BOARD — UPDATE EXISTING SUBITEMS (ERA)
# ============================================================

def update_existing_claims_subitems(claims_item_id: str, children: list) -> None:
    """
    UPDATE existing subitems on a Claims Board item with ERA data.
    Matches by HCPC code (name of the subitem).

    This is the key migration change: instead of CREATING new subitems
    (old flow), we UPDATE the 5 pre-populated subitems that already
    have HCPC codes, quantities, etc.

    Matching strategy:
    1. Primary: Match ERA child HCPC code to subitem name
    2. Fallback: Positional matching if HCPC match fails
    """
    if is_mock_mode():
        _mock_mutation("update existing Claims Board subitems",
                       claims_item_id=claims_item_id,
                       children=len(children))
        return

    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    # Step 1: Fetch existing subitems
    query = """
    query GetSubitems($itemId: ID!) {
      items(ids: [$itemId]) {
        subitems {
          id
          name
          board { id }
          column_values { id text }
        }
      }
    }
    """
    try:
        result = run_query(query, {"itemId": claims_item_id})
        existing_subitems = (
            result.get("data", {})
            .get("items", [{}])[0]
            .get("subitems", [])
        )
    except Exception as e:
        logger.error(f"Failed to fetch existing subitems: {e}")
        # Fallback: create new subitems the old way
        populate_era_service_line_subitems(claims_item_id, children)
        return

    if not existing_subitems:
        logger.warning(f"No existing subitems found for {claims_item_id} — creating new ones")
        populate_era_service_line_subitems(claims_item_id, children)
        return

    # Step 2: Build HCPC → subitem mapping
    hcpc_to_subitem = {}
    for sub in existing_subitems:
        sub_name = sub.get("name", "").strip()
        # Check if name is a HCPC code directly
        if sub_name:
            hcpc_to_subitem[sub_name.upper()] = sub

        # Also check if subitem has a HCPC code in its columns
        for col in sub.get("column_values", []):
            if col.get("id") in ("cb_sub_hcpc_code",) and col.get("text"):
                hcpc_to_subitem[col["text"].upper()] = sub

    # Also build name-to-subitem mapping for product name matching
    from claims_board_config import HCPC_TO_PRODUCT
    name_to_subitem = {}
    for sub in existing_subitems:
        name_to_subitem[sub.get("name", "").strip().lower()] = sub

    update_mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    matched_count = 0
    unmatched_children = []

    for child in children:
        hcpc_code = child.get("HCPC Code", "").strip().upper()

        # Try to find matching subitem
        matched_subitem = None

        # Strategy 1: Match by HCPC code
        if hcpc_code and hcpc_code in hcpc_to_subitem:
            matched_subitem = hcpc_to_subitem[hcpc_code]

        # Strategy 2: Match by product name via HCPC → product lookup
        if not matched_subitem and hcpc_code:
            product_name = HCPC_TO_PRODUCT.get(hcpc_code, "")
            if product_name and product_name.lower() in name_to_subitem:
                matched_subitem = name_to_subitem[product_name.lower()]

        if not matched_subitem:
            unmatched_children.append(child)
            continue

        subitem_id = matched_subitem.get("id")
        subitem_board_id = matched_subitem.get("board", {}).get("id", "")

        if not subitem_id or not subitem_board_id:
            unmatched_children.append(child)
            continue

        # Write ERA fields to the matched subitem
        _write_era_fields_to_subitem(
            subitem_id, subitem_board_id, child, update_mutation
        )
        matched_count += 1

    # Strategy 3: Positional fallback for unmatched children
    if unmatched_children:
        remaining_subitems = [
            s for s in existing_subitems
            if s.get("id") not in {
                hcpc_to_subitem.get(c.get("HCPC Code", "").upper(), {}).get("id")
                for c in children if c not in unmatched_children
            }
        ]

        for i, child in enumerate(unmatched_children):
            if i < len(remaining_subitems):
                sub = remaining_subitems[i]
                subitem_id = sub.get("id")
                subitem_board_id = sub.get("board", {}).get("id", "")
                if subitem_id and subitem_board_id:
                    logger.warning(
                        f"Positional fallback: ERA {child.get('HCPC Code', '?')} → "
                        f"subitem {sub.get('name', '?')}"
                    )
                    _write_era_fields_to_subitem(
                        subitem_id, subitem_board_id, child, update_mutation
                    )
                    matched_count += 1
            else:
                logger.warning(
                    f"No subitem match for ERA HCPC={child.get('HCPC Code', '?')} — skipping"
                )

    logger.info(f"ERA subitem update: {matched_count}/{len(children)} matched")


def _write_era_fields_to_subitem(
    subitem_id: str,
    subitem_board_id: str,
    child: dict,
    update_mutation: str,
) -> None:
    """Write ERA payment fields to a single subitem."""
    fields = {
        "Primary Paid":         child.get("Primary Paid"),
        "Raw Service Date":     child.get("Raw Service Date"),
        "Raw Line Item Charge": child.get("Raw Line Item Charge Amount"),
        "Patient Control #":    child.get("Patient Control #"),
        "Claim Status Code":    child.get("Claim Status Code"),
        "Raw Line Control #":   child.get("Raw Line Item Control Number"),
        "Raw Allowed Actual":   child.get("Raw Allowed Actual"),
        "Parsed PR Amount":     child.get("Parsed PR Amount"),
        "Parsed Deductible":    child.get("Parsed Deductible Amount"),
        "Parsed Coinsurance":   child.get("Parsed Coinsurance Amount"),
        "Parsed Copay":         child.get("Parsed Copay Amount"),
        "Parsed Other PR":      child.get("Parsed Other PR Amount"),
        "Parsed CO Amount":     child.get("Parsed CO Amount"),
        "Parsed CO-45":         child.get("Parsed CO-45 Amount"),
        "Parsed CO-253":        child.get("Parsed CO-253 Amount"),
        "Parsed Other CO":      child.get("Parsed Other CO Amount"),
        "Parsed OA Amount":     child.get("Parsed OA Amount"),
        "Parsed PI Amount":     child.get("Parsed PI Amount"),
        "Parsed Remark Codes":  child.get("Parsed Remark Codes"),
        "Parsed Remark Text":   child.get("Parsed Remark Text"),
        "Parsed Adj Codes":     child.get("Parsed Adjustment Codes"),
        "Parsed Adj Reasons":   child.get("Parsed Adjustment Reasons"),
    }

    for field_name, value in fields.items():
        if value is None or value == "" or value == 0.0:
            continue

        col_id, col_type = SUBITEM_ERA_COLUMN_MAP.get(field_name, (None, None))
        if not col_id:
            continue

        try:
            if col_type == "number":
                formatted = str(value)
            elif col_type == "date":
                formatted = '{"date": "' + str(value) + '"}'
            elif col_type == "long_text":
                formatted = '{"text": "' + str(value).replace('"', "'") + '"}'
            else:
                formatted = f'"{str(value)}"'

            run_query(update_mutation, {
                "itemId":   str(subitem_id),
                "boardId":  str(subitem_board_id),
                "columnId": col_id,
                "value":    formatted,
            })
        except Exception as e:
            logger.warning(f"  ERA subitem update failed for {field_name}: {e}")


def get_column_settings(board_id: str, column_id: str) -> dict:
    """Debug: Get column settings to find valid status labels"""
    query = """
    query GetColumns($boardId: ID!) {
      boards(ids: [$boardId]) {
        columns {
          id
          title
          type
          settings_str
        }
      }
    }
    """
    result = run_query(query, {"boardId": board_id})
    columns = (
        result.get("data", {})
        .get("boards", [{}])[0]
        .get("columns", [])
    )
    for col in columns:
        if col.get("id") == column_id:
            return col
    return {}