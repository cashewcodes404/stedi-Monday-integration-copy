import logging
import json
import os
import time
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from services.monday_service import (
    get_order_item,
    update_277_status,
    create_claims_board_item,
    update_claim_status,
    post_claim_update_to_monday,
    store_claim_pcn,
    get_claims_board_item,
    update_claims_board_workflow,
)

from services.claim_builder_service import build_claims_from_monday_item, build_claims_from_claims_board_item
from services.stedi_service import submit_claim, get_277_acknowledgement
from claims_board_config import is_claims_board_mode


# ============================================================
# WEBHOOK AUTHENTICATION
# ============================================================

def verify_webhook_secret(request: Request) -> bool:
    """
    Verify incoming Monday webhook using WEBHOOK_SECRET.

    Monday sends the secret as the Authorization header value
    when you configure the webhook integration. If WEBHOOK_SECRET
    is set, every incoming request MUST include a matching header.

    If WEBHOOK_SECRET is not set, verification is skipped (open mode).
    This is intentional for initial setup when Monday sends the
    challenge request before you can configure the secret.
    """
    secret = os.getenv("WEBHOOK_SECRET", "")
    if not secret:
        return True  # No secret configured — open mode

    auth_header = request.headers.get("Authorization", "")
    if auth_header == secret:
        return True

    logger.warning(f"Webhook auth FAILED — expected secret but got: '{auth_header[:20]}...'")
    return False


MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def retry_operation(fn, description: str, retries: int = MAX_RETRIES):
    """Retry a function up to N times with exponential backoff."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            last_error = e
            if attempt < retries:
                delay = RETRY_DELAY * attempt
                logger.warning(f"{description} failed (attempt {attempt}/{retries}): {e} — retrying in {delay}s")
                time.sleep(delay)
            else:
                logger.error(f"{description} failed after {retries} attempts: {e}")
    raise last_error

logger = logging.getLogger(__name__)
router = APIRouter()

COLUMN_MAP = {
    "status":               "Claim Status",
    "text_mm18zjmz":        "Gender",
    "text_mm187t6a":        "DOB",
    "phone_mm18rr9v":       "Phone",
    "location_mm187v29":    "Patient Address",
    "color_mm189t0b":       "Diagnosis Code",
    "color_mm18ds28":       "CGM Coverage",
    "text_mm18w2y4":        "Doctor Name",
    "text_mm18x1kj":        "Doctor NPI",
    "location_mm18qfed":    "Doctor Address",
    "phone_mm18t5ct":       "Doctor Phone",
    "color_mm18jhq5":       "Primary Insurance",
    "text_mm18s3fe":        "Member ID",
    "color_mm18h6yn":       "PR Payor",
    "text_mm18c6z4":        "Secondary ID",
    "color_mm18h05q":       "Subscription Type",
    "color_mm1bx9az":       "277 Status",
    "text_mm1b56xa":        "277 Rejected Reason",
}


@router.post("/webhook")
async def monday_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    # Always allow challenge (Monday sends it before secret can be configured)
    if "challenge" in body:
        logger.info("Monday challenge received")
        return JSONResponse({"challenge": body["challenge"]})

    # Verify webhook secret on all non-challenge requests
    if not verify_webhook_secret(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    background_tasks.add_task(handle_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


# ============================================================
# PROCESS ORDER → CLAIMS BOARD (NEW ENDPOINT)
# ============================================================

@router.post("/webhook/process-order")
async def monday_process_order_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Webhook for Order Board → Claims Board flow.

    Trigger: Order Board status change to "Process Claim"
    Monday automation: "When status changes to Process Claim" → send webhook

    Flow:
      1. Read Order Board item (patient, insurance, doctor, product quantities)
      2. Compute HCPC codes, units, modifiers, insurance label, frequency
      3. Create Claims Board parent item with all patient/insurance data
      4. Create 5 product subitems with HCPC, Insurance, Frequency, Qty
      5. Monday formulas auto-compute Claim Qty and Est. Pay
      6. Update Order Board status → "Claim Sent to Review"

    SAFETY: This endpoint NEVER contacts Stedi. It only reads Order Board
    and writes to Claims Board. The human reviews on Claims Board and
    manually triggers "Submit Claim" when ready.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    # Always allow challenge
    if "challenge" in body:
        logger.info("Monday process-order challenge received")
        return JSONResponse({"challenge": body["challenge"]})

    # Verify webhook secret
    if not verify_webhook_secret(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    background_tasks.add_task(handle_process_order_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


async def handle_process_order_event(body: dict):
    """
    Handle an Order Board → Claims Board creation event.
    Reads Order Board item, computes claim fields, creates Claims Board
    parent + subitems for human review.
    """
    event = body.get("event", {})
    item_id = str(event.get("pulseId") or event.get("itemId") or "")
    board_id = str(event.get("boardId") or "")

    if not item_id:
        logger.warning("[ProcessOrder] No item ID in event — ignoring")
        return

    logger.info(f"[ProcessOrder] Triggered for Order Board item {item_id}")

    try:
        from services.monday_service import run_query
        from claims_board_config import (
            ORDER_BOARD_COLUMN_MAP,
            CLAIMS_BOARD_PARENT_WRITE_MAP,
            resolve_subitem_insurance_label,
            PRODUCT_CATEGORIES,
        )
        from claim_infrastructure import (
            normalize_date,
            split_full_name,
            parse_address,
        )
        from routes.order_to_claims import compute_all_product_subitems
        from claim_infrastructure import build_normalized_order_template

        # ── Step 1: Fetch Order Board item ──
        logger.info(f"[ProcessOrder] Step 1: Fetching Order Board item {item_id}")
        fetch_query = """
        query GetOrderItem($itemId: ID!) {
          items(ids: [$itemId]) {
            id
            name
            column_values { id text type value }
          }
        }
        """
        result = retry_operation(
            lambda: run_query(fetch_query, {"itemId": item_id}),
            f"Fetch Order Board item {item_id}",
        )
        order_item = result.get("data", {}).get("items", [{}])[0]

        if not order_item or not order_item.get("column_values"):
            logger.warning(f"[ProcessOrder] Order Board item {item_id} has no data")
            return

        # Parse columns using Order Board map
        cols = {}
        for cv in order_item.get("column_values", []):
            col_id = cv.get("id", "")
            field_name = ORDER_BOARD_COLUMN_MAP.get(col_id, col_id)
            cols[field_name] = cv.get("text", "") or ""

        patient_name = order_item.get("name", "")
        patient_first, patient_last = split_full_name(patient_name)
        patient_addr = parse_address(cols.get("patient_address", ""))

        doctor_full = cols.get("doctor_name", "")
        doctor_first, doctor_last = split_full_name(doctor_full)
        doctor_addr = parse_address(cols.get("doctor_address", ""))

        logger.info(
            f"[ProcessOrder] Patient={patient_name}, "
            f"Payer={cols.get('primary_insurance')}, "
            f"Type={cols.get('insurance_type')}, "
            f"Freq={cols.get('frequency')}"
        )

        # ── Step 2: Build normalized order for compute functions ──
        normalized = build_normalized_order_template()
        normalized["patient_full_name"]   = patient_name
        normalized["patient_first_name"]  = patient_first
        normalized["patient_last_name"]   = patient_last
        normalized["patient_dob"]         = normalize_date(cols.get("dob", ""))
        normalized["patient_address_1"]   = patient_addr.get("address1", "")
        normalized["patient_city"]        = patient_addr.get("city", "")
        normalized["patient_state"]       = patient_addr.get("state", "")
        normalized["patient_postal_code"] = patient_addr.get("postal_code", "")

        normalized["primary_insurance_name"] = cols.get("primary_insurance", "")
        normalized["member_id"]              = cols.get("member_id", "")
        normalized["subscription_type"]      = cols.get("subscription_type", "")
        normalized["diagnosis_code"]         = cols.get("diagnosis_code", "")

        normalized["doctor_name"]         = doctor_full
        normalized["doctor_first_name"]   = doctor_first
        normalized["doctor_last_name"]    = doctor_last
        normalized["doctor_npi"]          = cols.get("doctor_npi", "")
        normalized["doctor_address_1"]    = doctor_addr.get("address1", "")
        normalized["doctor_city"]         = doctor_addr.get("city", "")
        normalized["doctor_state"]        = doctor_addr.get("state", "")
        normalized["doctor_postal_code"]  = doctor_addr.get("postal_code", "")

        dos = cols.get("dos", "")
        normalized["order_date"]   = normalize_date(dos)
        normalized["service_date"] = normalize_date(dos)

        # ── Step 3: Normalize product quantities ──
        order_cols = dict(cols)
        order_cols["pump_qty"] = cols.get("pump_qty", "")

        # CGM sensors: Order Board doesn't have a4239_units, derive from monitor_qty
        # If CGM type is set and monitor_qty > 0, sensors are usually same qty
        cgm_type = cols.get("cgm_type", "")
        monitor_qty = cols.get("monitor_qty", "")
        # For sensors, we use monitor_qty as the sensor qty (each monitor order
        # comes with sensors). This is a reasonable default; human can edit.
        if cgm_type and monitor_qty and int(float(monitor_qty or "0")) > 0:
            order_cols["cgm_sensor_qty"] = monitor_qty
            order_cols["cgm_monitor_qty"] = monitor_qty
        else:
            order_cols["cgm_sensor_qty"] = ""
            order_cols["cgm_monitor_qty"] = monitor_qty or ""

        # Infusion: combine inf_1 + inf_2
        try:
            inf1 = int(float(cols.get("infusion_1_qty", "0") or "0"))
            inf2 = int(float(cols.get("infusion_2_qty", "0") or "0"))
            inf_total = inf1 + inf2
            order_cols["infusion_set_qty"] = str(inf_total) if inf_total > 0 else ""
        except (ValueError, TypeError):
            order_cols["infusion_set_qty"] = ""

        # Cartridge = same as infusion total
        order_cols["cartridge_qty"] = order_cols.get("infusion_set_qty", "")

        logger.info(
            f"[ProcessOrder] Qty: pump={order_cols.get('pump_qty')}, "
            f"sensor={order_cols.get('cgm_sensor_qty')}, "
            f"monitor={order_cols.get('cgm_monitor_qty')}, "
            f"infusion={order_cols.get('infusion_set_qty')}, "
            f"cartridge={order_cols.get('cartridge_qty')}"
        )

        # ── Step 4: Compute product subitems ──
        product_subitems = compute_all_product_subitems(normalized, order_cols)

        if not product_subitems:
            logger.warning(f"[ProcessOrder] No products with qty > 0 for item {item_id}")
            # Still create Claims Board item even with no products
            # (human might want to add them manually)

        # Enrich products with insurance label and frequency
        parent_payor = cols.get("primary_insurance", "")
        insurance_type = cols.get("insurance_type", "")
        insurance_label = resolve_subitem_insurance_label(parent_payor, insurance_type)

        frequency_text = cols.get("frequency", "")
        order_frequency = ""
        if "90" in frequency_text:
            order_frequency = "90-Days"
        elif "60" in frequency_text:
            order_frequency = "60-Days"

        for product in product_subitems:
            product["subitem_insurance_label"] = insurance_label
            product["order_frequency"] = order_frequency

        # ── Step 5: Create Claims Board parent item ──
        logger.info("[ProcessOrder] Step 5: Creating Claims Board parent item")
        claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

        # Item name: "Patient Name" (same format as existing Claims Board items)
        claims_item_name = patient_name

        create_item_mutation = """
        mutation CreateItem($boardId: ID!, $itemName: String!, $columnValues: JSON!) {
          create_item(board_id: $boardId, item_name: $itemName, column_values: $columnValues) {
            id
          }
        }
        """

        # Build column values JSON for Claims Board parent
        import json as _json

        # Map Order Board data → Claims Board parent columns
        parent_values = {}

        # DOB
        if cols.get("dob"):
            parent_values["text_mkp3y5ax"] = cols["dob"]

        # Member ID
        if cols.get("member_id"):
            parent_values["text_mktat89m"] = cols["member_id"]

        # Doctor
        if cols.get("doctor_name"):
            parent_values["text_mkxrh4a4"] = cols["doctor_name"]
        if cols.get("doctor_npi"):
            parent_values["text_mkxr2r9b"] = cols["doctor_npi"]

        # DOS (date column needs {"date": "YYYY-MM-DD"})
        if dos:
            parent_values["date_mkwr7spz"] = {"date": normalize_date(dos)}

        # Primary Payor (status column — use label text)
        if parent_payor:
            parent_values["color_mkxmhypt"] = {"label": parent_payor}

        # Insurance Type (status column)
        if insurance_type:
            parent_values["color_mkxmmm77"] = {"label": insurance_type}

        # Frequency (status column)
        if frequency_text:
            parent_values["color_mky4mb3y"] = {"label": frequency_text}

        # Subscription Type (status column)
        sub_type = cols.get("subscription_type", "")
        if sub_type:
            parent_values["color_mky1qvcf"] = {"label": sub_type}

        # Diagnosis (status column)
        dx = cols.get("diagnosis_code", "")
        if dx:
            parent_values["color_mky2gpz5"] = {"label": dx}

        # Patient Address (location column)
        if cols.get("patient_address"):
            parent_values["location_mkxxpesw"] = {
                "lat": "", "lng": "",
                "address": cols["patient_address"],
            }

        # Doctor Address (location column)
        if cols.get("doctor_address"):
            parent_values["location_mkxr251b"] = {
                "lat": "", "lng": "",
                "address": cols["doctor_address"],
            }

        # Product quantities on parent
        if cols.get("pump_qty") and cols["pump_qty"] != "0":
            parent_values["numeric_mkwz4zkt"] = cols["pump_qty"]
        if cols.get("infusion_1_qty") and cols["infusion_1_qty"] != "0":
            parent_values["numeric_mkwz337y"] = cols["infusion_1_qty"]
        if cols.get("infusion_2_qty") and cols["infusion_2_qty"] != "0":
            parent_values["numeric_mkwz9g9f"] = cols["infusion_2_qty"]
        if monitor_qty and monitor_qty != "0":
            parent_values["numeric_mkwzb2f4"] = monitor_qty  # E2103 units
            parent_values["numeric_mkwz251j"] = monitor_qty  # A4239 units
            parent_values["numeric_mkwzr5js"] = monitor_qty  # Monitor Qty

        # Authorization
        auth = cols.get("authorization", "")
        if auth:
            parent_values["text_mkwrb2t9"] = auth

        # Medicaid ID
        medicaid_id = cols.get("medicaid_id", "")
        if medicaid_id:
            parent_values["text_mkwrwrpc"] = medicaid_id

        # Customer Order (link back to Order Board item)
        parent_values["text_mkwzbcme"] = str(item_id)

        column_values_json = _json.dumps(parent_values)

        create_result = retry_operation(
            lambda: run_query(create_item_mutation, {
                "boardId": str(claims_board_id),
                "itemName": claims_item_name,
                "columnValues": column_values_json,
            }),
            "Create Claims Board parent item",
        )

        claims_item_id = (
            create_result.get("data", {})
            .get("create_item", {})
            .get("id", "")
        )

        if not claims_item_id:
            logger.error("[ProcessOrder] Failed to create Claims Board item — no ID returned")
            return

        logger.info(f"[ProcessOrder] Created Claims Board item {claims_item_id}: {claims_item_name}")

        # ── Step 6: Create product subitems ──
        if product_subitems:
            logger.info(f"[ProcessOrder] Step 6: Creating {len(product_subitems)} subitems")
            from services.monday_service import populate_claims_board_subitems
            populate_claims_board_subitems(claims_item_id, product_subitems)
            logger.info(f"[ProcessOrder] Created and populated {len(product_subitems)} subitems")

        # ── Step 7: Update Order Board status → "Claim Sent to Review" ──
        try:
            order_board_id = os.getenv("MONDAY_ORDER_BOARD_ID")
            update_mutation = """
            mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
              change_column_value(item_id: $itemId, board_id: $boardId, column_id: $columnId, value: $value) { id }
            }
            """
            run_query(update_mutation, {
                "itemId": str(item_id),
                "boardId": str(order_board_id),
                "columnId": "color_mkwtxw9r",  # Order Status column
                "value": '{"index": 7}',  # "Claim Sent to Review"
            })
            logger.info(f"[ProcessOrder] Order Board status → 'Claim Sent to Review'")
        except Exception as e:
            logger.warning(f"[ProcessOrder] Failed to update Order Board status: {e}")

        # ── Step 8: Store Claims Board item ID back on Order Board ──
        try:
            run_query(update_mutation, {
                "itemId": str(item_id),
                "boardId": str(order_board_id),
                "columnId": "text_mm1g99yk",  # Stedi Claim ID column (repurposed)
                "value": f'"{claims_item_id}"',
            })
            logger.info(f"[ProcessOrder] Stored Claims Board ID {claims_item_id} on Order Board")
        except Exception as e:
            logger.warning(f"[ProcessOrder] Failed to store Claims Board ID: {e}")

        logger.info(
            f"[ProcessOrder] ✅ Complete: Order {item_id} → Claims Board {claims_item_id} "
            f"with {len(product_subitems)} subitems"
        )

    except Exception as e:
        logger.error(f"[ProcessOrder] Failed for item {item_id}: {e}", exc_info=True)


@router.post("/webhook/populate")
async def monday_populate_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Webhook for Claims Board item creation / "populate" trigger.

    When a Monday automation fires on item creation (or a status change
    to a populate-type label), this endpoint:
      1. Reads the Claims Board item (patient data copied by Monday automation)
      2. Fetches the linked New Order Board item to get product quantities
      3. Computes HCPC codes, units, modifiers, estimated charges
      4. Writes those computed values BACK into the Claims Board subitems

    The human then reviews, possibly edits, and clicks "Submit Claim"
    which hits the main /webhook endpoint above.

    SAFETY: This endpoint NEVER contacts Stedi. It only reads/writes Monday.

    Monday automation config:
      Trigger: "When an item is created"
      Action:  "Send webhook" → POST to {RAILWAY_URL}/monday/webhook/populate
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    # Always allow challenge
    if "challenge" in body:
        logger.info("Monday populate challenge received")
        return JSONResponse({"challenge": body["challenge"]})

    # Verify webhook secret
    if not verify_webhook_secret(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    background_tasks.add_task(handle_populate_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


async def handle_populate_event(body: dict):
    """
    Handle a Claims Board item creation event.
    Computes claim field values and writes them back to the Claims Board
    so humans can review before submitting.
    """
    event = body.get("event", {})
    item_id = str(event.get("pulseId") or event.get("itemId") or "")
    board_id = str(event.get("boardId") or "")

    if not item_id:
        logger.warning("[Populate] No item ID in event — ignoring")
        return

    logger.info(f"[Populate] Triggered for Claims Board item {item_id}")

    try:
        # Step 1: Fetch the Claims Board item (patient info, insurance, etc.)
        claims_item = retry_operation(
            lambda: get_claims_board_item(item_id),
            f"Fetch Claims Board item {item_id}",
        )

        if not claims_item or not claims_item.get("column_values"):
            logger.warning(f"[Populate] Claims Board item {item_id} has no data yet — skipping")
            return

        # Step 2: Extract patient/order data from the Claims Board parent
        from routes.order_to_claims import (
            extract_new_order_columns,
            compute_all_product_subitems,
        )
        from claims_board_config import (
            CLAIMS_BOARD_PARENT_COLUMN_MAP,
            NEW_ORDER_BOARD_COLUMN_MAP,
        )
        from claim_infrastructure import (
            normalize_date,
            normalize_gender,
            split_full_name,
            parse_address,
            build_normalized_order_template,
        )
        from claim_assumptions import resolve_payer_name

        # Build a normalized order from Claims Board parent columns
        cols = {}
        for cv in claims_item.get("column_values", []):
            col_id = cv.get("id", "")
            field_name = CLAIMS_BOARD_PARENT_COLUMN_MAP.get(col_id, col_id)
            cols[field_name] = cv.get("text", "") or ""

        patient_full_name = claims_item.get("name", "")
        # Claims Board item names are typically "Patient Name - Payer"
        # Strip payer suffix if present
        if " - " in patient_full_name:
            patient_full_name = patient_full_name.split(" - ")[0].strip()

        patient_first, patient_last = split_full_name(patient_full_name)
        patient_addr = parse_address(cols.get("patient_address", ""))

        doctor_full_name = cols.get("doctor_name", "")
        doctor_first, doctor_last = split_full_name(doctor_full_name)
        doctor_addr = parse_address(cols.get("doctor_address", ""))

        normalized = build_normalized_order_template()

        # Patient
        normalized["patient_full_name"]   = patient_full_name
        normalized["patient_first_name"]  = patient_first
        normalized["patient_last_name"]   = patient_last
        normalized["patient_dob"]         = normalize_date(cols.get("dob", ""))
        normalized["patient_gender"]      = normalize_gender(cols.get("gender", ""))
        normalized["patient_address_1"]   = patient_addr.get("address1", "")
        normalized["patient_city"]        = patient_addr.get("city", "")
        normalized["patient_state"]       = patient_addr.get("state", "")
        normalized["patient_postal_code"] = patient_addr.get("postal_code", "")

        # Insurance
        normalized["primary_insurance_name"] = cols.get("primary_insurance", "")
        normalized["member_id"]              = cols.get("member_id", "")
        normalized["subscription_type"]      = cols.get("subscription_type", "")
        normalized["diagnosis_code"]         = cols.get("diagnosis_code", "")
        normalized["cgm_coverage"]           = cols.get("cgm_coverage", "")

        # Doctor
        normalized["doctor_name"]         = doctor_full_name
        normalized["doctor_first_name"]   = doctor_first
        normalized["doctor_last_name"]    = doctor_last
        normalized["doctor_npi"]          = cols.get("doctor_npi", "")
        normalized["doctor_address_1"]    = doctor_addr.get("address1", "")
        normalized["doctor_city"]         = doctor_addr.get("city", "")
        normalized["doctor_state"]        = doctor_addr.get("state", "")
        normalized["doctor_postal_code"]  = doctor_addr.get("postal_code", "")

        # Order metadata
        dos = cols.get("dos", "")
        normalized["order_date"]   = normalize_date(dos)
        normalized["service_date"] = normalize_date(dos)

        # Step 3: Get product quantities from Claims Board parent columns
        # The Claims Board parent has quantity columns that need to be mapped
        # to the PRODUCT_CATEGORIES qty_field names used by compute functions.
        order_cols = dict(cols)  # copy for compute functions

        # Normalize Claims Board parent column names → PRODUCT_CATEGORIES qty_field names
        # Parent column semantic names (from CLAIMS_BOARD_PARENT_COLUMN_MAP):
        #   pump_qty, infusion_1_qty, infusion_2_qty, monitor_qty,
        #   a4239_units, e0784_units, e2103_units
        # PRODUCT_CATEGORIES expects:
        #   pump_qty, infusion_set_qty, cartridge_qty, cgm_sensor_qty, cgm_monitor_qty
        order_cols["pump_qty"] = order_cols.get("pump_qty", "") or order_cols.get("e0784_units", "")

        # CGM sensors: mapped from a4239_units on parent
        order_cols["cgm_sensor_qty"] = order_cols.get("a4239_units", "")

        # CGM monitor: mapped from e2103_units or monitor_qty on parent
        order_cols["cgm_monitor_qty"] = order_cols.get("e2103_units", "") or order_cols.get("monitor_qty", "")

        # Infusion: combine infusion_1_qty + infusion_2_qty
        try:
            inf1 = int(float(order_cols.get("infusion_1_qty", "0") or "0"))
            inf2 = int(float(order_cols.get("infusion_2_qty", "0") or "0"))
            inf_total = inf1 + inf2
            order_cols["infusion_set_qty"] = str(inf_total) if inf_total > 0 else ""
        except (ValueError, TypeError):
            order_cols["infusion_set_qty"] = ""

        # Cartridge: same count as infusion sets (each infusion set pairs with cartridges)
        order_cols["cartridge_qty"] = order_cols.get("infusion_set_qty", "")

        logger.info(
            f"[Populate] Qty mapping: pump={order_cols.get('pump_qty')}, "
            f"sensor={order_cols.get('cgm_sensor_qty')}, "
            f"monitor={order_cols.get('cgm_monitor_qty')}, "
            f"infusion={order_cols.get('infusion_set_qty')}, "
            f"cartridge={order_cols.get('cartridge_qty')}"
        )

        # Also try reading from existing Claims Board subitems (as fallback)
        existing_subitems = claims_item.get("subitems", [])
        if existing_subitems:
            # Claims Board subitems use numeric_mm1czbyg for Order Quantity
            for sub in existing_subitems:
                sub_name = sub.get("name", "").strip().lower()
                for cv in sub.get("column_values", []):
                    if cv.get("id") == "numeric_mm1czbyg" and cv.get("text"):
                        qty_val = cv["text"]
                        if "pump" in sub_name and not order_cols.get("pump_qty"):
                            order_cols["pump_qty"] = qty_val
                        elif "sensor" in sub_name and not order_cols.get("cgm_sensor_qty"):
                            order_cols["cgm_sensor_qty"] = qty_val
                        elif "monitor" in sub_name and not order_cols.get("cgm_monitor_qty"):
                            order_cols["cgm_monitor_qty"] = qty_val
                        elif "infusion" in sub_name and not order_cols.get("infusion_set_qty"):
                            order_cols["infusion_set_qty"] = qty_val
                        elif "cartridge" in sub_name and not order_cols.get("cartridge_qty"):
                            order_cols["cartridge_qty"] = qty_val

        # Step 4: Compute HCPC codes, units, modifiers, charges for each product
        product_subitems = compute_all_product_subitems(normalized, order_cols)

        if not product_subitems:
            logger.warning(f"[Populate] No products with qty > 0 for item {item_id}")
            return

        # Step 4b: Enrich each product with subitem-specific fields
        # The Monday formulas for Claim Qty and Est. Pay depend on
        # Primary Insurance and Order Frequency being set on each subitem.
        from claims_board_config import resolve_subitem_insurance_label

        parent_payor = cols.get("primary_payor", "")
        insurance_type = cols.get("insurance_type", "")
        insurance_label = resolve_subitem_insurance_label(parent_payor, insurance_type)

        # Determine order frequency from parent (Frequency status column)
        # The parent has both a status Frequency (color_mky4mb3y) and numeric Frequency (numeric_mm15t7ed)
        # We read the status column text which is "60-Day" / "90-Day" / "30-Day"
        frequency_text = cols.get("frequency", "")
        # Normalize: parent uses "60-Day" but subitems use "60-Days"
        order_frequency = ""
        if "90" in frequency_text:
            order_frequency = "90-Days"
        elif "60" in frequency_text:
            order_frequency = "60-Days"
        elif "30" in frequency_text:
            order_frequency = "60-Days"  # Closest match; 30-day not on subitems

        for product in product_subitems:
            product["subitem_insurance_label"] = insurance_label
            product["order_frequency"] = order_frequency

        logger.info(
            f"[Populate] Enriched {len(product_subitems)} products: "
            f"insurance={insurance_label}, frequency={order_frequency}"
        )

        # Step 5: Write computed values back to Claims Board subitems
        from services.monday_service import populate_claims_board_subitems
        populate_claims_board_subitems(item_id, product_subitems)

        logger.info(
            f"[Populate] Done: wrote {len(product_subitems)} product subitems "
            f"back to Claims Board item {item_id}"
        )

        # Step 6: Update workflow status to indicate fields are populated
        try:
            update_claims_board_workflow(claims_item_id=item_id, status="Pending")
            logger.info(f"[Populate] Set workflow → Pending (ready for review)")
        except Exception as e:
            logger.warning(f"[Populate] Workflow update failed: {e}")

    except Exception as e:
        logger.error(f"[Populate] Failed for item {item_id}: {e}", exc_info=True)


@router.get("/test-payer/{name}")
async def test_payer_lookup(name: str):
    """Test payer name lookup from Stedi directory"""
    from services.stedi_service import lookup_payer_name_by_internal
    result = lookup_payer_name_by_internal(name)
    return {"internal": name, "official": result}


async def handle_event(body: dict):
    event = body.get("event", {})
    item_id = str(event.get("pulseId") or event.get("itemId") or "")
    new_label = event.get("value", {}).get("label", {}).get("text", "")

    logger.info(f"Status: '{new_label}' | item: {item_id}")

    if new_label == "Submit Claim":
        is_test = False
    elif new_label == "Test Claim Submitted":
        logger.info("Test Claim Submitted triggered")
        is_test = True
    else:
        logger.info(f"Ignored — status is '{new_label}'")
        return

    # Route to the correct handler based on SUBMISSION_SOURCE
    if is_claims_board_mode():
        logger.info(f"CLAIMS BOARD MODE: Routing to Claims Board handler")
        await handle_claims_board_event(item_id, is_test)
    else:
        logger.info(f"ORDER BOARD MODE: Routing to legacy Order Board handler")
        await handle_order_board_event(item_id, is_test)


async def handle_claims_board_event(item_id: str, is_test: bool) -> None:
    """
    Claims Board flow: Read pre-computed values from Claims Board subitems,
    build Stedi claim JSON using those values, submit, and update workflow.

    SAFETY: If STEDI_API_KEY is not set, submit_claim() returns a mock
    response and NOTHING is sent to any insurance payer.
    """
    # Safety check — log clearly whether this is live or mock
    from services.stedi_service import is_mock_mode as stedi_mock
    if stedi_mock():
        logger.info(f"[CB] ⚠️  STEDI MOCK MODE — no real claim will be submitted")
    else:
        logger.info(f"[CB] 🔴 LIVE MODE — claims will be sent to real payers via Stedi")

    # Step 1: Fetch Claims Board item (with retry)
    logger.info(f"[CB] Step 1: Fetching Claims Board item {item_id}")
    try:
        claims_data = retry_operation(
            lambda: get_claims_board_item(item_id),
            f"Fetch Claims Board item {item_id}",
        )
    except Exception as e:
        logger.error(f"[CB] Failed to fetch Claims Board item: {e}", exc_info=True)
        return

    if not claims_data or not claims_data.get("column_values"):
        logger.error(f"[CB] Claims Board item {item_id} returned empty/invalid data")
        return

    # Step 2: Build Stedi claim JSON using pre-computed values
    logger.info("[CB] Step 2: Building Stedi claim JSON from Claims Board")
    try:
        stedi_payloads = build_claims_from_claims_board_item(claims_data)
        if not stedi_payloads:
            logger.warning("[CB] No payloads generated from Claims Board")
            return
        logger.info(f"[CB] Built {len(stedi_payloads)} payload(s)")
    except Exception as e:
        logger.error(f"[CB] Failed to build claim: {e}", exc_info=True)
        return

    submitted_claims = []

    # Step 3: Submit each payload
    for i, payload in enumerate(stedi_payloads, 1):
        if is_test:
            payload["tradingPartnerServiceId"] = "STEDITEST"
            payload["tradingPartnerName"]      = "Stedi Test Payer"
            payload["receiver"]                = {"organizationName": "Stedi"}
            payload["usageIndicator"]          = "T"
            logger.info("[CB] TEST MODE: payload overridden to Stedi Test Payer")

        payer = payload.get("tradingPartnerName", "Unknown")
        logger.info(f"[CB] Step 3: Submitting payload #{i} | payer={payer}")

        try:
            stedi_response = submit_claim(payload)
            claim_id               = stedi_response.get("claim_id", "")
            patient_control_number = stedi_response.get("patient_control_number", "")
            inline_277_status      = stedi_response.get("inline_277_status", "Pending")

            logger.info(f"[CB] Submitted: claim_id={claim_id} | pcn={patient_control_number}")

            # Step 4a: Update Claims Board workflow status → Submitted
            try:
                retry_operation(
                    lambda: update_claims_board_workflow(claims_item_id=item_id, status="Submitted"),
                    f"Update Claims Board workflow for {item_id}",
                )
            except Exception as e:
                logger.warning(f"[CB] Workflow update failed: {e}")

            # Step 4a2: Set Primary Status → Outstanding (per dev brief step 20)
            try:
                from services.monday_service import run_query
                claims_board_id = __import__('os').getenv("MONDAY_CLAIMS_BOARD_ID")
                mutation = """
                mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
                  change_column_value(item_id: $itemId, board_id: $boardId, column_id: $columnId, value: $value) { id }
                }
                """
                run_query(mutation, {
                    "itemId": str(item_id),
                    "boardId": str(claims_board_id),
                    "columnId": "color_mkxmywtb",
                    "value": '{"index": 0}',
                })
                logger.info(f"[CB] Primary Status → Outstanding")
            except Exception as e:
                logger.warning(f"[CB] Primary Status update failed: {e}")

            # Step 4b: Store correlation ID / PCN on Claims Board
            try:
                from services.monday_service import run_query
                claims_board_id = __import__('os').getenv("MONDAY_CLAIMS_BOARD_ID")
                mutation = """
                mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
                  change_column_value(item_id: $itemId, board_id: $boardId, column_id: $columnId, value: $value) { id }
                }
                """
                run_query(mutation, {
                    "itemId": str(item_id),
                    "boardId": str(claims_board_id),
                    "columnId": "text_mkwzbcme",
                    "value": f'"{patient_control_number}"',
                })
                logger.info(f"[CB] Stored PCN={patient_control_number} on Claims Board item")
            except Exception as e:
                logger.warning(f"[CB] PCN store failed: {e}")

            # Step 4c: Collect for batch update
            submitted_claims.append({
                "claim_id": claim_id,
                "payer": payer,
                "pcn": patient_control_number,
                "payload": payload,
            })

            # Step 4d: Inline 277 status
            if inline_277_status != "Pending":
                try:
                    from services.monday_service import update_claims_board_277
                    update_claims_board_277(
                        claims_item_id=item_id,
                        status=inline_277_status,
                    )
                except Exception as e:
                    logger.warning(f"[CB] Inline 277 update failed: {e}")

            # Step 4e: Set Claim Sent Date
            try:
                from datetime import date
                from services.monday_service import run_query
                claims_board_id = __import__('os').getenv("MONDAY_CLAIMS_BOARD_ID")
                mutation = """
                mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
                  change_column_value(item_id: $itemId, board_id: $boardId, column_id: $columnId, value: $value) { id }
                }
                """
                today = date.today().isoformat()
                run_query(mutation, {
                    "itemId": str(item_id),
                    "boardId": str(claims_board_id),
                    "columnId": "date_mm14rk8d",
                    "value": '{"date": "' + today + '"}',
                })
            except Exception as e:
                logger.warning(f"[CB] Claim Sent Date update failed: {e}")

        except Exception as e:
            logger.error(f"[CB] Failed on payload #{i}: {e}", exc_info=True)

    if submitted_claims:
        # Step 5: Post comment with claim_id + PCN (dev brief step 20, bullet 5)
        try:
            post_claim_update_to_monday(
                item_id=item_id,
                submitted_claims=submitted_claims,
                is_test=is_test,
            )
            logger.info(f"[CB] Posted claim update comment to Claims Board item {item_id}")
        except Exception as e:
            logger.warning(f"[CB] Monday update post failed: {e}")

        logger.info(f"[CB] Successfully submitted {len(submitted_claims)} claim(s)")


async def handle_order_board_event(item_id: str, is_test: bool) -> None:
    """
    Legacy Order Board flow: Read from Order Board subitems,
    compute everything from scratch, submit, create Claims Board item.
    """
    # Step 1: Fetch order (with retry)
    logger.info(f"Step 1: Fetching order {item_id} | test={is_test}")
    try:
        order_data = retry_operation(
            lambda: get_order_item(item_id),
            f"Fetch order {item_id}",
        )
        log_order_data(order_data)
    except Exception as e:
        logger.error(f"Failed to fetch order after retries: {e}", exc_info=True)
        return

    # Step 2: Build Stedi claim JSON
    logger.info("Step 2: Building Stedi claim JSON")
    try:
        stedi_payloads = build_claims_from_monday_item(order_data)
        if not stedi_payloads:
            logger.warning("No payloads generated")
            return
        logger.info(f"Built {len(stedi_payloads)} payload(s)")
    except Exception as e:
        logger.error(f"Failed to build claim: {e}", exc_info=True)
        return

    submitted_claims = []

    # Step 3: Submit each payload
    for i, payload in enumerate(stedi_payloads, 1):

        # Override payer to Stedi Test Payer in test mode
        if is_test:
            payload["tradingPartnerServiceId"] = "STEDITEST"
            payload["tradingPartnerName"]      = "Stedi Test Payer"
            payload["receiver"]                = {"organizationName": "Stedi"}
            payload["usageIndicator"]          = "T"
            logger.info("TEST MODE: payload overridden to Stedi Test Payer (usageIndicator=T)")

        payer = payload.get("tradingPartnerName", "Unknown")
        logger.info(f"Step 3: Submitting payload #{i} | payer={payer} | test={is_test}")

        try:
            stedi_response = submit_claim(payload)
            claim_id               = stedi_response.get("claim_id", "")
            transaction_id         = stedi_response.get("transaction_id", "")
            patient_control_number = stedi_response.get("patient_control_number", "")
            inline_277_status      = stedi_response.get("inline_277_status", "Pending")

            logger.info(f"Submitted: claim_id={claim_id} | pcn={patient_control_number}")

            # Step 4a: Update Order Board → Submitted (with retry)
            try:
                retry_operation(
                    lambda: update_claim_status(item_id=item_id, status="Submitted"),
                    f"Update status for item {item_id}",
                )
                logger.info("Order status → Submitted")
            except Exception as e:
                logger.warning(f"Status update failed after retries: {e}")

            # Step 4b: Store PCN on Order Board item (with retry)
            try:
                retry_operation(
                    lambda pcn=patient_control_number, cid=claim_id: store_claim_pcn(
                        item_id=item_id, pcn=pcn, claim_id=cid,
                    ),
                    f"Store PCN for item {item_id}",
                )
                logger.info(f"Stored claim_id={claim_id} on order item {item_id}")
            except Exception as e:
                logger.warning(f"PCN store failed after retries: {e}")

            # Step 4c: Collect for batch update comment
            submitted_claims.append({
                "claim_id": claim_id,
                "payer": payer,
                "pcn": patient_control_number,
                "payload": payload,
            })

            # Step 4d: Update inline 277 status if already available
            if inline_277_status != "Pending":
                try:
                    update_277_status(
                        item_id=item_id,
                        status=inline_277_status,
                        rejection_reason="",
                    )
                    logger.info(f"277 status → {inline_277_status}")
                except Exception as e:
                    logger.warning(f"277 update failed: {e}")

            # Step 5: Always create Claims Board item (claim was submitted successfully)
            try:
                claims_item_id = retry_operation(
                    lambda: create_claims_board_item(
                        order_item=order_data,
                        claim_id=claim_id,
                        payer_name=payer,
                    ),
                    f"Create Claims Board item for {patient_control_number}",
                )
                logger.info(f"Claims Board item created: {claims_item_id}")
            except Exception as e:
                logger.warning(f"Claims Board creation failed after retries: {e}")

        except Exception as e:
            logger.error(f"Failed on payload #{i}: {e}", exc_info=True)

    if submitted_claims:
        try:
            post_claim_update_to_monday(
                item_id=item_id,
                submitted_claims=submitted_claims,
                is_test=is_test,
            )
        except Exception as e:
            logger.warning(f"Monday update post failed: {e}")


def log_order_data(order: dict):
    logger.info("=" * 60)
    logger.info("FULL ORDER DATA FROM MONDAY")
    logger.info(f"Item ID   : {order.get('id')}")
    logger.info(f"Item Name : {order.get('name')}")
    logger.info("--- Column Values ---")
    for col in order.get("column_values", []):
        col_id = col.get("id")
        col_name = COLUMN_MAP.get(col_id, col_id)
        col_value = col.get("text") or "empty"
        # logger.info(f"  {col_name:30} | {col_value}")
    logger.info("--- Sub Items ---")
    for sub in order.get("subitems", []):
        logger.info(f"  Subitem: {sub.get('name')}")
        for col in sub.get("column_values", []):
            if col.get("text"):
                logger.info(f"    {col.get('id'):30} | {col.get('text')}")
    logger.info("=" * 60)
