import logging
import json
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

    if "challenge" in body:
        logger.info("Monday challenge received")
        return JSONResponse({"challenge": body["challenge"]})

    background_tasks.add_task(handle_event, body)
    return JSONResponse({"status": "received"}, status_code=200)

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
    """
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
