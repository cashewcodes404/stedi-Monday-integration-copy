"""
routes/stedi_webhook.py
========================
Handles incoming webhooks FROM Stedi.

Stedi fires this webhook when:
- A 277 acknowledgement is ready
- An 835 ERA is ready

Endpoint: POST /webhooks/stedi
"""

import logging
import os
from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from services.era_parser_service import (
    match_era_rows_to_claim_item,
    parse_era_from_string,
    summarize_era_row_for_monday,
)
from services.monday_service import populate_era_data_on_claims_item
from services.stedi_service import get_era_as_835_file, get_277_report

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Stedi Webhook"])


@router.post("/webhook")
async def stedi_webhook(request: Request, background_tasks: BackgroundTasks):
    """Receives Stedi transaction.processed.v2 events"""
    body: Dict[str, Any] = await request.json()

    # Return 200 immediately — Stedi requires response within 5 seconds
    background_tasks.add_task(handle_stedi_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


async def handle_stedi_event(body: dict) -> None:
    """Process Stedi event asynchronously"""

    # Stedi wraps the event under "event" key
    event = body.get("event", body)  # fallback to body itself if not wrapped

    event_id   = event.get("id", "")
    event_type = event.get("detail-type", "")
    detail     = event.get("detail", {})

    logger.info(f"Stedi event: id={event_id} | type={event_type}")

    if event_type != "transaction.processed.v2":
        logger.info(f"Ignored event type: {event_type}")
        return

    transaction_id = detail.get("transactionId", "")

    # transactionSetIdentifier can be int or string
    x12_meta = detail.get("x12", {}).get("metadata", {}).get("transaction", {})
    tx_set   = str(x12_meta.get("transactionSetIdentifier", ""))

    logger.info(f"Transaction: id={transaction_id} | set={tx_set}")

    if tx_set == "277":
        await handle_277_event(transaction_id, detail)
    elif tx_set == "835":
        await handle_835_event(transaction_id, detail)
    else:
        logger.info(f"Unhandled transaction set: {tx_set}")

async def handle_277_event(transaction_id: str, detail: dict) -> None:
    """Parse 277CA and update Monday Order Board 277 Status"""
    logger.info(f"[277] Processing transaction_id={transaction_id}")
    try:
        from services.stedi_service import get_277_report
        from services.monday_service import update_277_status

        report = get_277_report(transaction_id)
        logger.info(f"[277] Report fetched successfully")

        # Parse status from 277 report
        status, rejection_reason, patient_account_number = parse_277_status(report)
        logger.info(f"[277] Status={status} | PCN={patient_account_number}")

        # Find Order Board item by patientControlNumber
        item_id = find_order_item_by_pcn(patient_account_number)
        if not item_id:
            logger.warning(f"[277] No Order Board item found for PCN={patient_account_number}")
            return

        # Update 277 Status on Order Board
        update_277_status(
            item_id=item_id,
            status=status,
            rejection_reason=rejection_reason,
        )
        logger.info(f"[277] Updated Monday item {item_id} → {status}")

    except Exception as e:
        logger.error(f"[277] Failed: {e}", exc_info=True)


def parse_277_status(report: dict) -> tuple:
    """
    Extract claim status from 277 report.
    Returns (status, rejection_reason, patient_account_number)
    """
    try:
        claims = (
            report.get("transactions", [{}])[0]
            .get("payers", [{}])[0]
            .get("claimStatusTransactions", [{}])[0]
            .get("claimStatusDetails", [{}])[0]
            .get("patientClaimStatusDetails", [{}])[0]
            .get("claims", [{}])[0]
        )

        claim_status = claims.get("claimStatus", {})
        patient_account_number = claims.get("patientAccountNumber", "")

        # patientAccountNumber is on claimStatus, not claims — try both
        if not patient_account_number:
            patient_account_number = claim_status.get("patientAccountNumber", "")

        info_statuses = (
            claim_status
            .get("informationClaimStatuses", [{}])[0]
            .get("informationStatuses", [{}])[0]
        )

        category_code = info_statuses.get("healthCareClaimStatusCategoryCode", "")
        status_value  = info_statuses.get("statusCodeValue", "")

        # A1 = Accepted, A2 = Not Found, A3 = Rejected, A4 = Pending
        if category_code == "A1":
            status = "Accepted"
            rejection_reason = ""
        elif category_code in ("A2", "A3"):
            status = "Rejected"
            rejection_reason = status_value
        else:
            status = "Pending"
            rejection_reason = status_value

        logger.info(f"[277] category={category_code} | status={status} | pcn={patient_account_number}")
        return status, rejection_reason, patient_account_number

    except Exception as e:
        logger.error(f"[277] parse failed: {e}")
        return "Unknown", "", ""


def find_order_item_by_pcn(patient_control_number: str) -> str:
    """Find Order Board item by patient control number (paginated)."""
    from services.monday_service import search_board_items
    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")
    item_id = search_board_items(board_id, "text_mm1ra2v1", patient_control_number)
    if item_id:
        logger.info(f"Found Order item {item_id} for PCN={patient_control_number}")
    return item_id

async def handle_835_event(transaction_id: str, detail: dict) -> None:
    """
    Handle 835 ERA payment.
    1. Fetch ERA report from Stedi
    2. Parse ERA JSON
    3. Find matching Claims Board item
    4. Populate Monday Claims Board
    """
    logger.info(f"[835] Processing transaction_id={transaction_id}")
    try:
        # Step 1: Fetch ERA from Stedi
        era_content = get_era_as_835_file(transaction_id)
        if not era_content:
            logger.warning(f"[835] Empty ERA for {transaction_id}")
            return

        logger.info(f"[835] ERA fetched, length={len(era_content)}")

        # Step 2: Parse ERA JSON
        from services.era_parser_service import parse_era_from_string, summarize_era_row_for_monday

        era_rows = parse_era_from_string(era_content)
        if not era_rows:
            logger.warning(f"[835] No rows parsed — check ERA format in logs above")
            return

        logger.info(f"[835] Parsed {len(era_rows)} ERA row(s)")

        # Step 3 & 4: For each parsed row, find Claims Board item and populate
        for era_row in era_rows:
            parent = era_row.get("parent", {})
            patient_control_num = parent.get("raw_patient_control_num", "")

            logger.info(
                f"[835] PCN={patient_control_num} | "
                f"paid={parent.get('primary_paid')} | "
                f"pr={parent.get('pr_amount')}"
            )

            claims_item_id = _find_claims_item_by_correlation_id(transaction_id)
            if not claims_item_id:
                logger.info(f"[835] No item by transaction_id, trying PCN={patient_control_num}")
                claims_item_id = _find_claims_item_by_pcn(patient_control_num)

            if not claims_item_id:
                logger.warning(f"[835] No Claims Board item found for PCN={patient_control_num}")
                continue

            logger.info(f"[835] Found Claims Board item: {claims_item_id}")

            summary = summarize_era_row_for_monday(era_row)
            from services.monday_service import populate_era_data_on_claims_item
            populate_era_data_on_claims_item(claims_item_id, summary)
            logger.info(f"[835] Populated Claims Board item {claims_item_id}")

    except Exception as e:
        logger.error(f"[835] Failed: {e}", exc_info=True)


def _find_claims_item_by_pcn(patient_control_num: str) -> str:
    """Find Claims Board item by patient control number (paginated)."""
    from services.monday_service import search_board_items
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
    item_id = search_board_items(claims_board_id, "text_mkwzbcme", patient_control_num)
    if item_id:
        logger.info(f"Found Claims Board item {item_id} for pcn={patient_control_num}")
    return item_id

async def process_era_response(
    era_id: str,
    claim_id: str,
    patient_control_number: str,
) -> None:
    """
    Full ERA processing pipeline.
    1. Fetch raw 835 from Stedi
    2. Parse ERA JSON
    3. Find matching Claims Board item by correlationId/claim_id
    4. Populate parent columns with ERA data
    """
    try:
        logger.info(f"[ERA {era_id}] Fetching from Stedi")
        era_content = get_era_as_835_file(era_id)

        if not era_content:
            logger.warning(f"[ERA {era_id}] Empty content")
            return

        logger.info(f"[ERA {era_id}] Parsing ERA")
        era_rows = parse_era_from_string(era_content)

        if not era_rows:
            logger.warning(f"[ERA {era_id}] No rows parsed")
            return

        # Match by patient control number if provided
        if patient_control_number:
            era_rows = match_era_rows_to_claim_item(era_rows, patient_control_number)

        if not era_rows:
            logger.warning(f"[ERA {era_id}] No rows matched PCN={patient_control_number}")
            return

        # Find Claims Board item
        claims_item_id = _find_claims_item_by_claim_id(claim_id)
        if not claims_item_id:
            logger.warning(f"[ERA {era_id}] No Claims Board item for claim_id={claim_id}")
            return

        # Populate Monday Claims Board
        for era_row in era_rows:
            summary = summarize_era_row_for_monday(era_row)
            populate_era_data_on_claims_item(claims_item_id, summary)
            logger.info(f"[ERA {era_id}] Populated claims item {claims_item_id}")

    except Exception as e:
        logger.error(f"[ERA {era_id}] ERA processing failed: {e}", exc_info=True)


def _find_claims_item_by_claim_id(claim_id: str) -> str:
    """Find Claims Board item by Stedi claim_id (paginated)."""
    from services.monday_service import search_board_items
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
    return search_board_items(claims_board_id, "text_stedi_claim_id", claim_id)

def _find_claims_item_by_correlation_id(correlation_id: str) -> str:
    """Find Claims Board item by Stedi correlationId (paginated)."""
    from services.monday_service import search_board_items
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
    item_id = search_board_items(claims_board_id, "text_mkwzbcme", correlation_id)
    if item_id:
        logger.info(f"Found Claims item {item_id} by correlationId={correlation_id}")
    return item_id

@router.post("/277")
async def stedi_277_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    background_tasks.add_task(handle_stedi_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


@router.post("/835")
async def stedi_835_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    background_tasks.add_task(handle_stedi_event, body)
    return JSONResponse({"status": "received"}, status_code=200)
