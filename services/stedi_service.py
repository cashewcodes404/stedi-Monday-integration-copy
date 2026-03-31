"""
services/stedi_service.py
"""

import os
import json
import logging
import requests
from dotenv import load_dotenv
from functools import lru_cache

load_dotenv()
logger = logging.getLogger(__name__)


def is_mock_mode() -> bool:
    """Check if running without real API keys (mock mode)."""
    return not os.getenv("STEDI_API_KEY")


# Correct URL from Stedi docs
STEDI_BASE_URL = "https://healthcare.us.stedi.com/2024-04-01"
STEDI_CLAIMS_URL = f"{STEDI_BASE_URL}/change/medicalnetwork/professionalclaims/v3/submission"
STEDI_277_URL = f"{STEDI_BASE_URL}/change/medicalnetwork/claimstatus/v3"
STEDI_835_URL = f"{STEDI_BASE_URL}/reports/era"
STEDI_PAYER_SEARCH_URL = f"{STEDI_BASE_URL}/payers/search"

def get_stedi_headers(idempotency_key: str = None) -> dict:
    api_key = os.getenv("STEDI_API_KEY")
    if not api_key:
        raise ValueError("STEDI_API_KEY not set in .env")

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }

    # Idempotency key prevents duplicate claims on retry
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    return headers

@lru_cache(maxsize=256)
def lookup_payer_name(payer_id: str) -> str:
    """
    Look up the official payer name from Stedi's network directory.
    Uses payer_id to search and returns the exact Stedi network name.
    Results are cached so we don't call the API repeatedly for same payer.
    """
    if not payer_id:
        return ""

    api_key = os.getenv("STEDI_API_KEY")
    if not api_key:
        logger.warning("STEDI_API_KEY not set — skipping payer lookup")
        return ""

    try:
        response = requests.get(
            STEDI_PAYER_SEARCH_URL,
            params={"payerId": payer_id},
            headers=get_stedi_headers(),
            timeout=10,
        )
        response.raise_for_status()
        result = response.json()

        payers = result.get("payers", [])
        if payers:
            official_name = payers[0].get("payerName", "")
            logger.info(f"Payer lookup: id={payer_id} → name={official_name}")
            return official_name

        logger.warning(f"No payer found for id={payer_id}")
        return ""

    except Exception as e:
        logger.error(f"Payer lookup failed for id={payer_id}: {e}")
        return ""


def submit_claim(payload: dict) -> dict:
    """Submit a claim JSON to Stedi API. Returns mock response if no API key."""
    payer = payload.get("tradingPartnerName", "Unknown")
    logger.info(f"Submitting claim: payer={payer}")

    patient_control_number = (
        payload.get("claimInformation", {})
        .get("patientControlNumber", "")
    )

    if is_mock_mode():
        import uuid
        mock_id = f"MOCK_{uuid.uuid4().hex[:12].upper()}"
        logger.info(f"MOCK MODE: Returning mock response for payer={payer}")
        return {
            "claim_id": mock_id,
            "transaction_id": f"MOCK_TXN_{uuid.uuid4().hex[:8].upper()}",
            "patient_control_number": patient_control_number,
            "inline_277_status": "Accepted",
            "status": "mock_submitted",
            "raw": {"mock": True},
        }

    try:
        response = requests.post(
            STEDI_CLAIMS_URL,
            json=payload,
            headers=get_stedi_headers(idempotency_key=patient_control_number),
            timeout=30,
        )

        logger.info(f"Stedi response status: {response.status_code}")
        result = response.json()
        logger.info(f"Stedi full response: {json.dumps(result, indent=2)}")

        response.raise_for_status()

        claim_id = (
                result.get("claimReference", {}).get("correlationId") or
                result.get("claimReference", {}).get("claimId") or
                result.get("correlationId") or
                patient_control_number
        )

        transaction_id = (
                result.get("claimReference", {}).get("rhclaimNumber") or
                result.get("transactionId") or
                ""
        )

        logger.info(f"Claim submitted: claim_id={claim_id} | transaction_id={transaction_id}")

        return {
            "claim_id": claim_id,
            "transaction_id": transaction_id,
            "patient_control_number": patient_control_number,
            "inline_277_status": parse_inline_277_status(result),
            "status": "submitted",
            "raw": result,
        }

    except requests.exceptions.HTTPError as e:
        logger.error(f"Stedi HTTP error: {e.response.status_code} - {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"Stedi submission failed: {e}", exc_info=True)
        raise

@lru_cache(maxsize=256)
def lookup_payer_name_by_internal(internal_name: str) -> str:
    """
    Look up official payer displayName from Stedi directory.

    Process:
    1. Get payer ID from claim_assumptions.resolve_payer_id()
    2. Search Stedi payer directory with internal name
    3. Loop through items[] and match item.payer.primaryPayerId
    4. Return that item.payer.displayName
    """
    if not internal_name:
        return internal_name

    api_key = os.getenv("STEDI_API_KEY")
    if not api_key:
        return internal_name

    # Step 1: Get payer ID from Brandon's assumptions file
    payer_id = ""
    try:
        from claim_assumptions import resolve_payer_id
        payer_id = resolve_payer_id(internal_name) or ""
        logger.info(f"Payer ID for '{internal_name}': '{payer_id}'")
    except Exception as e:
        logger.warning(f"resolve_payer_id failed for '{internal_name}': {e}")

    if not payer_id:
        logger.warning(f"No payer_id for '{internal_name}' — using internal name")
        return internal_name

    # Step 2: Search Stedi payer directory
    try:
        response = requests.get(
            "https://payers.us.stedi.com/2024-04-01/payers/search",
            params={"query": internal_name},
            headers={"Authorization": api_key},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        # Response structure: {"items": [{"payer": {...}, "score": ..., "matches": {...}}]}
        items = data.get("items", [])

        if not items:
            logger.warning(f"No results from Stedi payer search for '{internal_name}'")
            return internal_name

        # Step 3: Find exact match by primaryPayerId
        for item in items:
            payer = item.get("payer", {})
            if payer.get("primaryPayerId") == payer_id:
                display_name = payer.get("displayName", internal_name)
                logger.info(
                    f"Payer matched: '{internal_name}' "
                    f"(primaryPayerId={payer_id}) → '{display_name}'"
                )
                return display_name

        # No exact primaryPayerId match — log what was returned
        returned = [(i.get("payer", {}).get("primaryPayerId"), i.get("payer", {}).get("displayName")) for i in items[:5]]
        logger.warning(
            f"No primaryPayerId match for '{internal_name}' "
            f"(looking for '{payer_id}', top results: {returned})"
        )
        return internal_name

    except Exception as e:
        logger.error(f"Payer lookup failed for '{internal_name}': {e}")
        return internal_name


def _get_payer_id(internal_name: str) -> str:
    """Get Stedi payer ID from Brandon's claim_assumptions."""
    try:
        from claim_assumptions import resolve_payer_id
        return resolve_payer_id(internal_name) or ""
    except Exception:
        return ""


def get_277_acknowledgement(claim_id: str) -> dict:
    """
    277 is delivered asynchronously via Stedi webhook.
    Do not poll for it — just return pending status.
    The /stedi/277 webhook endpoint will handle it when it arrives.
    """
    logger.info(f"277 for claim_id={claim_id} — will arrive via webhook")
    return {
        "status": "Pending",
        "rejection_reason": "",
        "raw": {},
    }

def parse_inline_277_status(result: dict) -> str:
    """
    Parse 277 acknowledgement status from the x12 field in the submission response.
    Stedi includes the 277CA inline for some payers.
    STC*A1 = Accepted, STC*A2 = Rejected
    """
    x12 = result.get("x12", "")
    if not x12:
        return "Pending"

    if "STC*A1" in x12:
        return "Accepted"
    elif "STC*A2" in x12:
        return "Rejected"
    elif "STC*A0" in x12:
        return "Accepted"  # A0 = Accepted with errors but forwarded

    return "Pending"

def get_277_report(transaction_id: str) -> dict:
    """
    Fetch 277CA acknowledgement report by transaction ID.
    Called after Stedi sends transaction.processed.v2 event with transactionSetIdentifier=277
    """
    if is_mock_mode():
        logger.info(f"MOCK MODE: Returning mock 277 report for {transaction_id}")
        return {
            "transactions": [{
                "payers": [{
                    "claimStatusTransactions": [{
                        "claimStatusDetails": [{
                            "patientClaimStatusDetails": [{
                                "claims": [{
                                    "patientAccountNumber": "MOCK_PCN",
                                    "claimStatus": {
                                        "informationClaimStatuses": [{
                                            "informationStatuses": [{
                                                "healthCareClaimStatusCategoryCode": "A1",
                                                "statusCodeValue": "Accepted"
                                            }]
                                        }]
                                    }
                                }]
                            }]
                        }]
                    }]
                }]
            }]
        }

    logger.info(f"Fetching 277 report: transaction_id={transaction_id}")
    try:
        response = requests.get(
            f"{STEDI_BASE_URL}/change/medicalnetwork/reports/v2/{transaction_id}/277",
            headers=get_stedi_headers(),
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"277 report fetched: {result}")
        return result
    except Exception as e:
        logger.error(f"277 report fetch failed: {e}", exc_info=True)
        raise


def get_era_as_835_file(transaction_id: str) -> str:
    """
    Fetch 835 ERA report by transaction ID.
    Called after Stedi sends transaction.processed.v2 event with transactionSetIdentifier=835
    """
    if is_mock_mode():
        import json
        logger.info(f"MOCK MODE: Returning mock 835 ERA for {transaction_id}")
        return json.dumps({
            "transactions": [{
                "financialInformation": {"checkIssueOrEFTEffectiveDate": "20260315"},
                "paymentAndRemitReassociationDetails": {"checkOrEFTTraceNumber": "MOCK_CHK_001"},
                "detailInfo": [{
                    "paymentInfo": [{
                        "claimPaymentInfo": {
                            "patientControlNumber": "MOCK_PCN",
                            "claimStatusCode": "1",
                            "claimPaymentAmount": "450.00",
                            "patientResponsibilityAmount": "50.00",
                            "totalClaimChargeAmount": "500.00",
                            "payerClaimControlNumber": "MOCK_PAYER_001",
                        },
                        "serviceLines": [{
                            "servicePaymentInformation": {
                                "adjudicatedProcedureCode": "A4239",
                                "lineItemProviderPaymentAmount": "450.00",
                                "lineItemChargeAmount": "500.00",
                            },
                            "serviceSupplementalAmounts": {"allowedActual": "500.00"},
                            "serviceDate": "20260316",
                            "lineItemControlNumber": "MOCKLINE001",
                            "serviceAdjustments": [],
                            "healthCareCheckRemarkCodes": [],
                        }],
                        "patientName": {"firstName": "John", "lastName": "TestPatient"},
                    }]
                }]
            }]
        })

    logger.info(f"Fetching 835 ERA: transaction_id={transaction_id}")
    try:
        headers = get_stedi_headers()
        headers["Accept"] = "application/json"

        response = requests.get(
            f"{STEDI_BASE_URL}/reports/era/{transaction_id}",
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"835 fetch failed: {e}", exc_info=True)
        raise
