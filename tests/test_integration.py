"""Integration tests — full mock flow through the FastAPI app."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Ensure mock mode by clearing env vars
os.environ.pop("MONDAY_API_TOKEN", None)
os.environ.pop("STEDI_API_KEY", None)

import pytest
from httpx import AsyncClient, ASGITransport
from main import app


@pytest.fixture
def transport():
    return ASGITransport(app=app)


@pytest.mark.asyncio
async def test_health(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_monday_challenge(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/monday/webhook", json={"challenge": "abc123"})
        assert r.status_code == 200
        assert r.json()["challenge"] == "abc123"


@pytest.mark.asyncio
async def test_monday_webhook_submit(transport):
    """Full mock flow: Monday webhook → build claim → mock submit."""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/monday/webhook", json={
            "event": {
                "pulseId": "99999",
                "value": {"label": {"text": "Submit Claim"}}
            }
        })
        assert r.status_code == 200
        assert r.json()["status"] == "received"


@pytest.mark.asyncio
async def test_monday_webhook_test_claim(transport):
    """Test mode: overrides payer to Stedi Test Payer."""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/monday/webhook", json={
            "event": {
                "pulseId": "99998",
                "value": {"label": {"text": "Test Claim Submitted"}}
            }
        })
        assert r.status_code == 200


@pytest.mark.asyncio
async def test_monday_webhook_ignored_status(transport):
    """Non-trigger statuses should be ignored."""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/monday/webhook", json={
            "event": {
                "pulseId": "99997",
                "value": {"label": {"text": "Working on it"}}
            }
        })
        assert r.status_code == 200


@pytest.mark.asyncio
async def test_submit_test_claim(transport):
    """Direct test claim to Stedi test payer (mock mode)."""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/submit-test-claim")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "submitted"
        assert data["pcn"]
        assert data["claim_id"].startswith("MOCK_")


@pytest.mark.asyncio
async def test_835_sample_parse(transport):
    """Test 835 ERA parsing endpoint."""
    era_body = {
        "claimPaymentInfo": {
            "patientControlNumber": "TEST_PCN",
            "claimStatusCode": "1",
            "claimPaymentAmount": "450.00",
            "patientResponsibilityAmount": "50.00",
            "totalClaimChargeAmount": "500.00",
        },
        "serviceLines": [{
            "servicePaymentInformation": {
                "adjudicatedProcedureCode": "A4239",
                "lineItemProviderPaymentAmount": "450.00",
                "lineItemChargeAmount": "500.00",
            },
            "serviceSupplementalAmounts": {"allowedActual": "500.00"},
            "serviceDate": "20260316",
            "lineItemControlNumber": "LINE001",
            "serviceAdjustments": [],
            "healthCareCheckRemarkCodes": [],
        }],
    }
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/test/835-sample", content=__import__("json").dumps(era_body))
        assert r.status_code == 200
        data = r.json()
        assert data["parsed_rows"] == 1
        assert data["results"][0]["children_count"] == 1


@pytest.mark.asyncio
async def test_stedi_277_webhook(transport):
    """Test 277 webhook processing (mock mode)."""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/stedi/webhook", json={
            "event": {
                "id": "evt_277",
                "detail-type": "transaction.processed.v2",
                "detail": {
                    "transactionId": "txn_277_test",
                    "x12": {"metadata": {"transaction": {"transactionSetIdentifier": "277"}}}
                }
            }
        })
        assert r.status_code == 200


@pytest.mark.asyncio
async def test_stedi_835_webhook(transport):
    """Test 835 webhook processing (mock mode)."""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/stedi/webhook", json={
            "event": {
                "id": "evt_835",
                "detail-type": "transaction.processed.v2",
                "detail": {
                    "transactionId": "txn_835_test",
                    "x12": {"metadata": {"transaction": {"transactionSetIdentifier": "835"}}}
                }
            }
        })
        assert r.status_code == 200


@pytest.mark.asyncio
async def test_stedi_unknown_event_ignored(transport):
    """Non-transaction events should be ignored."""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/stedi/webhook", json={
            "event": {
                "id": "evt_unknown",
                "detail-type": "some.other.event",
                "detail": {}
            }
        })
        assert r.status_code == 200


@pytest.mark.asyncio
async def test_era_test_endpoint(transport):
    """Test the /test/era endpoint."""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/test/era", json={
            "claimPaymentInfo": {
                "patientControlNumber": "ERA_TEST",
                "claimStatusCode": "1",
                "claimPaymentAmount": "100.00",
                "patientResponsibilityAmount": "0",
                "totalClaimChargeAmount": "100.00",
            },
            "patientName": {"firstName": "John", "lastName": "Test"},
            "serviceLines": [],
        })
        assert r.status_code == 200
        data = r.json()
        assert data["parent"]["raw_patient_control_num"] == "ERA_TEST"
