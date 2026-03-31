# Claims Board Migration Guide

## Overview

This migration moves claim submission from the **Order Board** (with subitems) to a **Claims Board** (parent + 5 pre-populated product subitems). The new flow also introduces a **New Order Board** that is flat (no subitems) with product quantities as parent-level columns.

## Architecture

```
NEW FLOW:
  New Order Board (flat)
    → POST /order-to-claims/migrate
    → Claims Board created with 5 pre-computed product subitems
    → Human reviews/edits HCPC codes, units, modifiers, charges
    → Status → "Submit Claim"
    → Monday webhook reads Claims Board (pre-computed values)
    → POST to Stedi API
    → 277CA webhook → updates Claims Board parent
    → 835 ERA webhook → UPDATES existing subitems (HCPC match)

LEGACY FLOW (still supported):
  Order Board (with subitems)
    → Status → "Submit Claim"
    → Monday webhook reads Order Board subitems
    → Computes HCPC/units/modifiers/charges from scratch
    → POST to Stedi API
    → 277CA → updates Order Board
    → 835 ERA → CREATES new subitems on Claims Board
```

## Environment Variables

Add these to your `.env`:

```
MONDAY_NEW_ORDER_BOARD_ID=18403054769
SUBMISSION_SOURCE=claims_board    # or "order_board" for legacy
```

## Files Changed

| File | Change |
|------|--------|
| `claims_board_config.py` | **NEW** — Centralized config for all column IDs, product categories, routing |
| `routes/order_to_claims.py` | **NEW** — New Order Board → Claims Board migration endpoint |
| `claim_infrastructure.py` | **MODIFIED** — `build_service_line_from_normalized_order` now checks for pre-computed values |
| `services/claim_builder_service.py` | **MODIFIED** — Added `claims_board_item_to_normalized_orders()` and `build_claims_from_claims_board_item()` |
| `services/monday_service.py` | **MODIFIED** — Added `get_new_order_item()`, `get_claims_board_item()`, `create_claims_board_parent()`, `populate_claims_board_subitems()`, `update_claims_board_277()`, `update_claims_board_workflow()`, `update_existing_claims_subitems()` |
| `routes/monday_webhook.py` | **MODIFIED** — Dual routing via `SUBMISSION_SOURCE` env var |
| `routes/stedi_webhook.py` | **MODIFIED** — 277 writes to Claims Board in new mode; 835 UPDATES existing subitems |
| `main.py` | **MODIFIED** — Registered `order_to_claims` router |
| `.env.example` | **MODIFIED** — Added `MONDAY_NEW_ORDER_BOARD_ID` and `SUBMISSION_SOURCE` |
| `tests/test_migration.py` | **NEW** — 44 comprehensive tests |

## Placeholder Column IDs

Column IDs prefixed with `cb_` or `nob_` are **placeholders** that need to be replaced with real Monday column IDs after board setup. To find real IDs:

1. Use the debug endpoint: `GET /test/order-board-columns`
2. Or check the Monday board URL and use the API explorer

Key placeholders to replace:
- `cb_277_status` — Claims Board 277 status column
- `cb_277_reason` — Claims Board 277 rejection reason
- `cb_claim_status` — Claims Board workflow status
- `cb_gender`, `cb_diagnosis_code`, `cb_patient_address` — Claims Board parent fields
- `cb_sub_hcpc_code`, `cb_sub_claim_qty`, etc. — Claims Board subitem fields
- All `nob_*` columns — New Order Board column IDs

## Potential Integration Issues

### 1. Column ID Mismatches
**Risk:** Real Monday column IDs may not match placeholders.
**Mitigation:** Use `/test/order-board-columns` and `/test/claims-subitem-columns` endpoints to verify. Replace all `cb_*` and `nob_*` prefixed IDs before going live.

### 2. Status Label Index Mismatches
**Risk:** `CLAIMS_BOARD_STATUS_TO_INDEX` and `CLAIMS_BOARD_277_STATUS_TO_INDEX` may not match the actual Monday board status column configuration.
**Mitigation:** Use `/test/order-status-settings` endpoint to verify label indexes match.

### 3. ERA Subitem Matching Failures
**Risk:** When 835 ERA data comes back, HCPC codes in the ERA may not exactly match subitem names (e.g., "A4239" vs "CGM Sensors").
**Mitigation:** The `update_existing_claims_subitems()` function uses a 3-tier matching strategy:
  1. Match ERA HCPC code to subitem name
  2. Match via `HCPC_TO_PRODUCT` lookup (HCPC → product name → subitem)
  3. Positional fallback

### 4. Payer Name Not on Claims Board Parent
**Risk:** The Claims Board item name format "Patient - Payer" is the only source of payer name. If the format changes, claim building fails.
**Mitigation:** The code handles names without " - " gracefully. Consider adding a dedicated payer column to Claims Board.

### 5. Missing Patient Data on Claims Board
**Risk:** The Claims Board parent may not have all fields needed for Stedi (gender, address, diagnosis code). If these columns aren't populated, claims will fail validation.
**Mitigation:** Ensure the `/order-to-claims/migrate` endpoint copies ALL required fields. Placeholder columns `cb_gender`, `cb_patient_address`, `cb_diagnosis_code` must be created and mapped.

### 6. Infusion Set 2 Logic
**Risk:** Some payers require a separate Infusion Set 2 line item. Current implementation has a placeholder `needs_infusion_set_2()` function.
**Mitigation:** Implement payer-specific rules before going live with affected payers.

### 7. Race Conditions on PCN Storage
**Risk:** Multiple concurrent claim submissions for the same patient could race on PCN column updates.
**Mitigation:** The existing `_pcn_lock` threading lock handles this. Claims Board mode stores PCN on the Claims Board item (one item per claim = no race).

### 8. Stedi Webhook Timing
**Risk:** 277 or 835 webhook may fire before the Claims Board item is fully populated.
**Mitigation:** Webhook handlers search by both correlation ID and PCN. Background tasks handle the processing asynchronously.

### 9. Mock Mode Behavior
**Risk:** Mock mode returns hardcoded data that may not reflect real board structure.
**Mitigation:** Tests use mock mode to validate logic flow. Integration testing with real API keys is required before production use.

### 10. Monday API Rate Limits
**Risk:** Creating a Claims Board parent + 5 subitems + writing multiple columns = many API calls per migration.
**Mitigation:** Consider batching column updates using Monday's `change_multiple_column_values` mutation instead of individual `change_column_value` calls.

## Testing

Run all tests:
```bash
python -m pytest tests/ -v
```

Run only migration tests:
```bash
python -m pytest tests/test_migration.py -v
```

## Deployment Steps

1. Replace all placeholder column IDs (`cb_*`, `nob_*`) with real Monday column IDs
2. Set `MONDAY_NEW_ORDER_BOARD_ID` in Railway environment
3. Set `SUBMISSION_SOURCE=claims_board` when ready to switch
4. Deploy to Railway
5. Test with `POST /order-to-claims/preview` first
6. Test with `POST /order-to-claims/migrate` on a single order
7. Verify Claims Board item and subitems look correct
8. Test full submit flow with "Test Claim Submitted" status
9. Monitor Railway logs for 277 and 835 webhook processing
