"""
routes/order_to_claims.py
=========================
New Order Board → Claims Board migration endpoint.

When a new order arrives on the New Order Board, this route:
1. Reads the flat order (no subitems — quantities are parent columns)
2. Computes HCPC codes, units, modifiers, and estimated pay for each product
3. Creates a Claims Board parent item
4. Populates 5 pre-created product subitems with computed values

This replaces the old "create Claims Board item after Stedi submission" flow.
Now the Claims Board item is created BEFORE submission, with all values
pre-computed so humans can review/edit before the claim is sent.

Endpoint: POST /order-to-claims/migrate
"""

import logging
import os
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from claims_board_config import (
    get_new_order_board_id,
    get_claims_board_id,
    NEW_ORDER_BOARD_COLUMN_MAP,
    PRODUCT_CATEGORIES,
)
from claim_infrastructure import (
    safe_str,
    normalize_date,
    normalize_gender,
    split_full_name,
    parse_address,
    build_normalized_order_template,
)
from claim_assumptions import (
    resolve_payer_name,
    resolve_payer_id,
    resolve_procedure_code,
    resolve_service_unit_count,
    resolve_procedure_modifiers,
    resolve_line_item_charge_amount,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Order to Claims"])


# ============================================================
# NEW ORDER BOARD DATA EXTRACTION
# ============================================================

def extract_new_order_columns(column_values: list) -> dict:
    """Convert New Order Board column_values into a semantic dict."""
    result = {}
    for col in column_values:
        col_id = col.get("id", "")
        field_name = NEW_ORDER_BOARD_COLUMN_MAP.get(col_id, col_id)
        result[field_name] = col.get("text", "") or ""
    return result


def new_order_to_normalized(monday_item: dict) -> dict:
    """
    Convert a New Order Board item (flat, no subitems) into a
    normalized order dict suitable for the claim builder pipeline.
    """
    cols = extract_new_order_columns(monday_item.get("column_values", []))

    patient_full_name = monday_item.get("name", "")
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
    normalized["patient_phone"]       = cols.get("phone", "")
    normalized["patient_address_1"]   = patient_addr.get("address1", "")
    normalized["patient_address_2"]   = patient_addr.get("address2", "")
    normalized["patient_city"]        = patient_addr.get("city", "")
    normalized["patient_state"]       = patient_addr.get("state", "")
    normalized["patient_postal_code"] = patient_addr.get("postal_code", "")

    # Insurance
    normalized["primary_insurance_name"] = cols.get("primary_insurance", "")
    normalized["member_id"]              = cols.get("member_id", "")
    normalized["secondary_member_id"]    = cols.get("secondary_id", "")
    normalized["subscription_type"]      = cols.get("subscription_type", "")
    normalized["diagnosis_code"]         = cols.get("diagnosis_code", "")
    normalized["cgm_coverage"]           = cols.get("cgm_coverage", "")

    # Doctor
    normalized["doctor_name"]         = doctor_full_name
    normalized["doctor_first_name"]   = doctor_first
    normalized["doctor_last_name"]    = doctor_last
    normalized["doctor_npi"]          = cols.get("doctor_npi", "")
    normalized["doctor_address_1"]    = doctor_addr.get("address1", "")
    normalized["doctor_address_2"]    = doctor_addr.get("address2", "")
    normalized["doctor_city"]         = doctor_addr.get("city", "")
    normalized["doctor_state"]        = doctor_addr.get("state", "")
    normalized["doctor_postal_code"]  = doctor_addr.get("postal_code", "")
    normalized["doctor_phone"]        = cols.get("doctor_phone", "")

    # Order metadata
    normalized["order_date"]    = normalize_date(cols.get("order_date", ""))
    normalized["service_date"]  = normalize_date(cols.get("order_date", ""))
    normalized["auth_id"]       = cols.get("auth_id", "")

    return normalized, cols


# ============================================================
# COMPUTE CLAIMS BOARD SUBITEM DATA
# ============================================================

def compute_product_subitem_data(
    normalized_order: dict,
    order_cols: dict,
    product_category: dict,
) -> Optional[dict]:
    """
    Compute HCPC code, units, modifiers, charge amount, and est. pay
    for a single product category subitem.

    Returns None if the product quantity is 0 or missing.
    """
    qty_field = product_category["qty_field"]
    quantity_str = order_cols.get(qty_field, "")
    quantity = 0
    try:
        quantity = int(float(quantity_str)) if quantity_str else 0
    except (ValueError, TypeError):
        pass

    if quantity <= 0:
        return None

    payer_name = resolve_payer_name(normalized_order)

    # Determine item name for resolver functions
    item_name = product_category["item_names"][0]

    # Get variant if applicable
    variant_field = product_category.get("variant_field")
    variant = order_cols.get(variant_field, "") if variant_field else ""

    # Compute HCPC code
    if product_category["fixed_hcpc"]:
        hcpc_code = product_category["fixed_hcpc"]
    else:
        hcpc_code = resolve_procedure_code(payer_name, item_name)

    # Compute units
    service_unit_count = resolve_service_unit_count(
        payer_name=payer_name,
        item_name=item_name,
        variant=variant,
        quantity=str(quantity),
        procedure_code=hcpc_code,
    )

    # Compute modifiers
    cgm_coverage = normalized_order.get("cgm_coverage", "")
    modifiers = resolve_procedure_modifiers(
        payer_name=payer_name,
        procedure_code=hcpc_code,
        cgm_coverage=cgm_coverage,
    )

    # Compute charge amount
    charge_amount = resolve_line_item_charge_amount(
        payer_name=payer_name,
        procedure_code=hcpc_code,
        service_unit_count=service_unit_count,
    )

    return {
        "product_name": product_category["name"],
        "hcpc_code": hcpc_code,
        "claim_qty": str(quantity),
        "units": str(service_unit_count),
        "modifiers": modifiers,
        "charge_amount": str(charge_amount),
        "est_pay": str(charge_amount),  # Initially same as charge; human can edit
        "variant": variant,
    }


def compute_all_product_subitems(
    normalized_order: dict,
    order_cols: dict,
) -> list[dict]:
    """
    Compute claim data for all 5 product categories.
    Returns list of subitem data dicts (only for products with qty > 0).
    """
    subitems = []
    for category in PRODUCT_CATEGORIES:
        data = compute_product_subitem_data(normalized_order, order_cols, category)
        if data:
            subitems.append(data)
            logger.info(
                f"  Product: {data['product_name']} | "
                f"HCPC={data['hcpc_code']} | "
                f"qty={data['claim_qty']} | "
                f"units={data['units']} | "
                f"charge={data['charge_amount']}"
            )
    return subitems


# ============================================================
# ROUTE ENDPOINTS
# ============================================================

@router.post("/migrate")
async def migrate_order_to_claims(request: Request):
    """
    Migrate a New Order Board item to the Claims Board.

    Request body:
    {
        "item_id": "12345"  // New Order Board item ID
    }

    Flow:
    1. Fetch item from New Order Board
    2. Compute HCPC codes, units, modifiers, charge amounts
    3. Create Claims Board parent
    4. Populate 5 product subitems
    """
    body = await request.json()
    item_id = str(body.get("item_id", ""))

    if not item_id:
        return JSONResponse({"error": "item_id required"}, status_code=400)

    logger.info(f"[Migrate] Starting migration for item {item_id}")

    try:
        # Step 1: Fetch from New Order Board
        from services.monday_service import get_new_order_item
        order_item = get_new_order_item(item_id)

        # Step 2: Normalize and compute
        normalized, order_cols = new_order_to_normalized(order_item)
        product_subitems = compute_all_product_subitems(normalized, order_cols)

        if not product_subitems:
            return JSONResponse({
                "error": "No products with qty > 0",
                "item_id": item_id,
            }, status_code=400)

        # Step 3: Create Claims Board parent
        from services.monday_service import (
            create_claims_board_parent,
            populate_claims_board_subitems,
        )

        payer_name = resolve_payer_name(normalized)
        patient_name = normalized.get("patient_full_name", "Unknown")

        claims_item_id = create_claims_board_parent(
            patient_name=patient_name,
            payer_name=payer_name,
            normalized_order=normalized,
        )

        if not claims_item_id:
            return JSONResponse({
                "error": "Failed to create Claims Board parent",
            }, status_code=500)

        # Step 4: Populate subitems
        populate_claims_board_subitems(claims_item_id, product_subitems)

        # Calculate total charge
        total_charge = sum(
            float(s.get("charge_amount", 0))
            for s in product_subitems
        )

        logger.info(
            f"[Migrate] Done: Claims item {claims_item_id} | "
            f"{len(product_subitems)} products | "
            f"total_charge=${total_charge:.2f}"
        )

        return JSONResponse({
            "status": "migrated",
            "claims_item_id": claims_item_id,
            "patient": patient_name,
            "payer": payer_name,
            "products": [
                {
                    "name": s["product_name"],
                    "hcpc": s["hcpc_code"],
                    "qty": s["claim_qty"],
                    "charge": s["charge_amount"],
                }
                for s in product_subitems
            ],
            "total_charge": f"{total_charge:.2f}",
        })

    except Exception as e:
        logger.error(f"[Migrate] Failed: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/preview")
async def preview_migration(request: Request):
    """
    Preview what would be computed for a New Order Board item
    WITHOUT writing to the Claims Board.
    Useful for testing and validation.
    """
    body = await request.json()
    item_id = str(body.get("item_id", ""))

    if not item_id:
        return JSONResponse({"error": "item_id required"}, status_code=400)

    try:
        from services.monday_service import get_new_order_item
        order_item = get_new_order_item(item_id)

        normalized, order_cols = new_order_to_normalized(order_item)
        product_subitems = compute_all_product_subitems(normalized, order_cols)

        payer_name = resolve_payer_name(normalized)
        total_charge = sum(
            float(s.get("charge_amount", 0))
            for s in product_subitems
        )

        return JSONResponse({
            "status": "preview",
            "patient": normalized.get("patient_full_name", ""),
            "payer": payer_name,
            "payer_id": resolve_payer_id(payer_name),
            "member_id": normalized.get("member_id", ""),
            "diagnosis_code": normalized.get("diagnosis_code", ""),
            "products": product_subitems,
            "total_charge": f"{total_charge:.2f}",
        })

    except Exception as e:
        logger.error(f"[Preview] Failed: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)
