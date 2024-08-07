-- Creates view that houses infotrax order lines and transforms this data in order to combine to Shopify
with
    order_lines as (
        select ol.*, sk.skuable_type
        from {{ ref("stg_infotrax__order_lines") }} ol
        left join {{ source("redaspen", "SKUS") }} sk on ol.infotrax_sku = sk.name
        where infotrax_sku <> 'Discount' and infotrax_sku <> 'HOSTCREDIT'
-- Grabs order lines that do not contain 'Discount' or 'HOSTCREDIT' rows
    ),
    order_lines_cond as (
        select ol.*, o.infotrax_original_order
        from order_lines ol
        join
            {{ ref("stg_infotrax__orders") }} o
            on ol.infotrax_order_number = o.infotrax_order_number
        where o.order_source = 904
-- Grabs all order lines that were not within a refunded order
    ),
    bundle_order_lines as (
        select * from {{ ref("stg_infotrax__bundle_refund_order_lines") }}
-- Grabs data from bundle order lines
-- Used to help match the Shopify format on how they calculate/display bundle information
    ),
    bundle_lines as (
        select
            infotrax_order_number,
            infotrax_original_order,
            order_line_id,
            bundle_product_number,
            product_name,
            retail_amount_cents,
            quantity_returned,
            quantity_ordered,
            line_item_price_cents,
            order_line,
            promo_id,
            component_status
        from bundle_order_lines
        where kit_line = 0
-- Grab all bundles within the bundle order lines table
-- Component status = M and P means is an Infotrax field. Basically states if the product shown was a bundle or not.
    ),
    product_bundle_array as (
        select distinct(bol.order_line_id),
            bol.infotrax_order_number,
            bol.order_line as product_order_line,
            bl.order_line as bundle_order_line,
            array_construct(
                object_construct(
                    'set',
                    bl.bundle_product_number,
                    'set_name',
                    bl.product_name,
                    'price',
                    bl.retail_amount_cents,
                    'quantity',
                    bl.quantity_returned,
                    'total_amount',
                    bl.line_item_price_cents,
                    'bundle_order_line_id',
                    bl.order_line_id,
                    'infotrax_order_number',
                    bl.infotrax_original_order,
                    'product_order_line_id', bol.order_line_id
                )
            ) as bundle_properties
        from bundle_lines bl
        join
            bundle_order_lines bol
            on bl.infotrax_order_number = bol.infotrax_order_number
            and bl.bundle_product_number = bol.bundle_product_number
            and bl.promo_id = bol.promo_id
            where abs(bol.order_line - bl.order_line) = bol.kit_line
            and bol.component_status not in ('M', 'P')
            and bol.order_line > bl.order_line
        order by bol.order_line_id
-- Grabs all products that were within a bundle
-- Creates array object to house all of the bundle information
    ),
    bundle_properties as (
        select bol.*, pba.bundle_properties
        from product_bundle_array pba
        join
            bundle_order_lines bol
            on pba.order_line_id = bol.order_line_id
            and bol.infotrax_order_number = pba.infotrax_order_number
            and bol.order_line = pba.product_order_line
-- Combines product bundle array with bundle order lines
    ),
    product_order_line as (
        select
            olc.*,
            bp.emma_price_cents,
            bp.bundle_product_allocation_revenue_cents,
            bp.bundle_properties
        from order_lines_cond olc
        left join
            bundle_properties bp
            on olc.id = bp.order_line_id
            and olc.infotrax_order_number = bp.infotrax_order_number
        where olc.component_status not in ('M', 'P')
-- Grabs the product order line of a product
    )
select
    id as order_line_id,
    infotrax_original_order as order_id,
    product_id,
    product_name,
    infotrax_sku as sku,
    order_line as refund_order_line,
    bundle_properties,
    (
        case
            when bundle_properties is not null and kit_line > 0
            then emma_price_cents
            else retail_amount_cents
        end
    ) as refund_price_cents,
    quantity_ordered as refund_quantity,
    quantity_returned as quantity_returned,
    CAST((refund_price_cents * refund_quantity) AS NUMBER) as line_item_price_cents,
    (
        case
            when kit_line > 0
            then bundle_product_allocation_revenue_cents
            else line_item_price_cents
        end
    ) as pre_tax_refund_cents
from product_order_line
order by infotrax_order_number, order_line
-- Organizes view into it's final format