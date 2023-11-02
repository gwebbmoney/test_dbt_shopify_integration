with
    order_lines as (
        select ol.*, sk.skuable_type
        from {{ ref("stg_infotrax__order_lines") }} ol
        left join {{ source("redaspen", "SKUS") }} sk on ol.infotrax_sku = sk.name
        where infotrax_sku <> 'Discount' and infotrax_sku <> 'HOSTCREDIT'
    ),
    order_lines_cond as (
        select ol.*, o.infotrax_original_order
        from order_lines ol
        join
            {{ ref("stg_infotrax__orders") }} o
            on ol.infotrax_order_number = o.infotrax_order_number
        where o.order_source = 904
    ),
    bundle_order_lines as (
        select * from {{ ref("stg_infotrax__bundle_refund_order_lines") }}
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
    ),
    product_bundle_array as (
        select distinct
            (bol.order_line_id),
            bol.infotrax_order_number,
            bol.infotrax_original_order,
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
                    'order_line_id',
                    bl.order_line_id,
                    'infotrax_order_number',
                    bl.infotrax_original_order
                )
            ) as properties
        from bundle_lines bl
        join
            bundle_order_lines bol
            on bl.infotrax_order_number = bol.infotrax_order_number
            and bl.bundle_product_number = bol.bundle_product_number
            and bl.promo_id = bol.promo_id
            and abs(bol.order_line - bl.order_line) = bol.kit_line
            and bol.component_status not in ('M', 'P')
        order by bol.order_line_id
    ),
    bundle_properties as (
        select bol.*, pba.properties
        from product_bundle_array pba
        join
            bundle_order_lines bol
            on pba.order_line_id = bol.order_line_id
            and bol.infotrax_order_number = pba.infotrax_order_number
            and bol.order_line = pba.product_order_line
    ),
    product_order_line as (
        select
            olc.*,
            bp.emma_price_cents,
            bp.bundle_product_allocation_revenue_cents,
            bp.properties
        from order_lines_cond olc
        left join
            bundle_properties bp
            on olc.id = bp.order_line_id
            and olc.infotrax_order_number = bp.infotrax_order_number
        where olc.component_status not in ('M', 'P')
    )
select
    id as order_line_id,
    infotrax_original_order as order_id,
    product_id,
    product_name,
    infotrax_sku as sku,
    order_line as refund_order_line,
    properties,
    (
        case
            when properties is not null and kit_line > 0
            then emma_price_cents
            else retail_amount_cents
        end
    ) as refund_price_cents,
    quantity_ordered as refund_quantity,
    quantity_returned as quantity_returned,
    (refund_price_cents * refund_quantity) as line_item_price_cents,
    (
        case
            when kit_line > 0
            then bundle_product_allocation_revenue_cents
            else line_item_price_cents
        end
    ) as pre_tax_refund_cents
from product_order_line
order by infotrax_order_number, order_line
