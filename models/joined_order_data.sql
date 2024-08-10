--model employees
with
    products as (
        select
            prd.product_id,
            prd.product_name,
            prd.unit_price,
            cat.category_name,
            sup.company_name as supplier_name
        from {{ source("sources", "products") }} prd
        left join
            {{ source("sources", "suppliers") }} sup
            on prd.supplier_id = sup.supplier_id
        left join
            {{ source("sources", "categories") }} cat
            on prd.category_id = cat.category_id
    ),
    order_details as (
        select prd.*, od.order_id, od.quantity, od.discount
        from products prd
        left join {{ ref("order_details") }} od on prd.product_id = od.product_id
    ),
    orders as (
        select
            ord.order_date,
            ord.order_id,
            cus.company_name as customer_name,
            emp.full_name as employee_name,
            emp.employee_age,
            emp.service_time
        from {{ source("sources", "orders") }} ord
        left join {{ ref("customers") }} cus on (ord.customer_id = cus.customer_id)
        left join {{ ref("employees") }} emp on (ord.employee_id = emp.employee_id)
        left join
            {{ source("sources", "shippers") }} shp on (ord.ship_via = shp.shipper_id)
    ),
    result as (
        select
            odt.*,
            ord.order_date,
            ord.customer_name,
            ord.employee_name,
            ord.employee_age,
            ord.service_time
        from order_details odt
        join orders ord on (odt.order_id = ord.order_id)
    )
select *
from result
