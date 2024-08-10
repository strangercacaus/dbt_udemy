-- customer model
with
    original as (
        select
            first_value(customer_id) over (
                partition by company_name, contact_name
            ) as result
        from{{ source("sources", "customers") }}
    ),
    deduped as (select distinct result from original),
    final as (
        select *
        from {{ source("sources", "customers") }} c
        where exists (select 1 from deduped d where d.result = customer_id)
    )
select * from final