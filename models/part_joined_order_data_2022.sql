select * from {{ref('joined_order_data')}}
where date_part(year,order_date) = 2022