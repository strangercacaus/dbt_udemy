select {{return_fields()}} from {{ref('joined_order_data')}}
where category_name = '{{var('category')}}'