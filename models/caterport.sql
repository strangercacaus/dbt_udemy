{{
    config(materialized='incremental',
    unique_keyu='category_id')
}}


select * from {{source('sources','categories')}}