{{
    config(
        materialized='table',
        pre_hook=[" 
            begin;
            lock table {{target.schema}}.{{this.identifier}};
        "],
        post_hook=["
            commit;
            grant usage on schema {{target.schema}} to group bi_users
            grant select on table {{target.schema}}.{{this.identifier}} to group bi_users;
        "
        ]
    )
}}



select * from {{ref('joined_order_data')}}