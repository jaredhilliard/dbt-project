{{
    config(
        materialized='view',
        bind = False
    )
}}

--had to make this table because redshift did not like applying partition over to a listagg function.

with orders as (
    select * from {{ ref('int_orders') }}
),
customer_orders_list as (
    select
        customer_id,
        listagg( distinct orders.order_id, ',' ) as customer_order_ids
    from orders
    group by customer_id

)
select * from customer_orders_list