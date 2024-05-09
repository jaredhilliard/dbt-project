{{
    config(
        materialized='table'
    )
}}

-- this is a refactoring of /models/legacy/customer_orders.sql
-- first added line breaks around logical blocks and horizontal line breaks for long lines
-- transformed everything to lowercase
-- then cte groupings:
--  import ctes
--  logical ctes
--  final ctes

--import ctes
--  jaffleshop orders, customers
--  stripe payments
--      import and logical ctes combined as staging tables, now only referenced.
with customers as (

    select * from {{ ref('stg_jaffle_shop_customers') }}

),
--orders
orders as (

    select * from {{ ref('int_orders') }}

),
orders_list as (

    select * from {{ ref('int_customer_orders_list') }}

),
--
customer_orders as (
    
    select

        orders.*,
        customers.full_name,
        customers.surname,
        customers.givenname,

        --Customer Level Aggregations
        min(orders.order_date) over(
            partition by orders.customer_id
        ) as customer_first_order_date,

        min(valid_order_date) over(
            partition by orders.customer_id
        ) as customer_first_non_returned_order_date,

        max(valid_order_date) over(
            partition by orders.customer_id
        ) as customer_most_recent_non_returned_order_date,

        count(*) over(
            partition by orders.customer_id
        ) as customer_order_count,

        sum(NVL2(orders.valid_order_date,1,0))  over(
            partition by orders.customer_id
        ) as customer_non_returned_order_count,

        sum(NVL2(orders.valid_order_date,order_value_dollars,0)) over(
            partition by orders.customer_id
        ) as customer_total_lifetime_value,

        -- -- -- array_agg(distinct orders.order_id) as order_ids    -- snowflake acceptable version, doesn't work in redshift
        orders_list.customer_order_ids --redshift version  --why is this logic even here?

    from orders
    inner join customers
        on orders.customer_id = customers.customer_id
    inner join orders_list
        on orders.customer_id = orders_list.customer_id
    -- group by customers.customer_id, customers.full_name, customers.surname, customers.givenname

),
add_avg_order_values as (
    select
    
        *,
        customer_total_lifetime_value / customer_non_returned_order_count as avg_non_returned_order_value
    
    from customer_orders
),
-- final ctes

final as (
select

    order_id,
    customer_id,
    surname,
    givenname,
    customer_first_order_date as first_order_date,
    customer_order_count as order_count,
    customer_total_lifetime_value as total_lifetime_value,
    order_value_dollars,
    order_status,
    payment_status

from add_avg_order_values

)

-- Simple select statement
select * from final