--redshift doesn't like views
--Redshift adapter: Redshift error: External tables are not supported in views
--use __config and materialize as a table
--
-- can use config(materialized='view', bind=False)
-- without binding, it generates when queried but doesn't have to be attached to the external tables.
-- a bound view will object if dependencies are being dropped, but a late-binding view will not.
--
-- but now, as best practices, we'll materialize it within the project yml

with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('fct_orders') }}
),
employees as (
    select * from {{ ref('employees') }}
),
customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as lifetime_value
    from orders
    group by 1

),
final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        employees.employee_id is not null as is_employee,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        customer_orders.lifetime_value

    from customers

    left join customer_orders using (customer_id)

    left join employees using (customer_id)

)

select * from final