--TODO: could left join against date spine over a particular window
-- taking out holidays and non-business days (seed?)
-- to actually compare one _business_ day to the previous _business_ day
with orders as (
    select * from {{ ref('stg_orders') }}
),
daily as (
    select
        order_date,
        count(*) as order_cnt,
    {% for order_status in ['returned','completed', 'return_pending', 'shipped', 'placed'] %}
        sum(case when status = '{{ order_status }}' then 1 else 0 end) as {{ order_status }}_total{{ ',' if not loop.last}}
    {% endfor %}
        
    from orders
    group by 1
),
compared as (
    select
        *,
        lag(order_cnt) over (order by order_date) as previous_days_orders
    from daily
)
select * from compared