with payments as (

    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        -- amount / 100 as amount,
        {{ cents_to_dollars('amount', 4) }} as amount, 
        --doesn't seem like you saved any time or anything in this case...
        --ah, once you start adding rounding logic, it has become large enough to be worth macroing maybe
        -- and to make me feel sane, they also questioned whether this was worth the sacrifice to readability.
        created as created_at

    from {{ source('stripe', 'payment') }}

)

select * from payments