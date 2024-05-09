-- this is a slightly different staging payments table from the refactoring cours
with source as (

    select * from {{ source('stripe', 'payment') }}

),

transformed as (

    select 
    
        id as payment_id,
        orderid as order_id,
        status as payment_status,
        round(amount/100.0, 2) as payment_amount
    
    from source

)

select * from transformed