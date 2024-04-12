with payments as (
    select * from {{ source('stripe', 'payment') }}
)

select
    orderid,
    sum(amount) as total_amount
from payments
group by 1
having total_amount < 0