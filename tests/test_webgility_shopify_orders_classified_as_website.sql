-- Fails if non-Amazon Webgility-imported Shopify orders regress into the Invoice channel.

select
    order_number,
    source_type,
    payment_method,
    terms,
    sales_channel
from {{ ref('fct_orders') }}
where source_type = 'invoice'
  and order_number like 'S-%'
  and terms = 'Credit Card'
  and coalesce(class, '') not like '%Amazon%'
  and sales_channel <> 'Website'
