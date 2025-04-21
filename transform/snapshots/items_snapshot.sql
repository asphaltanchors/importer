{% snapshot items_snapshot %}
    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='item_name',
          check_cols=['quantity_on_hand', 'purchase_cost', 'sales_price', 'status', 'sales_description'],
        )
    }}
    
    SELECT * FROM {{ ref('stg_items') }}
    
{% endsnapshot %}
