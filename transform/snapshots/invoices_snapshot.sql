{% snapshot invoices_snapshot %}
    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='invoice_no || product_service',
          check_cols=['status'],
        )
    }}
    
    SELECT * FROM {{ ref('stg_invoices') }}
    
{% endsnapshot %}
