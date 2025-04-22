{% snapshot invoices_snapshot %}
    {{
        config(
          strategy='check',
          unique_key='invoice_no || product_service',
          check_cols=['status'],
        )
    }}
    
    SELECT * FROM {{ ref('stg_invoices') }}
    
{% endsnapshot %}
