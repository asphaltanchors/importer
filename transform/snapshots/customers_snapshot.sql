{% snapshot customers_snapshot %}
    {{
        config(
          strategy='check',
          unique_key='quickbooks_id',
          check_cols=['customer_name', 'company_name', 'first_name', 'last_name', 'customer_type', 'email', 'status', 'current_balance'],
        )
    }}
    
    SELECT * FROM {{ ref('stg_customers') }}
    
{% endsnapshot %}
