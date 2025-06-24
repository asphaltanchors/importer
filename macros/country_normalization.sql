/*
ABOUTME: DBT macro for country normalization logic based on state/province codes and explicit country values
ABOUTME: Provides reusable country inference and standardization across all models
*/

{% macro normalize_country(billing_country, billing_state, shipping_country, shipping_state) %}
    -- Primary country (billing takes precedence, shipping as fallback)
    COALESCE(
        {{ _infer_country_from_address(billing_country, billing_state) }},
        {{ _infer_country_from_address(shipping_country, shipping_state) }},
        'United States'  -- Final fallback
    )
{% endmacro %}

{% macro normalize_billing_country(country_field, state_field) %}
    {{ _infer_country_from_address(country_field, state_field) }}
{% endmacro %}

{% macro normalize_shipping_country(country_field, state_field) %}
    {{ _infer_country_from_address(country_field, state_field) }}
{% endmacro %}

{% macro country_category(billing_country, billing_state, shipping_country, shipping_state) %}
    CASE 
        WHEN {{ normalize_country(billing_country, billing_state, shipping_country, shipping_state) }} = 'United States' THEN 'United States'
        WHEN {{ normalize_country(billing_country, billing_state, shipping_country, shipping_state) }} = 'Canada' THEN 'Canada'
        ELSE 'International'
    END
{% endmacro %}

{% macro region(billing_country, billing_state, shipping_country, shipping_state) %}
    CASE 
        WHEN {{ normalize_country(billing_country, billing_state, shipping_country, shipping_state) }} IN ('United States', 'Canada') THEN 'North America'
        ELSE 'International'
    END
{% endmacro %}

-- Private helper macro for individual address country inference
{% macro _infer_country_from_address(country_field, state_field) %}
    CASE 
        -- Use explicit country if provided and not empty
        WHEN NULLIF(TRIM({{ country_field }}), '') IS NOT NULL 
            THEN CASE
                WHEN UPPER(TRIM({{ country_field }})) IN ('USA', 'US', 'UNITED STATES') THEN 'United States'
                WHEN UPPER(TRIM({{ country_field }})) IN ('CANADA', 'CA') THEN 'Canada'
                WHEN UPPER(TRIM({{ country_field }})) = 'UK' THEN 'United Kingdom'
                ELSE TRIM({{ country_field }})
            END
        
        -- Infer from state/province if country is empty
        WHEN UPPER(TRIM({{ state_field }})) IN ({{ _us_states() }}) 
            THEN 'United States'
        WHEN UPPER(TRIM({{ state_field }})) IN ({{ _canadian_provinces() }}) 
            THEN 'Canada'
        
        -- Default fallback for empty state and country (assume US for legacy data)
        ELSE 'United States'
    END
{% endmacro %}

-- Private helper macro for US states
{% macro _us_states() %}
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'GU', 'AS', 'MP'
{% endmacro %}

-- Private helper macro for Canadian provinces
{% macro _canadian_provinces() %}
    'AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'NU', 
    'ON', 'PE', 'QC', 'SK', 'YT'
{% endmacro %}