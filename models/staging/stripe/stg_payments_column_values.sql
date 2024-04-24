--way more work to just get distinct payment methods, but here it is...
-- what I actually want to build is a set of tuples (source_table, column_name, column value)
-- for any column which has a distinct list of values to easily check against.
-- but OTOH good requirements and testing should catch and document all that without needing to query for it
-- anymore.
{% set payment_methods =
        dbt_utils.get_column_values(
        table=ref('stg_payments'), 
        column='payment_method') 
%}

{% for payment_method in payment_methods %}
    select '{{payment_method}}' {% if not loop.last %} union {% endif %}
{% endfor %}