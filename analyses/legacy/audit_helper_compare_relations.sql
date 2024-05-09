{# in dbt Develop #}
--for some reason this did not work with the original set of code, I had to provide a ref function

{# this does not work, it returns relation None when passed to get_filtered_columns_in_relation()
{% set old_etl_relation=adapter.get_relation(
      database=target.database,
      schema="old_etl_schema",
      identifier="fct_orders"
) -%}
#}
--this does work
{% set old_etl_relation=ref('customer_orders') -%}

{% set dbt_relation=ref('fct_customer_orders') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="order_id"
) }}


