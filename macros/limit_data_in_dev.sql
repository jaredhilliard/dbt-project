{# needed to convert current_timestamp to date to run on my particular warehouse.
    but this doesn't consider queries where you already have where... 
    and you would likely want to apply this at the beginning of your models, not the end, to process less data.
    where you might already have logic.
#}

{% macro limit_data_in_dev(column_name, dev_days_of_data = 3) %}
    {% if target.name == 'dev' or target.name == 'default' %}
        where {{column_name}} >= dateadd('day',-{{dev_days_of_data}},current_timestamp::date)
    {% endif %}
{% endmacro %}