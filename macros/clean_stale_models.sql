{#

    --let's develop a macro that
    1. queries the information schema of a database
    2. finds objects that are > 1 week old (i.e. no longer maintained)
    3. generates automated drop statements
    4. has the ability to execute those drop statements
    5. hawt

    Unfortunately, this will not apply to redshift, it does not keep a last_altered or equivalent column
    automatically.  Presumably it could be made to?  Another question for the future.

#}

{% macro clean_stale_models(database= target.database, schema=target.schema, days=7, dry_run = True) %}
    
    {% set query %}
        
        select
            -- table_type,
            -- table_schema,
            -- table_name,
            '2024-04-16'::date as last_altered,
            CASE when table_type = 'VIEW' then table_type else 'TABLE' end as drop_type,
            'DROP ' || drop_type || ' {{ database }}.' || table_schema || '.' || table_name || ';' as drop_query
        from {{ database }}.information_schema.tables
        where table_schema = '{{ schema }}'
        and last_altered <= current_date - {{ days }}
        -- order by last_altered desc

    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(query).columns[2].values() %}

    {% for query in drop_queries %}

        {% if dry_run %}
        
            {{ log(query, info=True) }}

        {% else %}

            {{ log('Dropping table with command: ' ~ query, info=True) }}
            {% do run_query(query) %}

        {% endif %}
    {% endfor %}

{% endmacro %}