--This doesn't seem to want to run against the dev database
-- as a model anyways,
-- but can run against the dev database
-- requires more investigation, but here is a solution 
-- which demonstrates the code working when compiled.  ¯\_(ツ)_/¯
-- note that union_relations from dbt_utils could handle unions with different columns.  Neat.

{{
    union_tables_by_prefix(
        database = 'dev',
        schema = 'dbt_jhilliard',
        prefix = ''
    )
}}