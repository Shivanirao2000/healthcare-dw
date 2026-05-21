{#
  generate_schema_name — environment-aware schema routing.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Two isolation modes are supported.  Choose one per environment.        │
  │                                                                         │
  │  Mode A — Database isolation (default, recommended for this project)    │
  │  ─────────────────────────────────────────────────────────────────────  │
  │  All environments use the same schema names (STAGING, MARTS) so that    │
  │  Snowflake grants in snowflake_setup.sql apply unchanged.               │
  │  Dev/CI isolation is achieved at the *database* level by pointing       │
  │  SNOWFLAKE_DATABASE at a zero-copy clone (DEV_HEALTHCARE_DW).           │
  │                                                                         │
  │  Activate: DBT_SCHEMA_ISOLATION=database  (or leave unset)             │
  │  Result:   target=dev  → STAGING, MARTS   (inside DEV_HEALTHCARE_DW)   │
  │            target=prod → STAGING, MARTS   (inside HEALTHCARE_DW)       │
  │                                                                         │
  │  Mode B — Schema isolation (shared-database environments)               │
  │  ─────────────────────────────────────────────────────────────────────  │
  │  Schema names are prefixed with the dbt target name, keeping dev/CI     │
  │  runs isolated inside the *same* Snowflake database.                    │
  │  Requires additional per-developer schema grants in Snowflake.          │
  │                                                                         │
  │  Activate: DBT_SCHEMA_ISOLATION=schema                                  │
  │  Result:   target=dev  → DEV_STAGING, DEV_MARTS                        │
  │            target=ci   → CI_STAGING,  CI_MARTS                         │
  │            target=prod → STAGING, MARTS  (prod is always exact)        │
  └─────────────────────────────────────────────────────────────────────────┘

  Logic summary
  ─────────────
  1. If no custom schema was requested (+schema not set on the model),
     fall back to target.schema — standard dbt behaviour.

  2. In Mode A (database isolation, the default):
     Always use the custom schema name exactly as written in dbt_project.yml.
     The database-level clone provides the environment boundary.

  3. In Mode B (schema isolation):
     • prod target → exact custom schema name (clean production names).
     • any other target → UPPER(target.name) + '_' + custom schema name
       so DEV_STAGING, CI_MARTS, etc. are created inside the shared database.

  Changing modes at runtime
  ─────────────────────────
  Override in .env:       DBT_SCHEMA_ISOLATION=schema
  Override per dbt run:   dbt run --vars '{"schema_isolation": "schema"}'
  The env var is checked first; the dbt var takes precedence if both are set.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}

        {# No custom schema — use whatever the profile sets as target.schema. #}
        {{ target.schema }}

    {%- else -%}

        {#
          Determine the isolation mode.
          Priority: dbt var (--vars) > env var > default ('database').
          Using env_var with a fallback means the project runs without any
          configuration file changes; the .env file alone controls the mode.
        #}
        {%- set _mode = var(
              'schema_isolation',
              env_var('DBT_SCHEMA_ISOLATION', 'database')
            ) | lower -%}

        {%- if _mode == 'schema' and target.name != 'prod' -%}

            {#
              Schema-isolation mode, non-prod target.
              Prefix with an uppercased target name so schemas are namespaced
              per environment within the shared database.
              e.g. target.name='dev'  + custom_schema='staging' → DEV_STAGING
                   target.name='ci'   + custom_schema='marts'   → CI_MARTS
            #}
            {{ (target.name ~ '_' ~ custom_schema_name) | upper | trim }}

        {%- else -%}

            {#
              Database-isolation mode (default), OR schema-isolation prod target.
              Use the custom schema name exactly as written.
              For prod this is always exact regardless of mode.
            #}
            {{ custom_schema_name | trim }}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
