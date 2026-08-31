USE ROLE ACCOUNTADMIN;

CREATE API INTEGRATION IF NOT EXISTS git_api_github
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/ManasShouche')
    ENABLED = TRUE;

CREATE SCHEMA IF NOT EXISTS dbt_pipe.workspace;

CREATE GIT REPOSITORY IF NOT EXISTS dbt_pipe.workspace.tpch_repo
    API_INTEGRATION = git_api_github
    ORIGIN = 'https://github.com/ManasShouche/dbt_project.git';

ALTER GIT REPOSITORY dbt_pipe.workspace.tpch_repo FETCH;

LS @dbt_pipe.workspace.tpch_repo/branches/main/snowflake/;
