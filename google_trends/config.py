import os

PROJECT_ID = os.getenv("PROJECT_ID", "my_project")
DATASET_ID = f"{PROJECT_ID}.my_dataset"

top_search ={
    "table_name" : f"{DATASET_ID}.google_trends_top_search",
    "if_exists_behaviour": "replace"
}

interest_over_time = {
    "table_name" : f"{DATASET_ID}.google_trends_interest_over_time",
    "if_exists_behaviour": "append"
}

SNOWFLAKE = {
    'user': os.getenv("SNOWFLAKE_USER","sample-user"),
    'password':os.getenv("SNOWFLAKE_PASSWORD","sample"),
    'account':os.getenv("SNOWFLAKE_ACCOUNT","sample-account"),
    'warehouse':os.getenv("SNOWFLAKE_WAREHOUSE","sample-wh"),
    'database':os.getenv("SNOWFLAKE_DATABASE",'sample-db'),
    'schema':os.getenv("SNOWFLAKE_SCHEMA",'sample-schema')
}