from pytrends.request import TrendReq
from utils import (build_data_per_country,
                  build_interest_by_country,
                  upload_df_to_google_cloud_bigquery)
from config import (
    PROJECT_ID,
    DATASET_ID,
    top_search,
    interest_over_time
)


if __name__ == '__main__':
    pytrends = TrendReq(timeout=(10,25), retries=10)
    config = {
            "keywords":["Disney+"],
            "countries":["AR"],
            "timeframe": "2021-01-01 2021-09-13"
        }

    topsearch_df = build_data_per_country(
        timeframe=config["timeframe"],
        geo_list=config["countries"],
        pytrends=pytrends,
        keyword_list=config["keywords"],
        include_cat=False

    )

    interest_overtime_df = build_interest_by_country(
        timeframe=config["timeframe"],
        geo_list=config["countries"],
        pytrends=pytrends,
        keyword_list=config["keywords"],
        category=0
    )

    
    upload_df_to_google_cloud_bigquery(
        source_df=topsearch_df,
        project_id=PROJECT_ID,
        dataset_name=top_search["table_name"],
        if_exists_behavior=top_search["if_exists_behaviour"]
    )
    
    upload_df_to_google_cloud_bigquery(
        source_df=interest_overtime_df,
        project_id=PROJECT_ID,
        dataset_name=interest_over_time["table_name"],
        if_exists_behavior=interest_over_time["if_exists_behaviour"]
    )
    

    







