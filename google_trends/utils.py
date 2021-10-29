from datetime import datetime
import time
import pandas as pd
import json
from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError
from enum import Enum

class GoogleTrendsQuery(Enum):
    """Options available for queries 

    Args:
        Enum ([Enum]): Base class
    """
    RELATED_QUERIES=1
    INTEREST_OVER_TIME=2

def _get_keywords(source)->list:
    """Generic function to extract keywords from source

    Args:
        source ([json]): JSON source to process

    Returns:
        list: All keywords found
    """
    data = json.loads(source)
    return list(data['keywords'])


def _get_countries(source)->list:
    """Generic function to extract countries from source

    Args:
        source ([json]): JSON source to process

    Returns:
        list: All countries found
    """
    data = json.loads(source)
    return list(data['countries'])

def _consolidate_google_trends_request(country:str, 
                                       timeframe:str, 
                                       keyword_list:list,
                                       google_trends_query:int, 
                                       category:int,
                                       pytrends:TrendReq=None)->pd.DataFrame:
    """Core function to wrap Google Trends request. This function can be used to
       query for every query option contained in GoogleTrendsQuery Enum.

    Args:
        country (str): Country to use in query
        timeframe (str): Time range to use in query
        keyword_list (list): List of keywords to use in query
        google_trends_query (int): Define which query option to use
        category (int): Category to use in query
        pytrends (TrendReq, optional): Custom TrendReq object to use. Defaults to None.

    Returns:
        pd.DataFrame: Consolidated DataFrame per keyword and country.
    """
                                       

    if pytrends is None:
        pytrends=TrendReq(timeout=(10,25), retries=10)

    keyword_df_list = []
    for kw in keyword_list:
        print(f"Running {google_trends_query} requests for keyword {kw}, for country {country}, category {category}")
        try:
            pytrends.build_payload([kw], timeframe=timeframe, geo=country, cat=category)
            if google_trends_query == GoogleTrendsQuery.RELATED_QUERIES:
                google_trends_response = pytrends.related_queries()
                df = pd.DataFrame(google_trends_response[list(google_trends_response.keys())[0]]['top'])
            elif google_trends_query == GoogleTrendsQuery.INTEREST_OVER_TIME:
                google_trends_response = pytrends.interest_over_time()
                df = pd.DataFrame(google_trends_response)
            df['country'] = country
            df['search_keyword'] = kw
            keyword_df_list.append(df)

        except ResponseError as e:
            print(e)
        except ValueError as ve:
            print(ve)
    try:
        return pd.concat(keyword_df_list)
    except ValueError as e:
        print(e)

def consolidate_top_query_per_keyword_category(country:str, 
                                               timeframe:str, 
                                               pytrends:TrendReq,
                                               keyword_list:list,
                                               include_cat:bool)-> pd.DataFrame:
    """Wraps top query per keyword including categories

    Args:
        country (str): Country to use in query
        timeframe (str): Time range to use in query
        pytrends (TrendReq): Custom TrendReq object to use.
        keyword_list (list): List of keywords to use in query
        include_cat (bool): True include category list, False do not include.

    Returns:
        pd.DataFrame: Consolidated data
    """

    cat_list = []
    print("Running requests by categories")
    try:
        if include_cat:
            categories = pd.json_normalize(pytrends.categories()['children'])
            for id in categories['id']:
                df = _consolidate_google_trends_request(country=country,
                                                timeframe=timeframe,
                                                pytrends=pytrends,
                                                keyword_list=keyword_list,
                                                google_trends_query=GoogleTrendsQuery.RELATED_QUERIES,
                                                category=id)
                df['category_id'] = id
                cat_list.append(df)
        else:
            df = _consolidate_google_trends_request(country=country,
                                                timeframe=timeframe,
                                                pytrends=pytrends,
                                                keyword_list=keyword_list,
                                                google_trends_query=GoogleTrendsQuery.RELATED_QUERIES,
                                                category=0)
            df['category_id'] = 0
            cat_list.append(df)
            
        return pd.concat(cat_list)
    except ValueError as e:
        print(e)

def build_data_per_country(timeframe:str, 
                           geo_list:list,
                           pytrends:TrendReq, 
                           keyword_list:list,
                           include_cat:bool) -> pd.DataFrame:
    """Builds a Dataframe with top queries consolidated by each topic in KW_LIST variable

    Args:
        timeframe (str): Timeframe for query
        geo_list (list): Country list
        include_cat (bool): If True function will include categories in result
        pytrends (TrendReq): Custom TrendReq object to use.
        keyword_list (list): List of keywords to use in query
        include_cat (bool): True include category list, False do not include.

    Returns:
        pd.DataFrame: Consolidated data
    """
    
    if timeframe is None:
        timeframe = 'today 1-m'
    _geo_list = []
    for geo in geo_list:
        kwdf = consolidate_top_query_per_keyword_category(country=geo, 
                                            timeframe=timeframe, 
                                            pytrends=pytrends,
                                            keyword_list=keyword_list,
                                            include_cat=include_cat)
        if type(kwdf) is not type(None):
            kwdf['date_loaded'] = datetime.now()
        _geo_list.append(kwdf)
        time.sleep(20)
    try:
        df_cols = ["query", "score", "country", "search_keyword", "category_id", "date_loaded"]
        result_df = pd.concat(_geo_list)
        result_df.columns = df_cols 
        return result_df
    except ValueError as e:
        print(e) 

def get_interest_overtime_by_term(term:str, 
                                  interest_over_time_df:pd.DataFrame)->pd.DataFrame:
    """Returns a formatted dataset using an specific keyword

    Args:
        term (str): Term/Keyword to perform the transformation
        interest_over_time_df (pd.DataFrame): Base DataFrame from which the result is extracted

    Returns:
        pd.DataFrame: Formatted dataset per keyword
    """
    if term in interest_over_time_df.columns:
        temp_df = interest_over_time_df[term].reset_index()
        temp_df.rename(columns={term:"score"}, inplace=True)
        temp_df['term'] = term
        return temp_df

def upload_df_to_google_cloud_bigquery(source_df: pd.DataFrame,
                                    if_exists_behaviour:str,
                                    project_id:str,
                                    dataset_name: str):
    """Uploads a DataFrame into Bigquery

    Args:
        source_df (pd.DataFrame): Source DataFrame
        if_exists_behaviour (str): Behaviour if the dataset exists in Bigquery
        project_id (str): GCP project ID
        dataset_name (str): Dataset Name
    """
    source_df.to_gbq(
        project_id=project_id,
        destination_table=dataset_name,
        if_exists=if_exists_behaviour
    )

def upload_df_to_snowflake(
        source_df: pd.DataFrame,
        snowflake_table:str,
        snowflake_config: dict
    ):
    """Uploads a DataFrame into SnowFlake

    Args:
        source_df (pd.DataFrame): Source DataFrame
        snowflake_table (str): Existing Snowflake table
        snowflake_config (dict): Snowflake connection config
    """
    from snowflake.connector import connect,  ProgrammingError
    from snowflake.connector.pandas_tools import write_pandas
    upper_cols = [col.upper() for col in source_df.columns]
    source_df.columns = upper_cols 
    
    connector = connect(
            user=snowflake_config['user'],
            password=snowflake_config['password'],
            account=snowflake_config['account'],
            warehouse=snowflake_config['warehouse'],
            database=snowflake_config['database'],
            schema=snowflake_config['schema']
    )
    try:
        success, nchunks, nrows, _ = write_pandas(
        connector,
        source_df,
        snowflake_table.upper()
        )
        print(f"Query status: {success}, Number of rows: {nrows}")
    except ProgrammingError as e:
        print(f"Snowflake query failed: {e}")

def build_interest_by_country(timeframe:str, 
                           geo_list:list,
                           pytrends:TrendReq,
                           category:int, 
                           keyword_list:list)->pd.DataFrame:
    """Consolidates interest by country

    Args:
        timeframe (str): Timeframe for query
        geo_list (list): Country list
        pytrends (TrendReq): Custom TrendReq object to use.
        category (int): Category to use in query
        keyword_list (list): List of keywords to use in query

    Returns:
        pd.DataFrame: Consolidated data
    """
    result_list = []
    for geo in geo_list:
        interest_query_df = _consolidate_google_trends_request(
            timeframe=timeframe,
            keyword_list=keyword_list,
            google_trends_query=GoogleTrendsQuery.INTEREST_OVER_TIME,
            pytrends=pytrends,
            country=geo,
            category=category

        )
        if type(interest_query_df) is not type(None):
            interest_query_df['date_loaded'] = datetime.now()
        time.sleep(20)

        kw_list_df = []
        for kw in keyword_list:
            interest_over_kw = get_interest_overtime_by_term(kw, interest_query_df)
            kw_list_df.append(interest_over_kw)
        
        temp_df = pd.concat(kw_list_df)
        
        temp_df['country_alpha_code_2'] = geo
        result_list.append(temp_df)
    result_df = pd.concat(result_list)
    return result_df
        
    
