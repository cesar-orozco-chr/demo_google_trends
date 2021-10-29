import pytest
import json
from pytrends.request import TrendReq
from utils import (
    _consolidate_google_trends_request, 
    GoogleTrendsQuery, 
    consolidate_top_query_per_keyword_category,
    build_data_per_country,
    _get_keywords,
    _get_countries
) 


source ={
            "keywords":["Disney+"],
            "countries":["AR"],
            "timeframe": "2021-01-01 2021-09-13"
        }
    


pytrends = TrendReq()

def test_extract_keywords_from_source():
    keywords = _get_keywords(json.dumps(source))
    assert len(keywords) == 1

def test_query_related_queries():
    
    fake_df = _consolidate_google_trends_request(
                country=source["countries"],
                timeframe=source["timeframe"],
                keyword_list=source["keywords"],
                category=0,
                google_trends_query=GoogleTrendsQuery.RELATED_QUERIES,
            
    )
    assert type(fake_df) is not None

def test_query_interest_over_time():
    fake_df = _consolidate_google_trends_request(
                country=source["countries"][0],
                timeframe=source["timeframe"],
                keyword_list=source["keywords"],
                category=0,
                google_trends_query=GoogleTrendsQuery.INTEREST_OVER_TIME,
            
    )
    assert type(fake_df) is not None

def test_consolidate_top_query_per_keyword_category():
    df = consolidate_top_query_per_keyword_category(
        country=source["countries"][0],
        timeframe=source["timeframe"],
        pytrends=pytrends,
        keyword_list=list(source["keywords"]),
        include_cat=False
    )
    assert type(df) is not None

def test_build_data_per_country():
    df = build_data_per_country(timeframe=source["timeframe"],
                    geo_list=list(source["countries"]),
                    pytrends=pytrends,
                    keyword_list=list(source["keywords"]),
                    
                    include_cat=False)
    assert type(df) is not None
