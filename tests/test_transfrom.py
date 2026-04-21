import pandas as pd
import pytest
from processing.transform import clean_raw_data, build_fact_video_daily


def test_clean_raw_data():
    df = pd.DataFrame({
        "metric_date": ["2026-01-01"],
        "publish_date": ["2026-01-01"],
        "views": ["100"],
        "likes": ["10"],
        "comments": ["2"],
        "shares": ["1"],
        "subscribers_gained": ["5"],
        "avg_watch_duration_sec": ["120"],
        "watch_time_hours": ["3.5"],
        "estimated_revenue": ["12.5"],
        "video_length_seconds": ["500"],
        "channel_name": [" TechWithData "],
        "category": [" Technology "],
        "country": [" India "],
        "video_title": [" Sample Video "],
        "video_category": [" Education "]
    })

    cleaned = clean_raw_data(df)

    assert cleaned.loc[0, "views"] == 100
    assert cleaned.loc[0, "channel_name"] == "TechWithData"


def test_build_fact_video_daily():
    df = pd.DataFrame({
        "metric_date": ["2026-01-01"],
        "channel_id": ["CH001"],
        "video_id": ["VID0001"],
        "views": [100],
        "likes": [10],
        "comments": [5],
        "shares": [5],
        "subscribers_gained": [3],
        "avg_watch_duration_sec": [150],
        "watch_time_hours": [4.2],
        "estimated_revenue": [20.5]
    })

    fact = build_fact_video_daily(df)

    assert fact.loc[0, "engagement_count"] == 20
    assert fact.loc[0, "engagement_rate_pct"] == 20.0


def test_build_fact_video_daily_handles_zero_views():
    df = pd.DataFrame({
        "metric_date": ["2026-01-01"],
        "channel_id": ["CH001"],
        "video_id": ["VID0001"],
        "views": [0],
        "likes": [10],
        "comments": [5],
        "shares": [5],
        "subscribers_gained": [3],
        "avg_watch_duration_sec": [150],
        "watch_time_hours": [4.2],
        "estimated_revenue": [20.5]
    })

    fact = build_fact_video_daily(df)

    assert fact.loc[0, "engagement_count"] == 20
    assert fact.loc[0, "engagement_rate_pct"] == 0


def test_clean_raw_data_requires_columns():
    with pytest.raises(ValueError, match="Missing required columns"):
        clean_raw_data(pd.DataFrame({"metric_date": ["2026-01-01"]}))
