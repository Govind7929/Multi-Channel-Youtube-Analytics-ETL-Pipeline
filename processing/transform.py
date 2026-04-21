import pandas as pd


def _require_columns(df: pd.DataFrame, required_cols: list[str]) -> None:
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def clean_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    _require_columns(
        df,
        [
            "metric_date",
            "publish_date",
            "views",
            "likes",
            "comments",
            "shares",
            "subscribers_gained",
            "avg_watch_duration_sec",
            "watch_time_hours",
            "estimated_revenue",
            "video_length_seconds",
            "channel_name",
            "category",
            "country",
            "video_title",
            "video_category",
        ],
    )

    df["metric_date"] = pd.to_datetime(df["metric_date"]).dt.date
    df["publish_date"] = pd.to_datetime(df["publish_date"]).dt.date

    numeric_cols = [
        "views",
        "likes",
        "comments",
        "shares",
        "subscribers_gained",
        "avg_watch_duration_sec",
        "watch_time_hours",
        "estimated_revenue",
        "video_length_seconds",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    integer_cols = [
        "views",
        "likes",
        "comments",
        "shares",
        "subscribers_gained",
        "avg_watch_duration_sec",
        "video_length_seconds",
    ]

    for col in integer_cols:
        df[col] = df[col].astype("int64")

    for col in ["channel_name", "category", "country", "video_title", "video_category"]:
        df[col] = df[col].astype(str).str.strip()

    return df


def build_dim_channel(df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(df, ["channel_id", "channel_name", "category", "country"])
    dim_channel = (
        df[["channel_id", "channel_name", "category", "country"]]
        .drop_duplicates()
        .sort_values("channel_id")
        .reset_index(drop=True)
    )
    return dim_channel


def build_dim_video(df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        df,
        [
            "video_id",
            "channel_id",
            "video_title",
            "video_category",
            "publish_date",
            "video_length_seconds",
        ],
    )
    dim_video = (
        df[
            [
                "video_id",
                "channel_id",
                "video_title",
                "video_category",
                "publish_date",
                "video_length_seconds",
            ]
        ]
        .drop_duplicates()
        .sort_values(["channel_id", "video_id"])
        .reset_index(drop=True)
    )
    return dim_video


def build_fact_video_daily(df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        df,
        [
            "metric_date",
            "channel_id",
            "video_id",
            "views",
            "likes",
            "comments",
            "shares",
            "subscribers_gained",
            "avg_watch_duration_sec",
            "watch_time_hours",
            "estimated_revenue",
        ],
    )
    fact = df[
        [
            "metric_date",
            "channel_id",
            "video_id",
            "views",
            "likes",
            "comments",
            "shares",
            "subscribers_gained",
            "avg_watch_duration_sec",
            "watch_time_hours",
            "estimated_revenue",
        ]
    ].copy()

    fact["engagement_count"] = fact["likes"] + fact["comments"] + fact["shares"]
    fact["engagement_rate_pct"] = (
        ((fact["engagement_count"] * 100) / fact["views"].where(fact["views"] > 0, 1))
        .where(fact["views"] > 0, 0)
        .round(2)
    )

    return fact
