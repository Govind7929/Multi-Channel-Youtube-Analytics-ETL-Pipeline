CREATE TABLE IF NOT EXISTS raw.raw_video_daily (
    metric_date DATE,
    channel_id VARCHAR(20),
    video_id VARCHAR(20),
    video_title VARCHAR(255),
    video_category VARCHAR(100),
    publish_date DATE,
    video_length_seconds INT,
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    shares BIGINT,
    subscribers_gained BIGINT,
    avg_watch_duration_sec INT,
    watch_time_hours NUMERIC(18,2),
    estimated_revenue NUMERIC(18,2),
    channel_name VARCHAR(255),
    category VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS mart.dim_channel (
    channel_id VARCHAR(20) PRIMARY KEY,
    channel_name VARCHAR(255),
    category VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS mart.dim_video (
    video_id VARCHAR(20) PRIMARY KEY,
    channel_id VARCHAR(20) REFERENCES mart.dim_channel(channel_id),
    video_title VARCHAR(255),
    video_category VARCHAR(100),
    publish_date DATE,
    video_length_seconds INT
);

CREATE TABLE IF NOT EXISTS mart.fact_video_daily (
    metric_date DATE,
    channel_id VARCHAR(20) REFERENCES mart.dim_channel(channel_id),
    video_id VARCHAR(20) REFERENCES mart.dim_video(video_id),
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    shares BIGINT,
    subscribers_gained BIGINT,
    avg_watch_duration_sec INT,
    watch_time_hours NUMERIC(18,2),
    estimated_revenue NUMERIC(18,2),
    engagement_count BIGINT,
    engagement_rate_pct NUMERIC(10,2)
);