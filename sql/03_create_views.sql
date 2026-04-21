CREATE OR REPLACE VIEW mart.vw_channel_daily_performance AS
SELECT
    f.metric_date,
    c.channel_id,
    c.channel_name,
    c.category,
    c.country,
    SUM(f.views) AS total_views,
    SUM(f.likes) AS total_likes,
    SUM(f.comments) AS total_comments,
    SUM(f.shares) AS total_shares,
    SUM(f.subscribers_gained) AS total_subscribers_gained,
    SUM(f.watch_time_hours) AS total_watch_time_hours,
    SUM(f.estimated_revenue) AS total_revenue,
    SUM(f.engagement_count) AS total_engagement,
    ROUND(
        CASE
            WHEN SUM(f.views) > 0 THEN (SUM(f.engagement_count)::numeric / SUM(f.views)) * 100
            ELSE 0
        END, 2
    ) AS engagement_rate_pct
FROM mart.fact_video_daily f
JOIN mart.dim_channel c
    ON f.channel_id = c.channel_id
GROUP BY
    f.metric_date,
    c.channel_id,
    c.channel_name,
    c.category,
    c.country;

CREATE OR REPLACE VIEW mart.vw_channel_ranking AS
SELECT
    c.channel_id,
    c.channel_name,
    c.category,
    SUM(f.views) AS total_views,
    SUM(f.watch_time_hours) AS total_watch_time_hours,
    SUM(f.subscribers_gained) AS total_subscribers_gained,
    SUM(f.estimated_revenue) AS total_revenue,
    ROUND(AVG(f.engagement_rate_pct), 2) AS avg_engagement_rate_pct,
    RANK() OVER (ORDER BY SUM(f.views) DESC) AS views_rank,
    RANK() OVER (ORDER BY SUM(f.estimated_revenue) DESC) AS revenue_rank,
    RANK() OVER (ORDER BY SUM(f.subscribers_gained) DESC) AS subscriber_rank
FROM mart.fact_video_daily f
JOIN mart.dim_channel c
    ON f.channel_id = c.channel_id
GROUP BY
    c.channel_id,
    c.channel_name,
    c.category;

CREATE OR REPLACE VIEW mart.vw_top_videos AS
SELECT
    v.video_id,
    v.video_title,
    c.channel_name,
    v.video_category,
    SUM(f.views) AS total_views,
    SUM(f.likes) AS total_likes,
    SUM(f.comments) AS total_comments,
    SUM(f.shares) AS total_shares,
    SUM(f.estimated_revenue) AS total_revenue,
    ROUND(AVG(f.engagement_rate_pct), 2) AS avg_engagement_rate_pct
FROM mart.fact_video_daily f
JOIN mart.dim_video v
    ON f.video_id = v.video_id
JOIN mart.dim_channel c
    ON f.channel_id = c.channel_id
GROUP BY
    v.video_id,
    v.video_title,
    c.channel_name,
    v.video_category;