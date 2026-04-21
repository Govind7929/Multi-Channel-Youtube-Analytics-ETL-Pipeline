# Power BI Dashboard Requirements

## Dashboard 1: Executive Overview
Cards:
- Total Views
- Total Revenue
- Total Watch Time Hours
- Total Subscribers Gained
- Average Engagement Rate %

Charts:
- Daily Views Trend by Channel
- Revenue by Channel
- Subscribers Gained by Channel
- Engagement Rate by Channel

Filters:
- Date
- Channel Name
- Category
- Country

Source tables/views:
- mart.vw_channel_daily_performance
- mart.vw_channel_ranking

---

## Dashboard 2: Channel Ranking Dashboard
Tables / visuals:
- Rank by Views
- Rank by Revenue
- Rank by Subscribers Gained
- Channel comparison matrix

Charts:
- Top 5 Channels by Views
- Top 5 Channels by Revenue
- Top 5 Channels by Watch Time

Source:
- mart.vw_channel_ranking

---

## Dashboard 3: Video Performance Dashboard
Cards:
- Top Video by Views
- Top Video by Revenue
- Top Video by Engagement Rate

Charts:
- Top 10 Videos by Views
- Top 10 Videos by Revenue
- Video Category Performance
- Video Publish Date vs Total Views

Source:
- mart.vw_top_videos
- mart.dim_video
- mart.fact_video_daily

---

## Suggested DAX Measures
Total Views = SUM(fact_video_daily[views])
Total Revenue = SUM(fact_video_daily[estimated_revenue])
Total Watch Time = SUM(fact_video_daily[watch_time_hours])
Total Subscribers Gained = SUM(fact_video_daily[subscribers_gained])
Avg Engagement Rate = AVERAGE(fact_video_daily[engagement_rate_pct])
