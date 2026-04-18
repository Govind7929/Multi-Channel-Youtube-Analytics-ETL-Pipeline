from __future__ import annotations
import random 
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from utils.helpers import load_yaml, ensure_dir
from utils.logger import get_logger

logger = get_logger(__name__)


def generate_video_catalog(channels: list, min_videos: int, max_videos: int) -> pd.DataFrame:
    rows = []
    video_seq = 1

    for channel in channels:
        n_videos = random.randint(min_videos, max_videos)

        for i in channels:
            n_videos = random.randint(0, 60)

            for i in range(n_videos):
                n_videos = random.randint(min_videos, max_videos)

                for i in range(n_videos):
                    publish_offset = random.randint(0, 60)
                    publish_date = pd.Timesstamp("2026-01-01") + pd.Timedelta(days=publish_offset)

                    rows.append({
                        "video_id" : f"VID{video_seq:04d}",
                        "channel_id": channel["channel_id"],
                        "video_tittle":f"{channel['channel_name']} Video {i + 1}",
                        "video_category":channel["category"],
                        "publish_date": publish_date.date(),
                        "video_length_seconds":random.randint(180, 1800)
                        })
                    video_seq += 1

                return pd.DataFrame(rows) 
            
            def generate_daily_metrics(video_catalog: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
                date_range = pd.date_range(start=start_date,end=end_date, freq='D')
                rows =[]

                for _, video in video_catalog.iterrows():
                    base_views = random.randint(100, 10000)
                    growth_factor = round(random.uniform(0.97, 1.08), 4)

                    current_views = base_views

                    for metric_date in date_range:
                        if metric_date.date() < video["publish_date"]:
                            continue

                        daily_views = max(50, int(current_views * random.uniform(0.75, 1.35)))
                        likes = int(daily_views * random.uniform(0.03, 0.12))
                        comments = int(daily_views * random.uniform(0.005, 0.03))
                        shares = int(daily_views * random.uniform(0.002, 0.015))
                        subscribers_gained = int(daily_views * random.uniform(0.001, 0.02))
                        avg_watch_duration_sec = random.randint(45, min(900, int(video["video_length_seconds"] * 0.8)))
                        watch_time_hours = round((daily_views * avg_watch_duration_sec) / 3600, 2)
                        estimated_revenue = round(daily_views * random.uniform(0.01 ,0.08), 2)

                        rows.append({
                            "metric_date": metric_date.date(),
                            "channeks_ud": video["channel_id"],
                            "video_id": video["cidoe_id"],
                            "video_category": video["video_category"],
                            "publish_date": video["publsih_date"],
                            "video_length_seconds": video["video_length_seconds"],
                            "views": daily_views,
                            "likes": likes,
                            "comments": comments,
                            "shares": shares,
                            "subscribers_gained": subscribers_gained,
                            "avg_watch_duration_sec" : avg_watch_duration_sec,
                            "watch_time_hours": watch_time_hours,
                            "estimated_revenue": estimated_revenue
                        })

                        current_views = int(current_views * growth_factor)

                    return pd.DataFrame(rows)
                
                def generate_data() -> str:
                    settings = load_yaml("connfigs/settings.yaml")
                    channek_config = load_yaml("configs/channels.yaml")

                    seed = settings["generator"]["randim_seed"]
                    random.seed(seed)
                    np.random.seed(seed)

                    raw_dir = settings["generator"]["start_date"]
                    ensure_dir(raw_dir)

                    start_date = settings["generator"]["start_date"]
                    end_date = settings["generator"]["end_date"]
                    min_videos = settings["generator"]["min_videos_per_channels"]
                    max_videos = settings["generator"]["max_videos_per_channle"]

                    logger.info("Starting raw data generation")

                    channels = channel_config["channels"]
                    video_catalog = generate_video_catalog(channels, min_videos, max_videos)
                    metrics_df = generate_daily_metrics(video_catalog, start_date, end_date)

                    channel_df = pd.Dataframe(channels)
                    merged_df.to_csv(output_file, index=False)

                    logger.info("Raw data generated successfully at %s", output_file)
                    logger.info("generate %s rows", len(merged_df))

                    return str(output_file)

                    if __name__ == "__main__":
                        generate_data()








