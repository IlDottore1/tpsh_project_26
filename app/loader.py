import asyncio
import json
import os
from datetime import datetime
import asyncpg
from pathlib import Path

DB_DSN = os.getenv("DATABASE_DSN", "postgresql://postgres:postgres@db:5432/postgres")
DATA_PATH = os.getenv("DATA_PATH", "/app/data/videos.json")

async def create_tables(conn):
    sql = Path("/app/sql/create_tables.sql").read_text()
    await conn.execute(sql)

async def load_data(conn):
    p = Path(DATA_PATH)
    if not p.exists():
        print("DATA file not found:", DATA_PATH)
        return
    data = json.loads(p.read_text(encoding="utf-8"))
    videos = data.get("videos") if isinstance(data, dict) else data

    async with conn.transaction():
        for v in videos:
            await conn.execute("""
                INSERT INTO videos (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7, now(), now())
                ON CONFLICT (id) DO UPDATE SET
                  creator_id = EXCLUDED.creator_id,
                  video_created_at = EXCLUDED.video_created_at,
                  views_count = EXCLUDED.views_count,
                  likes_count = EXCLUDED.likes_count,
                  comments_count = EXCLUDED.comments_count,
                  reports_count = EXCLUDED.reports_count,
                  updated_at = now();
            """,
            str(v.get("id")),
            str(v.get("creator_id")),
            parse_ts(v.get("video_created_at")),
            int_or_zero(v.get("views_count")),
            int_or_zero(v.get("likes_count")),
            int_or_zero(v.get("comments_count")),
            int_or_zero(v.get("reports_count"))
            )

            # snapshots
            snapshots = v.get("snapshots") or v.get("video_snapshots") or []
            for s in snapshots:
                await conn.execute("""
                    INSERT INTO video_snapshots (id, video_id, views_count, likes_count, comments_count, reports_count,
                    delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, now())
                    ON CONFLICT (id) DO UPDATE SET
                      views_count = EXCLUDED.views_count,
                      likes_count = EXCLUDED.likes_count,
                      comments_count = EXCLUDED.comments_count,
                      reports_count = EXCLUDED.reports_count,
                      delta_views_count = EXCLUDED.delta_views_count,
                      delta_likes_count = EXCLUDED.delta_likes_count,
                      delta_comments_count = EXCLUDED.delta_comments_count,
                      delta_reports_count = EXCLUDED.delta_reports_count,
                      updated_at = now();
                """,
                str(s.get("id")),
                str(v.get("id")),
                int_or_zero(s.get("views_count")),
                int_or_zero(s.get("likes_count")),
                int_or_zero(s.get("comments_count")),
                int_or_zero(s.get("reports_count")),
                int_or_zero(s.get("delta_views_count")),
                int_or_zero(s.get("delta_likes_count")),
                int_or_zero(s.get("delta_comments_count")),
                int_or_zero(s.get("delta_reports_count")),
                parse_ts(s.get("created_at"))
                )

def int_or_zero(x):
    try:
        return int(x) if x is not None else 0
    except:
        return 0

def parse_ts(x):
    if x is None:
        return None
    try:
        return datetime.fromisoformat(x.replace("Z", "+00:00"))
    except Exception:
        return None

async def main():
    conn = await asyncpg.connect(DB_DSN)
    try:
        await create_tables(conn)
        await load_data(conn)
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())