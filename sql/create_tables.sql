CREATE TABLE IF NOT EXISTS videos (
  id TEXT PRIMARY KEY,
  creator_id TEXT,
  video_created_at TIMESTAMP WITH TIME ZONE,
  views_count BIGINT DEFAULT 0,
  likes_count BIGINT DEFAULT 0,
  comments_count BIGINT DEFAULT 0,
  reports_count BIGINT DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS video_snapshots (
  id TEXT PRIMARY KEY,
  video_id TEXT REFERENCES videos(id) ON DELETE CASCADE,
  views_count BIGINT DEFAULT 0,
  likes_count BIGINT DEFAULT 0,
  comments_count BIGINT DEFAULT 0,
  reports_count BIGINT DEFAULT 0,
  delta_views_count BIGINT DEFAULT 0,
  delta_likes_count BIGINT DEFAULT 0,
  delta_comments_count BIGINT DEFAULT 0,
  delta_reports_count BIGINT DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots (created_at);
CREATE INDEX IF NOT EXISTS idx_videos_creator ON videos (creator_id);