-- Add migration script here
-- url & analytics
CREATE TABLE IF NOT EXISTS short_urls (
  id BIGSERIAL PRIMARY KEY,
  short_code VARCHAR(32) UNIQUE NOT NULL,
  original_url TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS url_analytics (
  short_url_id BIGINT PRIMARY KEY REFERENCES short_urls(id) ON DELETE CASCADE,
  click_count BIGINT NOT NULL DEFAULT 0,
  last_clicked_at TIMESTAMPTZ
);
