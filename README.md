# Blinky

Blinky is a Rust-based URL shortener. It accepts a long link, returns a short one, and tracks every redirect so you can see when it was used. PostgreSQL stores the core data, Redis can step in for caching and async work, and everything runs as a single Axum service.

Demo: https://blinky-demo.indexno.de/

## Features
- Straightforward HTTP endpoints to shorten links, follow them, view stats, and check health.
- Base58 short codes that stay readable, with automatic retries if a code collides.
- Clicks are recorded immediately; you can also stream events into Redis for background processing.
- Redis caching is optional, turn it on for faster lookups, or run without it when you prefer.
- Structured logs and configurable CORS.

## Quick start
1. **Install dependencies**  
   Rust toolchain and `sqlx-cli`. Provision PostgreSQL; Redis is optional.
2. **Adjust env vars**
   Copy `.env.example` to `.env`, then update `DATABASE_URL`, `BASE_URL`, and optional Redis URLs
   
3. **Apply migrations**
   ```bash
   sqlx migrate run
   ```
4. **Start the service**
   ```bash
   cargo run --release
   ```
5. **Create a shortcode**
   ```bash
   curl -X POST http://localhost:8080/shorten \
     -H 'Content-Type: application/json' \
     -d '{"url":"https://example.com"}'
   ```
   Follow the returned `short_url`, or call `/stats/{code}` to watch the counter update.

### Docker option
```bash
docker compose -f docker-compose.dev.yml up -d
```
This brings up PostgreSQL, Redis (cache + queue), runs migrations, and starts Blinky on port 8080.

## API overview
| Method | Path | Purpose | Notes |
| --- | --- | --- | --- |
| `POST` | `/shorten` | Exchange a long URL for a short one. | Body: `{"url":"https://..."}`. Response includes `short_url`. |
| `GET` | `/{code}` | Follow a short link. | Issues a `301` redirect and logs the click. |
| `GET` | `/stats/{code}` | Retrieve click analytics. | Returns the shortcode, total clicks, and last access time. |
| `GET` | `/health_check` | Verify the service is responding. | Returns `{"status":"ok"}` on success. |

## Configuration overview
Set these through `.env.example`, your shell, or your deployment platform. The most important flags are below; the rest are documented in `.env.example`.

| Variable | What it controls | Default |
| --- | --- | --- |
| `APP_HOST` / `APP_PORT` | Where the web server listens. | `0.0.0.0` / `8080` |
| `BASE_URL` | The domain that prefixes short codes. | `http://localhost:8080` |
| `DATABASE_URL` | Postgres connection string. | **required** |
| `REDIS_CACHE_URL` | Enables the cache layer when set. | unset |
| `CACHE_TTL_SECS` | How long cached URLs stick around. | `3600` |
| `REDIS_QUEUE_URL` | Turns on the click queue + worker. | unset |
| `QUEUE_*` knobs | Fine-tune how the worker reads the queue. | see `.env.example` |
| `SHORTCODE_MAX_RETRIES` | How many times we retry generating a code. | `5` |
| `CORS_*` settings | Allow other origins to call the API. | CORS off by default |
| `LOG_FORMAT` / `RUST_LOG` | Plain vs JSON logs and log level. | `json` / `info` |

## Deployment notes

- The multi-stage image compiles the release binary and produces a slim runtime layer. 
   ```bash
   docker build -t blinky:latest .
   docker run --rm -p 8080:8080 --env-file .env blinky:latest
   ```
- Use your own `docker-compose.yml` for the full stack (Postgres, Redis cache, Redis queue, migrations, and the app). For local development, `docker-compose.dev.yml` mounts your working tree and exposes developer-friendly ports.

- SQLx runs in offline mode via `.cargo/config.toml`, letting `cargo build/test` work without a live Postgres instance. After changing SQL queries, regenerate offline data with `cargo sqlx prepare -- --lib` (or `-- --bin blinky`) against a database that has the latest migrations applied.

## License
Released under the MIT License. See `LICENSE` for full text and terms.
