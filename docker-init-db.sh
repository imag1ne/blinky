#!/bin/bash
# Database initialization script for Docker
# This runs automatically when the PostgreSQL container first starts
#
# Environment Variables (set in .env file):
#   BLINKY_DB_PASSWORD - Password for the blinky application user
#
# The .env file is used for production configuration.
# For development/testing, copy .env.example to create your own .env file.

set -e

echo "Creating application user 'blinky'..."

# Create the blinky user with password from environment variable
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create the application user (if it doesn't already exist)
    DO \$\$
    BEGIN
      IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'blinky') THEN
        CREATE USER blinky WITH PASSWORD '${BLINKY_DB_PASSWORD}';
        RAISE NOTICE 'User "blinky" created successfully';
      ELSE
        RAISE NOTICE 'User "blinky" already exists, skipping creation';
        -- Update password in case it changed
        ALTER USER blinky WITH PASSWORD '${BLINKY_DB_PASSWORD}';
        RAISE NOTICE 'Updated password for user "blinky"';
      END IF;
    END
    \$\$;

    -- Grant connection privilege
    GRANT CONNECT ON DATABASE blinky TO blinky;

    -- Grant schema usage
    GRANT USAGE ON SCHEMA public TO blinky;

    -- Grant table privileges
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO blinky;

    -- Grant sequence privileges
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO blinky;

    -- Set default privileges for future tables
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
      GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO blinky;

    ALTER DEFAULT PRIVILEGES IN SCHEMA public
      GRANT USAGE, SELECT ON SEQUENCES TO blinky;

    SELECT 'User blinky created successfully' AS status;
EOSQL

echo "Database initialization complete!"
