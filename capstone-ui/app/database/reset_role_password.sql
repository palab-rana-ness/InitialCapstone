-- Run this if the "results" role already exists but its password doesn't match
-- what's in .env's DATABASE_URL (e.g. because CREATE ROLE ran once with the
-- placeholder before the file was edited). Safe to run any time.
ALTER ROLE results WITH PASSWORD 'qwertyuiop';
