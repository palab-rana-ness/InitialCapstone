-- Run this ONCE in pgAdmin's Query Tool while connected as the "postgres" superuser
-- (the account/password you set during the PostgreSQL install).
-- Replace 'CHANGE_ME' with a real password of your choosing before running,
-- then put that SAME password into C:\Capstone1-main\.env's DATABASE_URL.

CREATE ROLE results WITH LOGIN PASSWORD 'qwertyuiop';
CREATE DATABASE noticed_incidents OWNER results;
GRANT ALL PRIVILEGES ON DATABASE noticed_incidents TO results;
