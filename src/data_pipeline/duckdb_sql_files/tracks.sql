CREATE TABLE IF NOT EXISTS {table_name}
AS
SELECT
    id,
    name,
    artist,
    songwriters,
    duration,
    genres,
    album,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at
FROM delta_scan('{data_path}')