CREATE TABLE IF NOT EXISTS {table_name}
AS
SELECT
    user_id,
    CAST(items AS INTEGER[]) AS items,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at
FROM delta_scan('{data_path}')