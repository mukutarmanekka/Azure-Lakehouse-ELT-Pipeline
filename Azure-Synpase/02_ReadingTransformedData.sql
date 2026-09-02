SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://olistdatastoragemukut.dfs.core.windows.net/olistdata/silver/transformed_data',
        FORMAT = 'PARQUET'
    ) AS [result]