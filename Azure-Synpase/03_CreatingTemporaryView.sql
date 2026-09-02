CREATE OR ALTER VIEW dbo.final2
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://olistdatastoragemukut.dfs.core.windows.net/olistdata/silver/transformed_data/*.parquet',
    FORMAT = 'PARQUET'
) AS result
WHERE order_status = 'delivered';