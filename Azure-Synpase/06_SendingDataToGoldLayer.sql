CREATE SCHEMA gold;
CREATE EXTERNAL TABLE gold.finalTable
WITH (
    LOCATION = 'Final',
    DATA_SOURCE = goldlayer,
    FILE_FORMAT = extfileformat
)
AS
SELECT *
FROM olist.dbo.final2;