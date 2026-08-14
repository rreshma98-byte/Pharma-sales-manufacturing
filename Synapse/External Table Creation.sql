---creating credential-----

Create DATABASE scoped CREDENTIAL cred_pharma 
with 
IDENTITY = 'Managed Identity'


----Creating external datasource---

create EXTERNAL data source soruce_silver
WITH
(
   LOCATION = 'https://pharmastoragedatalake01.blob.core.windows.net/silver/', 
   CREDENTIAL = cred_pharma
)

create EXTERNAL data source soruce_gold
WITH
(
   LOCATION = 'https://pharmastoragedatalake01.blob.core.windows.net/gold/', 
   CREDENTIAL = cred_pharma
)


---Create File format----

Create EXTERNAl FILE FORMAT format_parquet
WITH(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

---Create External Table exthospital----

CREATE EXTERNAL Table gold.exhosp
WITH
(
   LOCATION = 'exthosp',
   DATA_SOURCE = soruce_gold,
   FILE_FORMAT =format_parquet
) AS
Select * from gold.hospital

------Create External Table extsales----

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = soruce_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales;

------Create External Table exinventory----

CREATE EXTERNAL TABLE gold.extinventory
WITH
(
    LOCATION = 'extinventory',
    DATA_SOURCE = soruce_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.inventory;


------Create External Table exreports----

CREATE EXTERNAL TABLE gold.extreports
WITH
(
    LOCATION = 'extreports',
    DATA_SOURCE = soruce_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.reports;
