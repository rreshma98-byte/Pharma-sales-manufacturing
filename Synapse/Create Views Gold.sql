
--------Creating VIEWS-----

---Hospital---
Create VIEW gold.hospital
as 
Select * from OPENROWSET
( BULK 'https://pharmastoragedatalake01.blob.core.windows.net/silver/Hospital/',
FORMAT = 'PARQUET') as Q1

---Inventory----

Create VIEW gold.inventory
as 
Select * from OPENROWSET
( BULK 'https://pharmastoragedatalake01.blob.core.windows.net/silver/Inventory/',
FORMAT = 'PARQUET') as Q2

----Sales----
Create VIEW gold.sales
as 
Select * from OPENROWSET
( BULK 'https://pharmastoragedatalake01.blob.core.windows.net/silver/Sales/',
FORMAT = 'PARQUET') as Q3


-----Reports--------
Create VIEW gold.reports
as 
Select * from OPENROWSET
( BULK 'https://pharmastoragedatalake01.blob.core.windows.net/silver/Reports/',
FORMAT = 'PARQUET') as Q4
