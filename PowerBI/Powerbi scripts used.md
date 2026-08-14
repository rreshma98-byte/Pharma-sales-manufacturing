**DIM customer table---**

DimCustomer =

DISTINCT(

&#x20;   SELECTCOLUMNS(

&#x20;       'gold exthosp',

&#x20;       "Customer\_ID", 'gold exthosp'\[Customer\_ID]

&#x20;   )

)



**Dim Product table----**

DimProduct =

DISTINCT(

&#x20;   UNION(

&#x20;       SELECTCOLUMNS(

&#x20;           'gold extsales',

&#x20;           "Product\_ID", 'gold extsales'\[Product\_ID]

&#x20;       ),

&#x20;       SELECTCOLUMNS(

&#x20;           'gold extinventory',

&#x20;           "Product\_ID", 'gold extinventory'\[Product\_ID]

&#x20;       ),

&#x20;       SELECTCOLUMNS(

&#x20;           'gold extreports',

&#x20;           "Product\_ID", 'gold extreports'\[Product\_ID]

&#x20;       )

&#x20;   )

)



**Dime Date----**



DimDate =

CALENDAR(

&#x20;   MIN('gold extsales'\[Sale\_Date]),

&#x20;   MAX('gold extsales'\[Sale\_Date])

)





**Measures Used**

========================================

GOLD EXTSALES — MEASURES

========================================

Total Sales =

SUM('gold extsales'\[Calculated\_Sales])



Total Units Sold =

SUM('gold extsales'\[Quantity\_Sold])



Total Orders =

DISTINCTCOUNT('gold extsales'\[Sale\_ID])



Average Order Value =

DIVIDE(

&#x20;   \[Total Sales],

&#x20;   \[Total Orders]

)

Average Discount % =

AVERAGE('gold extsales'\[Discount\_Percent])



Sales YTD =

TOTALYTD(

&#x20;   \[Total Sales],

&#x20;   DimDate\[Date]

)



Previous Month Sales =

CALCULATE(

&#x20;   \[Total Sales],

&#x20;   DATEADD(

&#x20;       DimDate\[Date],

&#x20;       -1,

&#x20;       MONTH

&#x20;   )

)



MoM Growth % =

DIVIDE(

&#x20;   \[Total Sales] - \[Previous Month Sales],

&#x20;   \[Previous Month Sales]

)



Product Sales Rank =

RANKX(

&#x20;   ALL(DimProduct\[Product\_ID]),

&#x20;   \[Total Sales],

&#x20;   ,

&#x20;   DESC,

&#x20;   DENSE

)



========================================

GOLD EXHOSP — MEASURES

========================================



Total Customers =

DISTINCTCOUNT('gold exhosp'\[Customer\_ID])



Total Hospitals =

DISTINCTCOUNT('gold exhosp'\[Hospital\_Name])



Total Doctors =

DISTINCTCOUNT('gold exhosp'\[Doctor\_Name])



Total Cities =

DISTINCTCOUNT('gold exhosp'\[City])



Total States =

DISTINCTCOUNT('gold exhosp'\[State])



Total Regions =

DISTINCTCOUNT('gold exhosp'\[Region])

========================================

GOLD EXTINVENTORY — MEASURES

========================================



Total Opening Stock =

SUM('gold extinventory'\[Opening\_Stock])



Total Production =

SUM('gold extinventory'\[Production\_Qty])



Inventory Quantity Sold =

SUM('gold extinventory'\[Quantity\_Sold])



Total Closing Stock =

SUM('gold extinventory'\[Closing\_Stock])



Inventory Records =

DISTINCTCOUNT('gold extinventory'\[Inventory\_ID])



Stock Utilization % =

DIVIDE(

&#x20;   \[Inventory Quantity Sold],

&#x20;   \[Total Production]

)



Closing Stock % =

DIVIDE(

&#x20;   \[Total Closing Stock],

&#x20;   \[Total Opening Stock] + \[Total Production]

)



========================================

GOLD EXTREPORTS — MEASURES

========================================



Total Inspections =

DISTINCTCOUNT('gold extreports'\[Quality\_ID])



Total Rejection Qty =

SUM('gold extreports'\[Rejection\_Qty])



Rejected Records =

CALCULATE(

&#x20;   \[Total Inspections],

&#x20;   'gold extreports'\[Rejection\_Status] = "Rejected"

)

Rejection Rate % =

DIVIDE(

&#x20;   \[Rejected Records],

&#x20;   \[Total Inspections]

)



Failed Inspections =

CALCULATE(

&#x20;   \[Total Inspections],

&#x20;   'gold extreports'\[Quality\_Status] = "Failed"

)



Quality Pass Rate % =

DIVIDE(

&#x20;   \[Total Inspections] - \[Failed Inspections],

&#x20;   \[Total Inspections]

)



Defect Rate % =

DIVIDE(

&#x20;   \[Failed Inspections],

&#x20;   \[Total Inspections]

)



Average Test Result =

AVERAGE('gold extreports'\[Test\_Result])



Avg Rejection per Inspection =

DIVIDE(

&#x20;   \[Total Rejection Qty],

&#x20;   \[Total Inspections]

)

