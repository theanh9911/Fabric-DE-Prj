CREATE TABLE [sim].[stg_sales_hierarchy] (
    [salesman_code]     NVARCHAR (20)  NOT NULL,
    [salesman_name]     NVARCHAR (200) NULL,
    [division]          NVARCHAR (100) NULL,
    [salesmanager_code] NVARCHAR (20)  NULL,
    [salesmanager_name] NVARCHAR (200) NULL,
    [position]          NVARCHAR (50)  NULL,
    [inserted_at]       DATETIME2 (0)  NULL,
    [updated_at]        DATETIME2 (0)  NULL
);


GO

