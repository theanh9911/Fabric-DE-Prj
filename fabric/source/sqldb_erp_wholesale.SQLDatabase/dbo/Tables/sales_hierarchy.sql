CREATE TABLE [dbo].[sales_hierarchy] (
    [salesman_code]     NVARCHAR (20)  NOT NULL,
    [salesman_name]     NVARCHAR (200) NULL,
    [division]          NVARCHAR (100) NULL,
    [salesmanager_code] NVARCHAR (20)  NULL,
    [salesmanager_name] NVARCHAR (200) NULL,
    [position]          NVARCHAR (50)  NULL,
    [inserted_at]       DATETIME2 (0)  CONSTRAINT [df_sales_hierarchy_inserted_at] DEFAULT (sysutcdatetime()) NULL,
    [updated_at]        DATETIME2 (0)  CONSTRAINT [df_sales_hierarchy_updated_at] DEFAULT (sysutcdatetime()) NULL,
    CONSTRAINT [pk_sales_hierarchy] PRIMARY KEY CLUSTERED ([salesman_code] ASC),
    CONSTRAINT [fk_sales_hierarchy_manager] FOREIGN KEY ([salesmanager_code]) REFERENCES [dbo].[sales_hierarchy] ([salesman_code])
);


GO
ALTER TABLE [dbo].[sales_hierarchy] NOCHECK CONSTRAINT [fk_sales_hierarchy_manager];


GO

