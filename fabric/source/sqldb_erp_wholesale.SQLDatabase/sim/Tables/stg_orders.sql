CREATE TABLE [sim].[stg_orders] (
    [order_no]      NVARCHAR (30)   NOT NULL,
    [customer_code] NVARCHAR (20)   NULL,
    [order_date]    DATE            NULL,
    [order_status]  NVARCHAR (30)   NULL,
    [product_code]  NVARCHAR (20)   NULL,
    [salesman_code] NVARCHAR (20)   NULL,
    [quantity]      DECIMAL (18, 2) NULL,
    [price]         DECIMAL (18, 2) NULL,
    [tax_rate]      NVARCHAR (20)   NULL,
    [created_at]    DATETIME2 (0)   NULL,
    [updated_at]    DATETIME2 (0)   NOT NULL
);


GO

