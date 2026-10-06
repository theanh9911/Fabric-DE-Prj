CREATE TABLE [dbo].[orders] (
    [order_line_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [order_no]      NVARCHAR (30)   NOT NULL,
    [customer_code] NVARCHAR (20)   NULL,
    [order_date]    DATE            NULL,
    [order_status]  NVARCHAR (30)   NULL,
    [product_code]  NVARCHAR (20)   NULL,
    [salesman_code] NVARCHAR (20)   NULL,
    [quantity]      DECIMAL (18, 2) NULL,
    [price]         DECIMAL (18, 2) NULL,
    [tax_rate]      NVARCHAR (20)   NULL,
    [created_at]    DATETIME2 (0)   CONSTRAINT [df_orders_created_at] DEFAULT (sysutcdatetime()) NULL,
    [updated_at]    DATETIME2 (0)   CONSTRAINT [df_orders_updated_at] DEFAULT (sysutcdatetime()) NOT NULL,
    CONSTRAINT [pk_orders] PRIMARY KEY CLUSTERED ([order_line_id] ASC),
    CONSTRAINT [fk_orders_customers] FOREIGN KEY ([customer_code]) REFERENCES [dbo].[customers] ([customer_code]),
    CONSTRAINT [fk_orders_products] FOREIGN KEY ([product_code]) REFERENCES [dbo].[products] ([product_code]),
    CONSTRAINT [fk_orders_sales_hierarchy] FOREIGN KEY ([salesman_code]) REFERENCES [dbo].[sales_hierarchy] ([salesman_code])
);


GO
ALTER TABLE [dbo].[orders] NOCHECK CONSTRAINT [fk_orders_customers];


GO
ALTER TABLE [dbo].[orders] NOCHECK CONSTRAINT [fk_orders_products];


GO
ALTER TABLE [dbo].[orders] NOCHECK CONSTRAINT [fk_orders_sales_hierarchy];


GO

CREATE NONCLUSTERED INDEX [ix_orders_order_no]
    ON [dbo].[orders]([order_no] ASC);


GO

CREATE NONCLUSTERED INDEX [ix_orders_updated_at]
    ON [dbo].[orders]([updated_at] ASC);


GO

