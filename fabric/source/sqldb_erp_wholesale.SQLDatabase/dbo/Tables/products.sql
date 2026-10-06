CREATE TABLE [dbo].[products] (
    [product_code]  NVARCHAR (20)  NOT NULL,
    [product_name]  NVARCHAR (200) NULL,
    [brand]         NVARCHAR (100) NULL,
    [category_code] NVARCHAR (20)  NULL,
    [created_at]    DATETIME2 (0)  CONSTRAINT [df_products_created_at] DEFAULT (sysutcdatetime()) NULL,
    [updated_at]    DATETIME2 (0)  CONSTRAINT [df_products_updated_at] DEFAULT (sysutcdatetime()) NULL,
    CONSTRAINT [pk_products] PRIMARY KEY CLUSTERED ([product_code] ASC)
);


GO

