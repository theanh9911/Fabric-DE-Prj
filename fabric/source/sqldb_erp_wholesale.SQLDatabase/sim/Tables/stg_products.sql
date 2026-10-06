CREATE TABLE [sim].[stg_products] (
    [product_code]  NVARCHAR (20)  NOT NULL,
    [product_name]  NVARCHAR (200) NULL,
    [brand]         NVARCHAR (100) NULL,
    [category_code] NVARCHAR (20)  NULL,
    [created_at]    DATETIME2 (0)  NULL,
    [updated_at]    DATETIME2 (0)  NULL
);


GO

