CREATE TABLE [sim].[stg_categories] (
    [category_code] NVARCHAR (20)  NOT NULL,
    [category_lvl1] NVARCHAR (100) NULL,
    [category_lvl2] NVARCHAR (100) NULL,
    [category_lvl3] NVARCHAR (100) NULL,
    [category_lvl4] NVARCHAR (100) NULL,
    [created_at]    DATETIME2 (0)  NULL,
    [updated_at]    DATETIME2 (0)  NULL
);


GO

