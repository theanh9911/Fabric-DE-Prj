CREATE TABLE [dbo].[categories] (
    [category_code] NVARCHAR (20)  NOT NULL,
    [category_lvl1] NVARCHAR (100) NULL,
    [category_lvl2] NVARCHAR (100) NULL,
    [category_lvl3] NVARCHAR (100) NULL,
    [category_lvl4] NVARCHAR (100) NULL,
    [created_at]    DATETIME2 (0)  CONSTRAINT [df_categories_created_at] DEFAULT (sysutcdatetime()) NULL,
    [updated_at]    DATETIME2 (0)  CONSTRAINT [df_categories_updated_at] DEFAULT (sysutcdatetime()) NULL,
    CONSTRAINT [pk_categories] PRIMARY KEY CLUSTERED ([category_code] ASC)
);


GO

