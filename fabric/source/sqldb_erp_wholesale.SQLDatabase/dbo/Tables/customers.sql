CREATE TABLE [dbo].[customers] (
    [customer_code] NVARCHAR (20)  NOT NULL,
    [customer_name] NVARCHAR (200) NULL,
    [country]       NVARCHAR (100) NULL,
    [city]          NVARCHAR (100) NULL,
    [gender]        NVARCHAR (20)  NULL,
    [address]       NVARCHAR (400) NULL,
    [created_at]    DATETIME2 (0)  CONSTRAINT [df_customers_created_at] DEFAULT (sysutcdatetime()) NULL,
    [updated_at]    DATETIME2 (0)  CONSTRAINT [df_customers_updated_at] DEFAULT (sysutcdatetime()) NULL,
    CONSTRAINT [pk_customers] PRIMARY KEY CLUSTERED ([customer_code] ASC)
);


GO

