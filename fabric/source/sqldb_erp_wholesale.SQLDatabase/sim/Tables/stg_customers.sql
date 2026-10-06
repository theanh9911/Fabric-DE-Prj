CREATE TABLE [sim].[stg_customers] (
    [customer_code] NVARCHAR (20)  NOT NULL,
    [customer_name] NVARCHAR (200) NULL,
    [country]       NVARCHAR (100) NULL,
    [city]          NVARCHAR (100) NULL,
    [gender]        NVARCHAR (20)  NULL,
    [address]       NVARCHAR (400) NULL,
    [created_at]    DATETIME2 (0)  NULL,
    [updated_at]    DATETIME2 (0)  NULL
);


GO

