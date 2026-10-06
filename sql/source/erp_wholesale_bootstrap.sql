/* =====================================================================
   sqldb_erp_wholesale — bootstrap DDL (chạy 1 lần trong SQL query editor)

   Mô hình "hybrid" (ADR 008):
     - Ép những gì đề mô tả chắc chắn: kiểu timestamp (datetime2, UTC), PK, NOT NULL trên key.
     - Giữ nguyên giá trị bẩn của data mẫu (status, tax_rate, tên, mã...) để platform xử lý.
     - FK được KHAI BÁO nhưng NOCHECK (không enforce) → orphan vẫn vào được, như data mẫu.

   Sau khi workspace sync Git, SQL project trong fabric/source/sqldb_erp_wholesale.SQLDatabase/
   là nguồn sự thật → xoá file này (P4, P13).
   ===================================================================== */

-- ---------------------------------------------------------------------
-- Schema sim: bảng staging nội bộ của simulator (platform KHÔNG đọc)
-- ---------------------------------------------------------------------
CREATE SCHEMA sim;
GO

-- ---------------------------------------------------------------------
-- Master data
-- ---------------------------------------------------------------------
CREATE TABLE dbo.categories (
    category_code   NVARCHAR(20)  NOT NULL,
    category_lvl1   NVARCHAR(100) NULL,
    category_lvl2   NVARCHAR(100) NULL,
    category_lvl3   NVARCHAR(100) NULL,
    category_lvl4   NVARCHAR(100) NULL,
    created_at      DATETIME2(0)  NULL CONSTRAINT df_categories_created_at DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2(0)  NULL CONSTRAINT df_categories_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_categories PRIMARY KEY (category_code)
);

CREATE TABLE dbo.customers (
    customer_code   NVARCHAR(20)  NOT NULL,
    customer_name   NVARCHAR(200) NULL,
    country         NVARCHAR(100) NULL,
    city            NVARCHAR(100) NULL,
    gender          NVARCHAR(20)  NULL,
    address         NVARCHAR(400) NULL,
    created_at      DATETIME2(0)  NULL CONSTRAINT df_customers_created_at DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2(0)  NULL CONSTRAINT df_customers_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_customers PRIMARY KEY (customer_code)
);

CREATE TABLE dbo.products (
    product_code    NVARCHAR(20)  NOT NULL,
    product_name    NVARCHAR(200) NULL,
    brand           NVARCHAR(100) NULL,
    category_code   NVARCHAR(20)  NULL,
    created_at      DATETIME2(0)  NULL CONSTRAINT df_products_created_at DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2(0)  NULL CONSTRAINT df_products_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_products PRIMARY KEY (product_code)
);

CREATE TABLE dbo.sales_hierarchy (
    salesman_code       NVARCHAR(20)  NOT NULL,
    salesman_name       NVARCHAR(200) NULL,
    division            NVARCHAR(100) NULL,
    salesmanager_code   NVARCHAR(20)  NULL,
    salesmanager_name   NVARCHAR(200) NULL,
    position            NVARCHAR(50)  NULL,
    inserted_at         DATETIME2(0)  NULL CONSTRAINT df_sales_hierarchy_inserted_at DEFAULT SYSUTCDATETIME(),
    updated_at          DATETIME2(0)  NULL CONSTRAINT df_sales_hierarchy_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_sales_hierarchy PRIMARY KEY (salesman_code)
);

-- ---------------------------------------------------------------------
-- Orders: append 1 dòng mỗi lần đổi status (audit trail)
-- ---------------------------------------------------------------------
CREATE TABLE dbo.orders (
    order_line_id   BIGINT IDENTITY(1,1) NOT NULL,
    order_no        NVARCHAR(30)  NOT NULL,
    customer_code   NVARCHAR(20)  NULL,
    order_date      DATE          NULL,
    order_status    NVARCHAR(30)  NULL,
    product_code    NVARCHAR(20)  NULL,
    salesman_code   NVARCHAR(20)  NULL,
    quantity        DECIMAL(18,2) NULL,
    price           DECIMAL(18,2) NULL,
    tax_rate        NVARCHAR(20)  NULL,
    created_at      DATETIME2(0)  NULL CONSTRAINT df_orders_created_at DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2(0)  NOT NULL CONSTRAINT df_orders_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_orders PRIMARY KEY (order_line_id)
);

-- Incremental extract của platform lọc theo updated_at → cần index (như hệ thống thật)
CREATE INDEX ix_orders_updated_at ON dbo.orders (updated_at);
CREATE INDEX ix_orders_order_no   ON dbo.orders (order_no);

-- ---------------------------------------------------------------------
-- FK: khai báo quan hệ nhưng không enforce (hybrid)
-- ---------------------------------------------------------------------
ALTER TABLE dbo.products WITH NOCHECK
    ADD CONSTRAINT fk_products_categories FOREIGN KEY (category_code) REFERENCES dbo.categories (category_code);
ALTER TABLE dbo.orders WITH NOCHECK
    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_code) REFERENCES dbo.customers (customer_code);
ALTER TABLE dbo.orders WITH NOCHECK
    ADD CONSTRAINT fk_orders_products FOREIGN KEY (product_code) REFERENCES dbo.products (product_code);
ALTER TABLE dbo.orders WITH NOCHECK
    ADD CONSTRAINT fk_orders_sales_hierarchy FOREIGN KEY (salesman_code) REFERENCES dbo.sales_hierarchy (salesman_code);

ALTER TABLE dbo.sales_hierarchy WITH NOCHECK
    ADD CONSTRAINT fk_sales_hierarchy_manager FOREIGN KEY (salesmanager_code) REFERENCES dbo.sales_hierarchy (salesman_code);

ALTER TABLE dbo.products NOCHECK CONSTRAINT fk_products_categories;
ALTER TABLE dbo.sales_hierarchy NOCHECK CONSTRAINT fk_sales_hierarchy_manager;
ALTER TABLE dbo.orders   NOCHECK CONSTRAINT fk_orders_customers;
ALTER TABLE dbo.orders   NOCHECK CONSTRAINT fk_orders_products;
ALTER TABLE dbo.orders   NOCHECK CONSTRAINT fk_orders_sales_hierarchy;
GO

-- ---------------------------------------------------------------------
-- Staging của simulator: cùng cột với dbo, không constraint
-- (notebook ghi vào đây bằng JDBC rồi MERGE / INSERT sang dbo trong 1 transaction)
-- ---------------------------------------------------------------------
SELECT TOP 0 * INTO sim.stg_categories      FROM dbo.categories;
SELECT TOP 0 * INTO sim.stg_customers       FROM dbo.customers;
SELECT TOP 0 * INTO sim.stg_products        FROM dbo.products;
SELECT TOP 0 * INTO sim.stg_sales_hierarchy FROM dbo.sales_hierarchy;

SELECT TOP 0 order_no, customer_code, order_date, order_status, product_code, salesman_code,
             quantity, price, tax_rate, created_at, updated_at
INTO sim.stg_orders
FROM dbo.orders;
GO
