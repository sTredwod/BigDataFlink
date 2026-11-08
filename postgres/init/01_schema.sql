CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key VARCHAR(32) PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT NOT NULL,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_key VARCHAR(32) PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT NOT NULL,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key VARCHAR(32) PRIMARY KEY,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(12, 2),
    stock_quantity INTEGER,
    product_weight NUMERIC(12, 3),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(4, 2),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key VARCHAR(32) PRIMARY KEY,
    store_name TEXT,
    store_location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key VARCHAR(32) PRIMARY KEY,
    supplier_name TEXT,
    contact_name TEXT,
    email TEXT NOT NULL,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    sale_date DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    event_id TEXT PRIMARY KEY,
    original_id INTEGER,
    customer_key VARCHAR(32) NOT NULL REFERENCES dim_customer(customer_key),
    seller_key VARCHAR(32) NOT NULL REFERENCES dim_seller(seller_key),
    product_key VARCHAR(32) NOT NULL REFERENCES dim_product(product_key),
    store_key VARCHAR(32) NOT NULL REFERENCES dim_store(store_key),
    supplier_key VARCHAR(32) NOT NULL REFERENCES dim_supplier(supplier_key),
    sale_date DATE NOT NULL REFERENCES dim_date(sale_date),
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12, 2),
    source_file TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_seller ON fact_sales(seller_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store ON fact_sales(store_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_supplier ON fact_sales(supplier_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(sale_date);
