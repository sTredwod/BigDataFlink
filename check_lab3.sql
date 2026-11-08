-- Expected after the producer has sent all 10,000 rows and Flink has drained the topic.
SELECT 'fact_sales' AS table_name, COUNT(*) AS rows_count FROM fact_sales
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
ORDER BY table_name;

-- Check that each fact has all required dimension links.
SELECT
    COUNT(*) AS fact_rows,
    COUNT(c.customer_key) AS customer_matches,
    COUNT(s.seller_key) AS seller_matches,
    COUNT(p.product_key) AS product_matches,
    COUNT(st.store_key) AS store_matches,
    COUNT(su.supplier_key) AS supplier_matches,
    COUNT(d.sale_date) AS date_matches
FROM fact_sales f
LEFT JOIN dim_customer c ON c.customer_key = f.customer_key
LEFT JOIN dim_seller s ON s.seller_key = f.seller_key
LEFT JOIN dim_product p ON p.product_key = f.product_key
LEFT JOIN dim_store st ON st.store_key = f.store_key
LEFT JOIN dim_supplier su ON su.supplier_key = f.supplier_key
LEFT JOIN dim_date d ON d.sale_date = f.sale_date;

-- Example analytical query: monthly sales.
SELECT
    d.year,
    d.month,
    COUNT(*) AS sales_count,
    SUM(f.sale_total_price) AS revenue
FROM fact_sales f
JOIN dim_date d ON d.sale_date = f.sale_date
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
