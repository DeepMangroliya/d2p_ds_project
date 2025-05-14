SELECT 
    s.customer_id,
    s.country,
    s.date
FROM 
    processed_db.sales s
JOIN 
    processed_db.products p ON s.product_id = p.product_id
JOIN (
    SELECT 
        customer_id, 
        MAX(date) AS max_date
    FROM 
        processed_db.sales
    GROUP BY 
        customer_id
) latest_sales
ON s.customer_id = latest_sales.customer_id
   AND s.date = latest_sales.max_date
ORDER BY 
    s.date DESC;
q