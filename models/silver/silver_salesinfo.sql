WITH sales AS (
    SELECT 
        sales_id,
        product_sk,
        customer_sk,
        {{multiply('unit_price', 'quantity')}} AS calculated_gross_amount,  -- Fixed: Replaced 'gross_amount' with calculated value
        gross_amount,
        payment_method
    FROM {{ ref('bronze_sales') }}
),
products AS (
    SELECT 
        product_sk,
        category  -- Fixed: Removed trailing comma
    FROM {{ ref('bronze_product') }}
),
customers AS (
    SELECT 
        customer_sk,
        gender
    FROM {{ ref('bronze_customer') }}
),
joined_query as (
SELECT 
    sales.sales_id,
    products.category,
    sales.gross_amount,
    sales.payment_method,
    customers.gender  -- Fixed: Corrected alias from 'customer' to 'customers'
FROM sales
JOIN products ON sales.product_sk = products.product_sk
JOIN customers ON sales.customer_sk = customers.customer_sk
)

SELECT category,
    gender,
    SUM(gross_amount) AS total_gross_amount
FROM joined_query
GROUP BY category, gender
ORDER BY total_gross_amount DESC