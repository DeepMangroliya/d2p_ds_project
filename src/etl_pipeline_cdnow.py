# etl_pipeline_cdnow.py (MySQL version with Faker-generated customer and product tables and CLI support)

import pandas as pd
# import boto3
from src.utils import get_data

# Step 1: Load CDNOW dataset
# cdnow = pd.read_csv("data/original_data.csv", sep=',', names=['customer_id', 'date', 'quantity', 'product_id', 'price', 'product_category', 'country'], header=0)
cdnow = get_data('data/original_data.csv')
# print(cdnow)
# Create dales table - includes original customer_id
sales_data = pd.DataFrame({
    'customer_id': cdnow['customer_id'],
    'country': cdnow['country'],
    'date':cdnow['date'],
    'product_id': cdnow['product_id']
})

# Create product table - includes sample price from original data
# sample_prices = random.choices(cdnow['price'], k=num_products)
product_data = pd.DataFrame({
    'product_id': cdnow['product_id'],
    'price': cdnow['price'],
    'category': cdnow['product_category']
})

#Converting the DataFrame to csv files
sales_data.to_csv('data/sales.csv', index=False)
product_data.to_csv('data/products.csv', index=False)

