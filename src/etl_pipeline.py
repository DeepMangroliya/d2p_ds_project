import pandas as pd
from utils import get_data

# Loading CDNOW dataset
cdnow = get_data('data/original_data.csv')

# Create sales table - includes original customer_id
sales_data = pd.DataFrame({
    'customer_id': cdnow['customer_id'],
    'country': cdnow['country'],
    'date':cdnow['date'],
    'product_id': cdnow['product_id']
})

# Creating product table - includes sample price from original data
product_data = pd.DataFrame({
    'product_id': cdnow['product_id'],
    'quantity': cdnow['quantity'],
    'price': cdnow['price'],
    'category': cdnow['product_category']
})

#Converting the DataFrame to csv files
sales_data.to_csv('data/sales.csv', index=False)
product_data.to_csv('data/products.csv', index=False)
