import numpy as np
import pandas as pd
from faker import Faker
from xgboost import XGBClassifier, XGBRegressor

from src.utils import gcp_feed_data, read_file_s3

fake = Faker()

#modeling function for gathering insights
def modeling(original_data: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a CLV model using synthetic customer and product data, 
    then trains regression and classification models to predict 
    customer spending and likelihood of purchase.

    Parameters:
        original_data : pd.DataFrame (raw sales data)

    Returns:
        pd.DataFrame: DataFrame with customer features, predicted spend, and purchase probability.
    """
    
    customer_data = original_data[['customer_id']]
    customer_data.drop_duplicates(inplace=True)

    #creating fake column which will include the country of customer
    countries = []

    for i in range(25):
        countries.append(fake.country())

        countries = list(set(countries))

    p = np.random.uniform(0, 0.99999, len(countries))
    p = p/sum(p)
    customer_data['country'] = np.random.choice(countries, size=len(customer_data), p=p)

    customer_data['country'].value_counts().reset_index()

    #Product Data - Creating product dataframe with fake data -> product_id, product_category, price(from original dataframe)
    original_data['product_id'] = np.random.randint(10000, 1909221900, len(original_data))
    product_data = original_data[['product_id', 'price']]
    original_data.drop(['price'], axis=1, inplace=True)

    #fake list to create product's category columns
    products = ['fruit', 'vegetables', 'refrigerated items', 'frozen', 'spices and herbs', 'canned foods', 
                'packaged foods', 'condiments and sauces', 'beverages', 'dairy', 'cheese', 'meat', 'seafood', 
                'baked goods', 'baking', 'snacks', 'baby products', 'pets', 'personal care', 'medicine', 'kitchen', 
                'cleaning products']
    
    p = np.random.uniform(0, 0.99999, len(products))
    p=p/sum(p)
    product_data["product_category"] = np.random.choice(products, size=len(product_data), p=p)

    product_data['product_category'].value_counts().reset_index()

    #Merge Product, customer data into original data

    original_data = original_data.merge(product_data, on='product_id', how='left')
    original_data = original_data.merge(customer_data, on='customer_id', how='left')

    #CLV
    original_data['date'] = pd.to_datetime(original_data['date'].astype(str))

    """##Finding First purchase of customer"""

    first_purchases = original_data.sort_values(by=['customer_id','date']).groupby('customer_id').first()

    #ML : feature eng

    n_days=60
    max_date = original_data['date'].max()
    cutoff_date = max_date - pd.to_timedelta(n_days, unit='d')

    historical_data = original_data[original_data['date']<=cutoff_date]
    future_data = original_data[original_data['date']>cutoff_date]

    #Targets DataFrame for ML Modeling

    targets_df = future_data.drop(['quantity'], axis=1)

    targets_df.drop(['date'], axis=1, inplace=True)
    targets_df = targets_df.groupby('customer_id').sum().rename({'price':'spend_60_day'}, axis=1)
    targets_df['spend_60_flag'] = 1

    targets_df.drop(['product_category', 'product_id', 'country'], axis=1, inplace=True)

    #Recency

    max_date = historical_data['date'].max()

    recency_df = historical_data[['customer_id', 'date']].groupby('customer_id').apply(lambda x: (x['date'].max() - max_date) / pd.to_timedelta(1, "day"))
    recency_df = recency_df.to_frame(name='recency')

    #Frequency

    frequency_df = historical_data[['customer_id', 'date']].groupby('customer_id').count().set_axis(['frequency'],axis=1)
    # frequency_df = frequency_df.rename(columns = {"date":"frequency"})

    #Overall Price and Price Mean

    price_df = historical_data[['customer_id','price']].groupby('customer_id').agg({
        'price':['sum','mean']
    }).set_axis({"price_sum","price_mean"},axis=1)

    features_df = pd.concat([recency_df,frequency_df,price_df], axis=1)

    features_df = pd.merge(features_df, targets_df, left_index=True, right_index=True, how="left").fillna(0)

    #ML: Modelling

    #Regression

    X= features_df[['recency','frequency','price_sum','price_mean']]
    y= features_df[['spend_60_day','spend_60_flag']]

    xgbr = XGBRegressor(verbosity=0, random_state=42)

    xgbr.fit(X,y)

    score = xgbr.score(X, y)
    print("Training score: ", score)

    predictions = xgbr.predict(X)

    #Classification

    y_prob = features_df['spend_60_flag']

    xgb_classification = XGBClassifier(
        objective    = "reg:squarederror",
        random_state = 123
    )

    xgb_classification.fit(X, y_prob)
    score = xgb_classification.score(X, y_prob)

    predictions_prob = xgb_classification.predict_proba(X)

    predictions_df = pd.concat([
            pd.DataFrame(predictions)[[1]].set_axis(['pred_spend'], axis=1),
            pd.DataFrame(predictions_prob)[[1]].set_axis(['pred_prob'], axis=1),
            features_df.reset_index()], axis=1)

    predictions_df = predictions_df.merge(customer_data, on='customer_id', how='left')

    predictions_df.to_csv('predictions.csv', index=False)

    return predictions_df


def process() -> None:
    """
    Loads data from S3, runs the CLV modeling pipeline, 
    prints results, and uploads them to Google Sheets.

    Returns: 
        None
    """
    
    s3_bucket = "d2p.testing.bucket"
    df = read_file_s3(bucket=s3_bucket, object_name='clv_data.csv')
    results_df = modeling(df)
    print(results_df)
    
    #push the processed data to google sheets
    gcp_feed_data(spreadsheet_id='1h9V1yHMFfzS-CYz31xN4jzDUoWTaKOrCM2zuIsJykes', worksheet_name='sales', df=results_df)