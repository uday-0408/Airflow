from airflow.sdk import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id="simple_etl",
    schedule="@daily",
    catchup=False,
    tags=["Assignment-1", "simple_etl", "Uday"]
)
def simple_etl():
    @task()
    def extract(**context):
        products=[
            {"product_id": 1, "name": "Product A", "price": 10.0},
            {"product_id": 2, "name": "Product B", "price": 20.0},
            {"product_id": 3, "name": "Product C", "price": 30.0},
            {"product_id": 4, "name": "Product D", "price": 40.0},
        ]
        print(f"Extracting sales data for date: {context['ds']}")
        for i in products:
            print(f"Product ID: {i['product_id']}, Name: {i['name']}, Price: {i['price']}")
        return products
    @task()
    def validate(products):
        print("Validating data...")
        print(f"Validation passed — {len(products)} records found")
        return products
    @task()
    def transform(products):
        print("Transforming data...")
        print("Applying 10% discount to all records")
        print("Transformed data:")
        for i in products:
            i['price'] = i['price'] * 0.9
            print(f"Product ID: {i['product_id']}, Name: {i['name']}, Discounted Price: {i['price']}")
        return products
    @task()
    def load(products, **context):
        print("Loading to warehouse...")
        print(f"Successfully loaded {len(products)} records on {context['ds']}")
        for i in products:
            print(f"Product ID: {i['product_id']}, Name: {i['name']}, Final Price: {i['price']}")
    data = extract()
    validated = validate(data)
    transformed = transform(validated)
    load(transformed)
simple_etl = simple_etl()