import os
from pathlib import Path
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum, avg, month, year, dense_rank, row_number
from pyspark.sql.window import Window
from dotenv import load_dotenv

# Load environment variables
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

# Configuration
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
spark_host = "spark://dibimbing-dataeng-spark-master:7077"

# Initialize Spark
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
    pyspark.SparkConf()
    .setAppName('Olist ETL Analysis')
    .setMaster(spark_host)
    .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
))
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# PostgreSQL connection properties
jdbc_url = f'jdbc:postgresql://{postgres_host}:5432/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

def create_tables_if_not_exists():
    """Create required PostgreSQL tables if they don't exist"""
    from pyspark.sql import SQLContext
    sqlContext = SQLContext(sparkcontext)
    
    # Create orders table
    orders_schema = """
    order_id STRING, customer_id STRING, order_status STRING, 
    order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
    """
    
    # Execute table creation
    create_table_query = """
    CREATE TABLE IF NOT EXISTS olist_orders (
        order_id VARCHAR(255),
        customer_id VARCHAR(255),
        order_status VARCHAR(50),
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP
    )
    """
    
    df = spark.read.jdbc(
        jdbc_url,
        '(SELECT 1) AS dummy',
        properties=jdbc_properties
    )
    df._jdf.sparkSession().sql(create_table_query)

def load_and_transform_data():
    """Load data from CSV and perform transformations"""
    # Read orders data
    orders_df = spark.read.csv(
        "/data/olist/olist_orders_dataset.csv",
        header=True,
        inferSchema=True
    )
    
    # Convert timestamp columns
    timestamp_columns = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    
    for col_name in timestamp_columns:
        orders_df = orders_df.withColumn(
            col_name,
            col(col_name).cast('timestamp')
        )
    
    return orders_df

def perform_analysis(df: DataFrame):
    """Perform various analyses on the data"""
    # Monthly order analysis
    monthly_orders = df.withColumn(
        'order_month',
        month('order_purchase_timestamp')
    ).withColumn(
        'order_year',
        year('order_purchase_timestamp')
    ).groupBy('order_year', 'order_month').agg(
        count('order_id').alias('total_orders'),
        count('order_delivered_customer_date').alias('delivered_orders')
    )
    
    # Calculate delivery performance
    delivery_performance = df.withColumn(
        'delivery_time_days',
        (col('order_delivered_customer_date').cast('long') - 
         col('order_purchase_timestamp').cast('long')) / (24 * 60 * 60)
    ).agg(
        avg('delivery_time_days').alias('avg_delivery_days'),
        count(when(col('order_status') == 'delivered', True)).alias('successful_deliveries'),
        count(when(col('order_status') == 'canceled', True)).alias('canceled_orders')
    )
    
    return monthly_orders, delivery_performance

def save_results(monthly_orders: DataFrame, delivery_performance: DataFrame):
    """Save analysis results to PostgreSQL"""
    # Save monthly orders analysis
    (monthly_orders
     .write
     .mode("overwrite")
     .jdbc(
         jdbc_url,
         'public.olist_monthly_orders',
         properties=jdbc_properties
     ))
    
    # Save delivery performance analysis
    (delivery_performance
     .write
     .mode("overwrite")
     .jdbc(
         jdbc_url,
         'public.olist_delivery_performance',
         properties=jdbc_properties
     ))
    
    # Print results
    print("Monthly Orders Analysis:")
    monthly_orders.show()
    
    print("\nDelivery Performance Analysis:")
    delivery_performance.show()

def main():
    try:
        print("Starting ETL and analysis process...")
        
        # Create tables if they don't exist
        create_tables_if_not_exists()
        
        # Load and transform data
        orders_df = load_and_transform_data()
        
        # Save transformed data
        (orders_df
         .write
         .mode("overwrite")
         .jdbc(
             jdbc_url,
             'public.olist_orders',
             properties=jdbc_properties
         ))
        
        # Perform analysis
        monthly_orders, delivery_performance = perform_analysis(orders_df)
        
        # Save and display results
        save_results(monthly_orders, delivery_performance)
        
        print("ETL and analysis process completed successfully!")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()