from multiprocessing import freeze_support
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql import SparkSession
import warnings

# Remove additional spaces in name
def remove_extra_spaces(df, column_name):
    # Remove extra spaces from the specified column
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))

    return df_transformed

def main():
    warnings.filterwarnings("ignore")

    spark = SparkSession.builder \
        .remote("sc://localhost:15002") \
        .appName("GDELTAnalyzer") \
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
        ) \
        .config(
            "spark.mongodb.write.connection.uri",
            "mongodb://mongodb:27017/local.processed_results"
        ) \
        .config(
            "spark.mongodb.read.connection.uri",
            "mongodb://mongodb:27017/local.processed_results"
        ) \
        .getOrCreate()

    print(spark.version)

    sample_data = [{"name": "John    D.", "age": 30},
    {"name": "Alice   G.", "age": 25},
    {"name": "Bob  T.", "age": 35},
    {"name": "Eve   A.", "age": 28}]
    
    df = spark.createDataFrame(sample_data)
    
    transformed_df = remove_extra_spaces(df, "name")
    
    transformed_df.show()
    
    transformed_df.write \
        .format("mongodb") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    freeze_support()
    main()