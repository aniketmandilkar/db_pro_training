# Bronze Layer: Raw Ingestion
import pandas as pd

# Read from GitHub using pandas
pdf = pd.read_csv("https://raw.githubusercontent.com/a-forty-two/stackroute-08May-Data_Engineering-Batch6/main/data.csv")

# Convert to Spark DataFrame
bronze_df = spark.createDataFrame(pdf)

# Create temp view for inspection
bronze_df.createOrReplaceTempView("bronze_data").display()

# Silver Layer: Data Cleaning & Transformation
from pyspark.sql.functions import col

#Dropping the unwanted column
silver_df = bronze_df.drop("Unnamed: 32")

# Renaming columns with spaces to use underscores
for col_name in silver_df.columns:
    if " " in col_name:
        silver_df = silver_df.withColumnRenamed(col_name, col_name.replace(" ", "_"))

# Saving as a temp view for inspection
silver_df.createOrReplaceTempView("silver_data").display()

# Gold Layer: Business-Level Aggregates
# Count of records per diagnosis
gold_counts = silver_df.groupBy("diagnosis").count()
gold_counts.createOrReplaceTempView("gold_diagnosis_counts")

# Average metrics per diagnosis
gold_aggr = (
    silver_df.groupBy("diagnosis")
    .agg(
        {"radius_mean": "avg",
         "texture_mean": "avg",
         "area_mean": "avg",
         "perimeter_mean": "avg"}
    )
    .withColumnRenamed("avg(radius_mean)", "avg_radius_mean")
    .withColumnRenamed("avg(texture_mean)", "avg_texture_mean")
    .withColumnRenamed("avg(area_mean)", "avg_area_mean")
    .withColumnRenamed("avg(perimeter_mean)", "avg_perimeter_mean")
)
gold_aggr.createOrReplaceTempView("gold_diagnosis_aggregates")

# Display both
print("Diagnosis Counts:")
display(gold_counts)

print("Diagnosis Aggregates:").display()
