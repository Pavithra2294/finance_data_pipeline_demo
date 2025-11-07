from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
import sys

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from Glue catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="finance",
    table_name="extract_raw",
    format_options={"withHeader": True}

)

# Convert to DataFrame
df = dyf.toDF()
print("Columns:", df.columns)
df = df.select([F.col(c).alias(c.lower()) for c in df.columns])
df = df.dropna(subset=['Sales', 'Country', 'Segment', 'Product'])
df = df.dropDuplicates()
# Drop unnecessary columns if present
columns_to_drop = ["Month Number", "Month Name", "Discount Band"]
for c in columns_to_drop:
    if c in df.columns:
        df = df.drop(c)

# Convert Date column to proper date format
if "Date" in df.columns:
    df = df.withColumn("Date", F.to_date(F.col("Date"), "dd/MM/yyyy"))

# Filter rows where Profit > 0
if "Profit" in df.columns:
    df = df.filter(F.col("Profit") > 0)

# Add new column: Profit Margin = Profit / Sales
#if all(col in df.columns for col in ["Profit", "Sales"]):
   # df = df.withColumn("ProfitMargin", F.round(F.col("Profit") / F.col("Sales"), 4))

# Aggregate
agg_df = df.groupBy("Country", "Segment", "Product").agg(
    F.sum("Sales").alias("TotalSales"),
    F.sum("Profit").alias("TotalProfit"),
   # F.avg("ProfitMargin").alias("AvgProfitMargin")
)

# Convert back to DynamicFrame
result_dyf = DynamicFrame.fromDF(agg_df, glueContext, "result_dyf")

# Write output to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=result_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://finance-datapav/destination-refined/",
        "partitionKeys": ["Country", "Segment"]
    },
    format="parquet"
)

job.commit()
