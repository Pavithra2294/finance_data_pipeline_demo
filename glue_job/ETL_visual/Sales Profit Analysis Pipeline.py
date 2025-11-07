import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1762424479773 = glueContext.create_dynamic_frame.from_catalog(database="finance", table_name="extract_raw", transformation_ctx="AWSGlueDataCatalog_node1762424479773")

# Script generated for node Drop Fields
DropFields_node1762381784788 = DropFields.apply(frame=AWSGlueDataCatalog_node1762424479773, paths=["month number", "month name", "discount band"], transformation_ctx="DropFields_node1762381784788")

# Script generated for node Aggregate
Aggregate_node1762381913101 = sparkAggregate(glueContext, parentFrame = DropFields_node1762381784788, groups = ["country", "product"], aggs = [["sales", "sum"], ["profit", "sum"], ["profit", "avg"]], transformation_ctx = "Aggregate_node1762381913101")

# Script generated for node Rename Field
RenameField_node1762382064324 = RenameField.apply(frame=Aggregate_node1762381913101, old_name="`sum(sales)`", new_name="total_sales", transformation_ctx="RenameField_node1762382064324")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameField_node1762382064324, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1762383044115", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RenameField_node1762382064324.count() >= 1):
   RenameField_node1762382064324 = RenameField_node1762382064324.coalesce(1)
AmazonS3_node1762383641870 = glueContext.write_dynamic_frame.from_options(frame=RenameField_node1762382064324, connection_type="s3", format="glueparquet", connection_options={"path": "s3://pavithra-bucket-yml", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1762383641870")

job.commit()
