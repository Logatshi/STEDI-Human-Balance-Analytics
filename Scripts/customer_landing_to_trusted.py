import sys
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load customer_landing data
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2", 
    table_name="customer_landing"
)

# Filter by shareWithResearchAsOfDate
filtered_df = customer_landing.toDF().filter("shareWithResearchAsOfDate IS NOT NULL")

# Convert back to DynamicFrame
filtered_dynamic_frame = DynamicFrame.fromDF(filtered_df, glueContext, "filtered_df")

# Write Data with Schema Evolution Enabled
S3bucket_node = glueContext.getSink(
    path="s3://stedibucket/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node"
)
S3bucket_node.setFormat("json")
S3bucket_node.writeFrame(filtered_dynamic_frame)

job.commit()