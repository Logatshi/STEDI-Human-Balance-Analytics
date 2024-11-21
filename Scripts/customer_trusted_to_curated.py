import sys
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load Data
customer_trusted = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="customer_trusted")
accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="accelerometer_trusted")

# Join Tables
joined_data = Join.apply(customer_trusted, accelerometer_trusted, 'email', 'user')

# Write Data with Schema Evolution Enabled
S3bucket_node = glueContext.getSink(
    path="s3://stedibucket/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node"
)
S3bucket_node.setFormat("json")
S3bucket_node.writeFrame(joined_data)

job.commit()