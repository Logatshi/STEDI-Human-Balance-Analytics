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
customer_curated = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="customer_curated")
step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="step_trainer_landing")

# Join by Serial Number
joined_data = Join.apply(step_trainer_landing, customer_curated, 'serialNumber', 'serialNumber')

# Write Data with Schema Evolution Enabled
S3bucket_node = glueContext.getSink(
    path="s3://stedibucket/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node"
)
S3bucket_node.setFormat("json")
S3bucket_node.writeFrame(joined_data)

job.commit()