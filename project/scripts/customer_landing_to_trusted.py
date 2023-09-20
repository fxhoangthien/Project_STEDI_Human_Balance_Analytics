import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-glue-spark-bucket/project/customers/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node",
)

# Script generated for node ApplyMapping
ApplyMapping = Filter.apply(
    frame=S3bucket_node,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ApplyMapping",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping,
    database="project",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog",
)

job.commit()