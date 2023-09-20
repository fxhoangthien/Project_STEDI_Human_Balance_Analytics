import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
DropFieldsCustomerCurated = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="customer_curated",
    transformation_ctx="DropFieldsCustomerCurated",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-glue-spark-bucket/project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding",
)

# Script generated for node Join Customer and Step Trainer
StepTrainerTrustJoinCustomerandStepTrainer = Join.apply(
    frame1=StepTrainerLanding,
    frame2=DropFieldsCustomerCurated,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="StepTrainerTrustJoinCustomerandStepTrainer",
)

# Script generated for node Drop Fields
DropFields = DropFields.apply(
    frame=StepTrainerTrustJoinCustomerandStepTrainer,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields,
    database="project",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted",
)

job.commit()