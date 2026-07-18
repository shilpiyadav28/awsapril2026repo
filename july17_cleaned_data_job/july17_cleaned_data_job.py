import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="july17_database",
    table_name="july17_tableraw_layer"
)

df = dynamic_frame.toDF()

df.write \
    .mode("append") \
    .format("csv") \
    .option("header", True) \
    .save("s3://july17-dev-eu-north-01-bucket/cleaned_layer/")

job.commit()