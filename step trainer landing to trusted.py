import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
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

# Script generated for node customer curated
customercurated_node1783342077484 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer curated", transformation_ctx="customercurated_node1783342077484")

# Script generated for node step trainer landing
steptrainerlanding_node1783342078712 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedihumanlake/step_trainer/step_trainer-landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1783342078712")

# Script generated for node SQL Query
SqlQuery2882 = '''
SELECT DISTINCT
    stepTrainerLanding.sensorReadingTime,
    stepTrainerLanding.serialNumber,
    stepTrainerLanding.distanceFromObject
FROM stepTrainerLanding
INNER JOIN (
    SELECT DISTINCT serialnumber
    FROM customerCurated)customerSerials
ON stepTrainerLanding.serialNumber = customerSerials.serialnumber
'''
SQLQuery_node1783342083884 = sparkSqlQuery(glueContext, query = SqlQuery2882, mapping = {"stepTrainerLanding":steptrainerlanding_node1783342078712, "customerCurated":customercurated_node1783342077484}, transformation_ctx = "SQLQuery_node1783342083884")

# Script generated for node step trainer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783342083884, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783341709680", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1783342086169 = glueContext.getSink(path="s3://stedihumanlake/steptrainer/steptrainer-trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1783342086169")
steptrainertrusted_node1783342086169.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step trainer trusted")
steptrainertrusted_node1783342086169.setFormat("json")
steptrainertrusted_node1783342086169.writeFrame(SQLQuery_node1783342083884)
job.commit()