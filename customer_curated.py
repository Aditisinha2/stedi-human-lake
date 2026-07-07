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

# Script generated for node accelerometer landing
accelerometerlanding_node1783338286251 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer landing", transformation_ctx="accelerometerlanding_node1783338286251")

# Script generated for node Customer trusted
Customertrusted_node1783338286420 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customertrusted", transformation_ctx="Customertrusted_node1783338286420")

# Script generated for node Join
Join_node1783338295584 = Join.apply(frame1=Customertrusted_node1783338286420, frame2=accelerometerlanding_node1783338286251, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1783338295584")

# Script generated for node Drop fields and duplicates
SqlQuery2916 = '''
SELECT DISTINCT
    customername,
    email,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
FROM myDataSource
'''
Dropfieldsandduplicates_node1783338388218 = sparkSqlQuery(glueContext, query = SqlQuery2916, mapping = {"myDataSource":Join_node1783338295584}, transformation_ctx = "Dropfieldsandduplicates_node1783338388218")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=Dropfieldsandduplicates_node1783338388218, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783338010693", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1783338630088 = glueContext.getSink(path="s3://stedihumanlake/customer/customer-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1783338630088")
customercurated_node1783338630088.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer curated")
customercurated_node1783338630088.setFormat("json")
customercurated_node1783338630088.writeFrame(Dropfieldsandduplicates_node1783338388218)
job.commit()