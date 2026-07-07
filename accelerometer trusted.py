import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node customer trusted
customertrusted_node1783336052794 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customertrusted", transformation_ctx="customertrusted_node1783336052794")

# Script generated for node accelerometer landing
accelerometerlanding_node1783336052193 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer landing", transformation_ctx="accelerometerlanding_node1783336052193")

# Script generated for node customer privacy filter
customerprivacyfilter_node1783334741239 = Join.apply(frame1=customertrusted_node1783336052794, frame2=accelerometerlanding_node1783336052193, keys1=["email"], keys2=["user"], transformation_ctx="customerprivacyfilter_node1783334741239")

# Script generated for node Drop Fields
DropFields_node1783401632945 = DropFields.apply(frame=customerprivacyfilter_node1783334741239, paths=["phone", "email", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "customername"], transformation_ctx="DropFields_node1783401632945")

# Script generated for node accelerometer trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1783401632945, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783329591403", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1783334769972 = glueContext.getSink(path="s3://stedihumanlake/accelerometer/acclerometer-trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1783334769972")
accelerometertrusted_node1783334769972.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer trusted")
accelerometertrusted_node1783334769972.setFormat("json")
accelerometertrusted_node1783334769972.writeFrame(DropFields_node1783401632945)
job.commit()