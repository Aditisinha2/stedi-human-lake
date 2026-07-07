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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1783348585171 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusted_node1783348585171")

# Script generated for node step trainer trusted
steptrainertrusted_node1783348585544 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step trainer trusted", transformation_ctx="steptrainertrusted_node1783348585544")

# Script generated for node SQL Query
SqlQuery2986 = '''
SELECT DISTINCT
    st.sensorReadingTime,
    st.serialNumber,
    st.distanceFromObject,
    ac.user,
    ac.timeStamp,
    ac.x,
    ac.y,
    ac.z
FROM st
INNER JOIN ac
ON st.sensorReadingTime = ac.timeStamp
'''
SQLQuery_node1783348603688 = sparkSqlQuery(glueContext, query = SqlQuery2986, mapping = {"ac":Accelerometertrusted_node1783348585171, "st":steptrainertrusted_node1783348585544}, transformation_ctx = "SQLQuery_node1783348603688")

# Script generated for node machine learning curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783348603688, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783348557768", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machinelearningcurated_node1783348607836 = glueContext.getSink(path="s3://stedihumanlake/machine-learning-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinelearningcurated_node1783348607836")
machinelearningcurated_node1783348607836.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine learning trusted")
machinelearningcurated_node1783348607836.setFormat("json")
machinelearningcurated_node1783348607836.writeFrame(SQLQuery_node1783348603688)
job.commit()