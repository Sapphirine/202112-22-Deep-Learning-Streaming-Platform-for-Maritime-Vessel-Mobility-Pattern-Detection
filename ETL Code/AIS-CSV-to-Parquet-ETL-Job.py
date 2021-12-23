##############################################################################
# eecse-6893-load-ais-data-lake - Ingests incoming AIS data (supplied via the
# source "input_dataset" job parameter, referencing the incoming Glue Template
# table) CSV data into partitioned Parquet files in the AIS data lake.
#
# Note: This file was copied out of the AWS Glue Environment for submission
# purposes.  Excessive line lengths and awsglue/pyspark dependencies are
# normally handled within that environment.
#
# Author: Joseph Krozak     UNI: JKK2139    Date 12/14/2021
##############################################################################

import sys
import re
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definitions needed to support posit basedatetime and status processing

from pyspark.sql.functions import input_file_name
from datetime import datetime
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType, TimestampType
from awsglue.dynamicframe import DynamicFrame

#################################################
# Utility functions defined below
#################################################


def xlate_vessel_type(vessel_type_num):
    vessel_type_str = "Not Available"

    if vessel_type_num in [21, 22, 31, 32, 52, 1023, 1025]:
        vessel_type_str = "Tug Tow"
    elif vessel_type_num in [30, 1001, 1002]:
        vessel_type_str = "Fishing"
    elif vessel_type_num in [35, 1021]:
        vessel_type_str = "Military"
    elif vessel_type_num in [36, 37, 1019]:
        vessel_type_str = "Pleasure"
    elif vessel_type_num in list(range(60, 70)) or \
            vessel_type_num in list(range(1012, 1016)):
        vessel_type_str = "Passenger"
    elif vessel_type_num in list(range(70, 80)) or \
            vessel_type_num in [1003, 1004, 1016]:
        vessel_type_str = "Cargo"
    elif vessel_type_num in list(range(80, 90)) or \
            vessel_type_num in [1017, 1024]:
        vessel_type_str = "Tanker"
    else:
        vessel_type_str = "Other"

    return vessel_type_str

def normalizePositStatus(posit_status):
    """
    Convert incoming posit status phrase into standardized
    status of Anchored, Moored, Fishing or Underway
    :param posit_status: Incoming posit data status
    :return: Normalized / standardized posit status.
    """
    # Convert incoming status to lower case and remove all whitespace
    temp_posit_status = posit_status.lower()
    temp_posit_status = "".join(posit_status.split())

    # Perform status transformations to Anchored, Moored, Fishing or Underway

    if "underway" in temp_posit_status:
        posit_status = "Underway"
    elif "fish" in temp_posit_status:
        posit_status = "Fishing"
    elif "anchor" in temp_posit_status:
        posit_status = "Anchored"
    elif "moor" in temp_posit_status:
        posit_status = "Moored"

    return posit_status


# Externalize status normalizer to UDF for Spark engine use
xformPositStatus = udf(lambda z: normalizePositStatus(z), StringType())

vesselTypeNum2String = udf(lambda z: xlate_vessel_type(z), StringType())

dateString2Date = udf(lambda z:  datetime.strptime(z, '%Y-%m-%dT%H:%M:%S'), TimestampType())

dateString2Year = udf(lambda z: z[0:4], StringType())
dateString2Month = udf(lambda z: z[5:7], StringType())
dateString2Day = udf(lambda z: z[8:10], StringType())

dateString2DT = udf(lambda z: z[0:10], StringType())

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Initialize job, including the mandatory 'input_dataset' parameter
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_DATASET'])

job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "eecse-6893-ais-csv", table_name = "2015_input_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []

# Retrieve and use the 'input_dataset' parameter supplied when job was executed.
input_dataset = args['INPUT_DATASET']

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "eecse-6893-ais-csv", table_name = input_dataset, transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("mmsi", "long", "mmsi", "long"), ("basedatetime", "string", "basedatetime", "string"), ("lat", "double", "lat", "double"), ("lon", "double", "lon", "double"), ("sog", "double", "sog", "double"), ("cog", "double", "cog", "double"), ("heading", "double", "heading", "double"), ("vesselname", "string", "vesselname", "string"), ("imo", "string", "imo", "string"), ("callsign", "string", "callsign", "string"), ("vesseltype", "long", "vesseltype", "long"), ("status", "string", "status", "string"), ("length", "double", "length", "double"), ("width", "double", "width", "double"), ("draft", "double", "draft", "double"), ("cargo", "long", "cargo", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("mmsi", "long", "mmsi", "long"), ("basedatetime", "string", "basedatetime", "string"), ("lat", "double", "lat", "double"), ("lon", "double", "lon", "double"), ("sog", "double", "sog", "double"), ("cog", "double", "cog", "double"), ("heading", "double", "heading", "double"), ("vesselname", "string", "vesselname", "string"), ("imo", "string", "imo", "string"), ("callsign", "string", "callsign", "string"), ("vesseltype", "long", "vesseltype", "long"), ("status", "string", "status", "string"), ("length", "double", "length", "double"), ("width", "double", "width", "double"), ("draft", "double", "draft", "double"), ("cargo", "long", "cargo", "long")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

######################################################################################################
# JKK - Perform custom processing (i.e. add dates, convert vessel types, create / hydrate partitions.
######################################################################################################

# Remove erroneous records (i.e. NULL IMO, Vessel Name, Date Time, Lat/long or speed)
posit_df = dropnullfields3.toDF()
posit_df = posit_df.where("'basedatetime' is NOT NULL AND 'lat' is NOT NULL AND 'lon' is NOT NULL AND 'sog' is NOT NULL AND 'imo' is NOT NULL AND 'vesselname' is NOT NULL")

# Standardize the posit status indication
posit_df = posit_df.withColumn("status", xformPositStatus(posit_df["status"]))

# Convert vessel type number to descriptive string
posit_df = posit_df.withColumn("vesseltype_str", vesselTypeNum2String(posit_df["vesseltype"]))

theDate = dateString2Date(col("basedatetime"))

# Add new datetime field to the data frame, based on 'basedatetime'
posit_df = posit_df.withColumn("basedatetime_dt", theDate)

# Add partition key 'dt', which is stringified date in YYYY-MM-DD format for range
# query purposes.

posit_df = posit_df.withColumn("dt", dateString2DT(col("basedatetime")))

# Convert back to dynamic frame for subsequent insertion into the job
after_custom_fields_added = DynamicFrame.fromDF(posit_df, glueContext, "after_custom_fields_added")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://eecse-6893-ais-datalake"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = after_custom_fields_added, connection_type = "s3", connection_options = {"path": "s3://eecse-6893-ais-datalake-j", "partitionKeys": ["dt"]}, format = "parquet", transformation_ctx = "datasink4")
job.commit()