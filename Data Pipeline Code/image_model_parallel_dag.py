##############################################################################
# image_model_parallel_dag - Leverage Airflow DAG task definitions and associated
# python functions to 1) acquire IMOS with patterns of interest, 2)
# compute brackets for mobility patterns of interest, 3) compute images
# for each bracket, 4) persist images to an S3 landing zone for subsequent
# ingest into SageMaker / Tensorflow Jupyter notebook by mobility pattern type.
#
# Dynamic DAG task creation is used to achieve parallelism based on specified
# Branching factors.
#
# Author: Joseph Krozak    UNI: JKK2139       Date: 12/20/2021
##############################################################################

##########################################################################
# Required libraries definition section.
##########################################################################
import collections

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime
import logging
from AthenaQuery import AthenaQuery
from ImageFactory import ImageFactory
from PIL import Image
import base64
import boto3
import uuid
from io import BytesIO
from airflow.utils.db import provide_session
from airflow.models import XCom

QUERY_RESULTS_BUCKET = 'eecse-6893-athena'
IMAGE_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/728418413538/TRAINING-IMAGES"

OUTPUT_IMAGE_BUCKET = 'eecse-6893-track-images'

# Specifies the number of mobility bracket finder tasks to be allocated
# per supplier task (i.e. singleton, vessel_finder).

MOBILITY_BRACKET_FINDER_BRANCH_FACTOR = 15

# Specifies the number of mobility bracket imager tasks to be allocated
# per supplier task (i.e. mobility bracket finder).

MOBILITY_BRACKET_IMAGER_BRANCH_FACTOR = 5

# Mobility patterns of interest for query and image snapshot purposes.

mobility_patterns = ["Anchored", "Moored", "Underway", "Fishing"]

##########################################################################
# 0. DAG utility function implementation Section
##########################################################################

dateString2DT = lambda z: z[0:10]


def process_dag_arguments(**kwargs):
    # 
    start_date_time_parm = kwargs['dag_run'].conf['startdatetime']
    stop_date_time_parm = kwargs['dag_run'].conf['stopdatetime']
    run_mode_parm = kwargs['dag_run'].conf['runmode']
    max_imo_parm = kwargs['dag_run'].conf['maximos']

    if ((not (start_date_time_parm and start_date_time_parm.strip())) or
            (not (stop_date_time_parm and stop_date_time_parm.strip())) or
            (not (run_mode_parm and run_mode_parm.strip())) or
            (not (max_imo_parm and max_imo_parm.strip()))):
        raise ValueError("Task execution attempted without one or more required parameters.")

    logging.info(
        'Task Running with Start: {0},  Stop: {1}, RunMode: {2} and MaxIMOs: {3}'.format(
            start_date_time_parm, stop_date_time_parm, run_mode_parm, max_imo_parm))

    return start_date_time_parm, stop_date_time_parm, run_mode_parm, max_imo_parm


##########################################################################
# 1. DAG Task implementation Section
##########################################################################


def find_vessels_by_mobility_pattern_function(**kwargs):
    # Select the IMO numbers of vessels transiting over the period of interest
    # that have at least one occurrence of mobility of interest (e.g., Moored,
    # Anchored, Underway, Fishing.

    logging.info('In Vessel Finder')
    start_date_time_parm, stop_date_time_parm, run_mode_parm, max_imo_parm = \
        process_dag_arguments(**kwargs)

    query_string = \
        """
        SELECT DISTINCT(IMO) FROM "eecse-6893-ais-csv"."eecse_6893_ais_datalake_j" 
            WHERE DT BETWEEN '{0}' and '{1}' and status = 'Anchored' 
        """
    query_string = \
        query_string.format(
            dateString2DT(start_date_time_parm),
            dateString2DT(stop_date_time_parm))

    query_results_df = AthenaQuery.execute(query_string, QUERY_RESULTS_BUCKET)
    query_results_df = query_results_df.dropna(axis=0, how='any')
    imo_query_results = query_results_df["IMO"]
    logging.debug(imo_query_results)

    selected_imo_numbers = [each_imo for each_imo in imo_query_results]
    selected_imo_numbers = selected_imo_numbers[0:min(int(max_imo_parm), len(selected_imo_numbers))]
    logging.info('Returning {0} IMOs for workflow processing'.format(str(len(selected_imo_numbers))))
    return selected_imo_numbers


def calculate_task_workload_slice(task_id, num_tasks, total_workload):
    total_workload_size = len(total_workload)
    work_chunk_size = round(total_workload_size / num_tasks)
    start = task_id * work_chunk_size
    end = min(total_workload_size, start + work_chunk_size)

    return start, end


def find_brackets_by_vessel_and_mobility_pattern_function(mobility_finder_id, num_mobility_finders, **kwargs):
    # Find the start and stop datetime (i.e. brackets) a given vessel is
    # engaged in a given mobility pattern (e.g., Fishing) while transiting
    # over a given period of interest.  Leverages an approach described in the following
    # source: https://stackoverflow.com/questions/61380734/group-by-chunks-and-get-start-and-end-times-of-each-sql

    logging.info('In Mobility Pattern Finder ' + str(mobility_finder_id) + ' out of ' + str(num_mobility_finders))

    start_date_time_parm, stop_date_time_parm, run_mode_parm, max_imo_parm = \
        process_dag_arguments(**kwargs)

    ti = kwargs['ti']
    imos_to_process = ti.xcom_pull(task_ids='vessel_finder')

    # Calculate and retrieve the incoming work for this task

    work_start, work_end = \
        calculate_task_workload_slice( \
            mobility_finder_id, num_mobility_finders, imos_to_process)

    logging.info(
        "Taking work with start: {0} and end:  {1}".format(work_start, work_end))

    imos_to_process = imos_to_process[work_start: work_end]

    # This collection will contain stringified brackets for subsequent imaging purposes.

    mobility_brackets_to_image = []

    for imo_to_process in imos_to_process:
        for mobility_pattern in mobility_patterns:
            query_string = \
                """
                WITH cte_diff AS(
                  SELECT
                    "eecse-6893-ais-csv"."eecse_6893_ais_datalake_j".*,
                    COALESCE(LEAD(dt) OVER(PARTITION BY imo ORDER BY dt), '{2}') next_start_time,
                    CASE WHEN status = LAG(status) OVER(PARTITION BY imo ORDER BY dt) THEN 0 ELSE 1 END as differs
                  FROM
                    "eecse-6893-ais-csv"."eecse_6893_ais_datalake_j"
                  WHERE DT BETWEEN '{1}'and '{2}'and imo = '{3}' 
                ), 
                cte_runtot AS(
                 SELECT 
                   cte_diff.*, 
                   SUM(differs) OVER(PARTITION BY imo ORDER BY dt ROWS UNBOUNDED PRECEDING) as run_tot_dif
                 FROM
                   cte_diff
                )
    
                SELECT 
                  imo, 
                  status, 
                  MIN(dt) as start_time, 
                  MAX(next_start_time) as end_time
                FROM
                  cte_runtot
                WHERE status = '{0}'
                GROUP BY
                  imo, status, run_tot_dif
                ORDER BY
                  start_time
                """

            query_string = \
                query_string.format(
                    mobility_pattern, dateString2DT(start_date_time_parm),
                    dateString2DT(stop_date_time_parm), imo_to_process)

            query_result_df = AthenaQuery.execute(query_string, QUERY_RESULTS_BUCKET)
            query_result_df = query_result_df.dropna(axis=0, how='any')
            query_result_rows = query_result_df.to_records(index=False)
            selected_mobility_brackets = list(query_result_rows)

            # Return single list of stringified brackets over XCOM for downstream taska.

            for each_bracket in selected_mobility_brackets:
                stringified_bracket = ','.join(each_bracket)
                mobility_brackets_to_image.append(stringified_bracket)

    return mobility_brackets_to_image


def snap_track_images_by_mobility_pattern_bracket_function(
        parent_mobility_finder_id, mobility_imager_id, num_mobility_imagers, **kwargs):
    # Crate a vessel track image for the incoming bracket (i.e. IMO,
    # mobility pattern, start and stop date / times).

    logging.info('In Mobility Pattern Imager Parent:' + str(parent_mobility_finder_id) +
                 ', Current: ' + str(mobility_imager_id) + 'out of: ' + str(num_mobility_imagers))

    start_date_time_parm, stop_date_time_parm, run_mode_parm, max_imo_parm = \
        process_dag_arguments(**kwargs)

    ti = kwargs['ti']

    # Retrieve stringified bracket, before inflating into tuple form

    stringified_brackets = ti.xcom_pull(
        task_ids='mobility_bracket_finder_' + str(parent_mobility_finder_id))

    # Calculate and retrieve the incoming work for this task

    work_start, work_end = \
        calculate_task_workload_slice( \
            mobility_imager_id, num_mobility_imagers, stringified_brackets)

    logging.info(
        "Taking work with start: {0} and end:  {1}".format(work_start, work_end))

    stringified_brackets = stringified_brackets[work_start: work_end]

    for stringified_bracket in stringified_brackets:
        bracket_to_process = tuple(stringified_bracket.split(","))

        # Retrieve query components from the bracket tuple for subsequent query

        imo_to_process = bracket_to_process[0]
        mobility_to_process = bracket_to_process[1]
        bracket_start_datetime = bracket_to_process[2]
        bracket_stop_datetime = bracket_to_process[3]

        query_string = \
            """
            SELECT BASEDATETIME, LAT, LON, SOG FROM "eecse-6893-ais-csv"."eecse_6893_ais_datalake_j" WHERE 
                IMO = '{0}' AND STATUS ='{1}' AND DT  BETWEEN '{2}' and '{3}'
            """
        query_string = \
            query_string.format(
                imo_to_process, mobility_to_process,
                dateString2DT(bracket_start_datetime),
                dateString2DT(bracket_stop_datetime))

        query_result_df = AthenaQuery.execute(query_string, QUERY_RESULTS_BUCKET)
        query_result_df = query_result_df.dropna(axis=0, how='any')
        query_result_rows = query_result_df.to_records(index=False)
        selected_posits = list(query_result_rows)

        # Build dictionary by basedatetime of posit latitude, longitude and speed

        posits_by_date_time = {}

        for each_posit in selected_posits:
            posit_date_time = \
                datetime.strptime(each_posit[0], '%Y-%m-%dT%H:%M:%S')

            posit_latitude = float(each_posit[1])
            posit_longitude = float(each_posit[2])
            posit_speed = float(each_posit[3])

            # ... and accumulate posit tuples by date / time for return

            posits_by_date_time[posit_date_time] = \
                (posit_latitude, posit_longitude, posit_speed)

        # Snap picture of bracketed mobility pattern

        image_factory = ImageFactory(QUERY_RESULTS_BUCKET)
        image_array = image_factory.generateImage( \
            collections.OrderedDict(sorted(posits_by_date_time.items())))

        # Next, Base64 encode the image and push to SQS

        image = Image.fromarray(image_array.astype('uint8'), mode='RGB')
        buffer = BytesIO()
        image.save(buffer, "PNG")
        buffer.seek(0)

        s3_client = boto3.client("s3", region_name="us-east-2")

        file_name = run_mode_parm + '/' + mobility_to_process + '/' + \
                    imo_to_process + '-' + bracket_start_datetime + '-' + \
                    bracket_stop_datetime + '.png'

        sent_data = s3_client.put_object(Bucket=OUTPUT_IMAGE_BUCKET, Key=file_name, Body=buffer)
        if sent_data['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ValueError('Failed to write image {} to bucket {}'.format(file_name, OUTPUT_IMAGE_BUCKET))


################################################
# 2. DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
################################################


default_args = \
    {
        'owner': 'airflow',
        'retries': 1
    }


@provide_session
def cleanup_xcom(context, session=None):
    dag_id = context["ti"]["dag_id"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


mobility_pattern_processor_dag = \
    DAG('Mobility-Pattern-Image-Parallel-Processor',
        default_args=default_args,
        description='Parallel process positional data as imagery for CNN Models',
        catchup=False,
        start_date=datetime(2021, 12, 1),
        schedule_interval=None,
        on_success_callback=cleanup_xcom
        )

##########################################
# 3. DEFINE AIRFLOW OPERATORS
##########################################

find_vessels_by_mobility_pattern_task = \
    PythonOperator(
        task_id='vessel_finder',
        python_callable=find_vessels_by_mobility_pattern_function,
        provide_context=True,
        dag=mobility_pattern_processor_dag)


def create_dynamic_mobility_finder_task(current_task_number):
    return PythonOperator(
        task_id='mobility_bracket_finder_' + str(current_task_number),
        python_callable=find_brackets_by_vessel_and_mobility_pattern_function,
        provide_context=True,

        # Each task must know it's position relative to the total number
        # of similar tasks in order to compute its workload.

        op_args=[int(current_task_number), int(MOBILITY_BRACKET_FINDER_BRANCH_FACTOR)],
        dag=mobility_pattern_processor_dag)


def create_dynamic_mobility_imager_task(parent_task_number, current_task_number):
    return PythonOperator(
        task_id='mobility_bracket_imager_' + str(parent_task_number) + '_' + str(current_task_number),
        python_callable=snap_track_images_by_mobility_pattern_bracket_function,
        provide_context=True,

        # Each task must know it's position relative to the total number
        # of similar tasks in order to compute its workload.

        op_args=[int(parent_task_number), int(current_task_number),
                 int(MOBILITY_BRACKET_IMAGER_BRANCH_FACTOR)],

        dag=mobility_pattern_processor_dag)


##########################################
# 4. DEFINE OPERATORS HIERARCHY
##########################################

parallel_mobility_finders = []

# Create parallel mobility finders...

for mobility_finder_index in range(int(MOBILITY_BRACKET_FINDER_BRANCH_FACTOR)):
    parallel_mobility_finders.append(
        create_dynamic_mobility_finder_task(mobility_finder_index))

# Latch parallel mobility finders to initial vessel finder task...

find_vessels_by_mobility_pattern_task >> parallel_mobility_finders
mobility_finder_index = 0

# Now latch parallel mobility imager tasks to each mobility finder.

for parallel_mobility_finder in parallel_mobility_finders:
    parallel_mobility_imagers = []

    for mobility_imager_index in range(int(MOBILITY_BRACKET_IMAGER_BRANCH_FACTOR)):
        parallel_mobility_imagers.append(
            create_dynamic_mobility_imager_task(
                mobility_finder_index, mobility_imager_index))

    parallel_mobility_finder >> parallel_mobility_imagers
    mobility_finder_index += 1
