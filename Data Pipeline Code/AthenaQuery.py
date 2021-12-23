##############################################################################
# AthenaQuery - This is a utility class developed as a facade that simplifies
# Athena query execution and encapsulates tll the asynchronous semantics
# the underlying Athena API requires for query execution and status / results
# retrieval.
#
# Author: Joseph Krozak     UNI: JKK2139    Date 12/08/2021
##############################################################################

import sys

import boto3
import time
import re
import io
import pandas as pd
import logging


class AthenaQuery:
    MAX_QUERY_RUNTIME = 300  # Longest time we wait for query to finish executing (in seconds)

    @classmethod
    def wait_for_query_completion(cls, query_service_client, query_execution_id: str, max_wait):
        """
        determines if the query has finished executing. If it has not, the function waits for the query to finished.
        Parameters:
          :param max_wait:
          :param query_execution_id: the query execution id (as a string)
          :param query_service_client: the client instance providing query access (i.e. Athena)
        """
        while True:
            max_wait = max_wait - 1
            response = query_service_client.get_query_execution(QueryExecutionId=query_execution_id)

            if 'QueryExecution' in response and \
                    'Status' in response['QueryExecution'] and \
                    'State' in response['QueryExecution']['Status']:
                state = response['QueryExecution']['Status']['State']
                logging.info(state)
                if state == 'FAILED':
                    raise ValueError(
                        response['QueryExecution']['Status']['StateChangeReason'])
                elif state == 'SUCCEEDED':
                    s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                    filename = re.findall('.*\/(.*)', s3_path)[0]
                    return filename
            if max_wait <= 0:
                break
            time.sleep(1)
        raise ValueError(
            'Query failed to return within {} seconds.'.format(AthenaQuery.MAX_QUERY_RUNTIME))

    @classmethod
    def execute(cls, query_string: str, result_bucket: str):
        """
       Executes an Athena query and returns the results from s3.

        Parameters:
          :param query_string: the query to be executed (as a string)
          :param result_bucket: the s3 location where Athena saves the results (as a string)
        """
        athena_client = boto3.client('athena', region_name='us-east-1')
        s3_client = boto3.client('s3', region_name='us-east-1')
        query_result_location = "s3://" + result_bucket + "/"

        response = athena_client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration=
            {
                'OutputLocation': query_result_location
            }
        )
        query_execution_id = response['QueryExecutionId']

        query_result_file_name = \
            AthenaQuery.wait_for_query_completion(
                athena_client, query_execution_id, AthenaQuery.MAX_QUERY_RUNTIME)

        # Finally, access the query results CSV file and do a byte image read to
        # access its contents.

        file_object = s3_client.get_object(Bucket=result_bucket, Key=query_result_file_name)
        result_df = pd.read_csv(io.BytesIO(file_object['Body'].read()))

        return result_df
