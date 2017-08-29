#!/usr/bin/env python
# coding=utf-8

# Copyright 2016-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License. A copy of the License is located at
# http://aws.amazon.com/apache2.0/
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

import sys
import os
# add the lib directory to the path
from copy import deepcopy

sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

import attr
import base64
import boto3
from botocore.exceptions import ParamValidationError
import datetime
import json
import logging
from time import sleep
import pg8000

__version__ = "1.3"

logging.basicConfig(format='%(levelname) -10s %(asctime)s %(module)s at line %(lineno)d: %(message)s')
logger = logging.getLogger('redshift-monitoring')


# Configuration
# Set it here by changing defaults or in the environment

@attr.s
class Reporter(object):
    user = attr.ib(default=os.environ.get('USER') or '')
    encrypted_password = attr.ib(default=os.environ.get('ENCRYPTED_PASSWORD') or '')
    host = attr.ib(default=os.environ.get('HOST') or '')
    environment = attr.ib(default=os.environ.get('ENVIRONMENT') or '', )
    port = attr.ib(default=os.environ.get('PORT') or 5439)
    database = attr.ib(default=os.environ.get('DATABASE') or '')
    ssl = attr.ib(default=os.environ.get('SSL') or True)
    cluster = attr.ib(default=os.environ.get('CLUSTER') or '')
    interval = attr.ib(default=os.environ.get('INTERVAL') or '1 hour')
    debug = attr.ib(default=bool(os.environ.get('DEBUG')) or False)
    region = attr.ib(default=os.environ.get('REGION') or '')

    def gather_table_stats(self, cursor):
        self.run_command(cursor, '''SELECT /* Lambda CloudWatch Exporter */ 
                                  \"schema\" || '.' || \"table\" AS table, 
                                  encoded, max_varchar, unsorted, stats_off, tbl_rows, skew_sortkey1, skew_rows 
                                  FROM svv_table_info''')

        tables_not_compressed = 0
        max_skew_ratio = 0
        total_skew_ratio = 0
        number_tables_skew = 0
        number_tables = 0
        max_skew_sort_ratio = 0
        total_skew_sort_ratio = 0
        number_tables_skew_sort = 0
        number_tables_statsoff = 0
        max_varchar_size = 0
        max_unsorted_pct = 0
        total_rows = 0

        result = cursor.fetchall()

        for table in result:
            table_name, encoded, max_varchar, unsorted, stats_off, tbl_rows, skew_sortkey1, skew_rows = table
            number_tables += 1
            if encoded == 'N':
                tables_not_compressed += 1
            if skew_rows is not None:
                if skew_rows > max_skew_ratio:
                    max_skew_ratio = skew_rows
                total_skew_ratio += skew_rows
                number_tables_skew += 1
            if skew_sortkey1 is not None:
                if skew_sortkey1 > max_skew_sort_ratio:
                    max_skew_sort_ratio = skew_sortkey1
                total_skew_sort_ratio += skew_sortkey1
                number_tables_skew_sort += 1
            if stats_off is not None and stats_off > 5:
                number_tables_statsoff += 1
            if max_varchar is not None and max_varchar > max_varchar_size:
                max_varchar_size = max_varchar
            if unsorted is not None and unsorted > max_unsorted_pct:
                max_unsorted_pct = unsorted
            if tbl_rows is not None:
                total_rows += tbl_rows

        if number_tables_skew > 0:
            avg_skew_ratio = total_skew_ratio / float(number_tables_skew)
        else:
            avg_skew_ratio = 0

        if number_tables_skew_sort > 0:
            avg_skew_sort_ratio = total_skew_sort_ratio / float(number_tables_skew_sort)
        else:
            avg_skew_sort_ratio = 0

        # build up the metrics to put in cloudwatch
        return [
            {
                'MetricName': 'TablesNotCompressed',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': tables_not_compressed,
                'Unit': 'Count'
            },
            {
                'MetricName': 'MaxSkewRatio',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': max_skew_ratio,
                'Unit': 'None'
            },
            {
                'MetricName': 'AvgSkewRatio',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': avg_skew_ratio,
                'Unit': 'None'
            },
            {
                'MetricName': 'Tables',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': number_tables,
                'Unit': 'Count'
            },
            {
                'MetricName': 'MaxSkewSortRatio',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': max_skew_sort_ratio,
                'Unit': 'None'
            },
            {
                'MetricName': 'AvgSkewSortRatio',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': avg_skew_sort_ratio,
                'Unit': 'None'
            },
            {
                'MetricName': 'TablesStatsOff',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': number_tables_statsoff,
                'Unit': 'Count'
            },
            {
                'MetricName': 'MaxVarcharSize',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': max_varchar_size,
                'Unit': 'None'
            },
            {
                'MetricName': 'MaxUnsorted',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': max_unsorted_pct,
                'Unit': 'Percent'
            },
            {
                'MetricName': 'Rows',
                'Dimensions': [
                    {'Name': self.environment, 'Value': self.cluster}
                ],
                'Timestamp': datetime.datetime.utcnow(),
                'Value': total_rows,
                'Unit': 'Count'
            }
        ]

    def run_external_commands(self, command_set_type, file_name, cursor):
        if not os.path.exists(file_name):
            logger.error("File {} does not exit.".format(file_name))
            return []

        try:
            external_commands = json.load(open(file_name))
        except ValueError as e:
            # handle a malformed user query set gracefully
            if e.message == "No JSON object could be decoded":
                logger.error(e.message + " in {}".format(file_name))
                return []
            else:
                raise

        output_metrics = []

        for command in external_commands:
            if command['type'] == 'value':
                cmd_type = "Query"
            else:
                cmd_type = "Canary"

            logger.info("Executing %s %s: %s" % (command_set_type, cmd_type, command['name']))

            t = datetime.datetime.now()
            interval = self.run_command(cursor, command['query'])
            try:
                value = cursor.fetchone()[0]
            except pg8000.core.ProgrammingError:
                logger.info("No values fetched for commamnd {}".format(command['name']))
                value = None

            if value is None:
                value = 0

            # append a cloudwatch metric for the value, or the elapsed interval, based upon the configured 'type' value
            if command['type'] == 'value':
                output_metrics.append({
                    'MetricName': command['name'],
                    'Dimensions': [
                        {'Name': self.environment, 'Value': self.environment}
                    ],
                    'Timestamp': t,
                    'Value': value,
                    'Unit': command['unit']
                })
            else:
                output_metrics.append({
                    'MetricName': command['name'],
                    'Dimensions': [
                        {'Name': self.environment, 'Value': self.cluster}
                    ],
                    'Timestamp': t,
                    'Value': interval,
                    'Unit': 'Milliseconds'
                })

        return output_metrics

    @staticmethod
    def run_command(cursor, statement):
        logger.debug("Running Statement: %s" % statement)

        t = datetime.datetime.now()
        try:
            cursor.execute(statement)
        except pg8000.core.ProgrammingError as e:
            logger.error('Error executing statement %s: %s', statement, e)
        interval = (datetime.datetime.now() - t).microseconds / 1000

        return interval

    def gather_service_class_stats(self, cursor):
        metrics = []
        poll_ts = datetime.datetime.utcnow()
        self.run_command(cursor, '''SELECT service_class, num_queued_queries, num_executing_queries 
                                            FROM stv_wlm_service_class_state w WHERE w.service_class >= 6 ORDER BY 1
                                    ''')
        service_class_info = cursor.fetchall()

        for service_class in service_class_info:
            queued_metric = {'MetricName': 'ServiceClass%s-Queued' % service_class[0],
                             'Dimensions': [{'Name': self.environment, 'Value': self.cluster}],
                             'Timestamp': poll_ts,
                             'Value': service_class[1]}
            metrics.append(deepcopy(queued_metric))

            executing_metric = {'MetricName': 'ServiceClass%s-Executing' % service_class[0],
                                'Dimensions': [{'Name': self.environment, 'Value': self.cluster}],
                                'Timestamp': poll_ts,
                                'Value': service_class[2]}
            metrics.append(deepcopy(executing_metric))

        return metrics


def configure():
    reporter = Reporter()

    if reporter.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("DEBUG mode set.")
    else:
        logger.setLevel(logging.INFO)

    for k, v in attr.asdict(reporter).items():
        if not v:
            logger.warning("Parameter {} is unset".format(k))

    return reporter


def lambda_handler(event, context):
    reporter = configure()

    cw = boto3.client('cloudwatch', region_name=reporter.region)

    pg8000.paramstyle = "qmark"

    try:
        kms = boto3.client('kms', region_name=reporter.region)
        # Check if decryption is possible
        password = kms.decrypt(CiphertextBlob=base64.b64decode(reporter.encrypted_password))['Plaintext']
    except:
        logger.error('KMS access failed: exception %s' % sys.exc_info()[1])
        raise

    try:
        logger.debug('Connecting to Redshift: %s' % reporter.host)
        logger.debug('Redshift user: {}'.format(reporter.user))
        logger.debug('Redshift pwd: {}'.format(reporter.encrypted_password))

        conn = pg8000.connect(database=reporter.database,
                              user=reporter.user,
                              password=password,
                              host=reporter.host,
                              port=5439,
                              ssl=reporter.ssl)
    except (pg8000.InterfaceError, pg8000.ProgrammingError) as e:
        logger.error('Redshift Connection Failed: exception %s' % e)
        sys.exit(1)

    logger.debug('Successfully Connected to Cluster')
    cursor = conn.cursor()

    # collect table statistics
    put_metrics = reporter.gather_table_stats(cursor)

    # collect service class statistics
    put_metrics.extend(reporter.gather_service_class_stats(cursor))

    # run the externally configured commands and append their values onto the put metrics
    put_metrics.extend(
        reporter.run_external_commands('Redshift Diagnostic', 'monitoring-queries.json', cursor))

    # run the supplied user commands and append their values onto the put metrics
    put_metrics.extend(reporter.run_external_commands('User Configured', 'user-queries.json', cursor))

    max_metrics = 20
    group = 0
    logger.info("Publishing %s CloudWatch Metrics" % (len(put_metrics)))

    for x in range(0, len(put_metrics), max_metrics):
        group += 1

        # slice the metrics into blocks of 20 or just the remaining metrics
        put = put_metrics[x:(x + max_metrics)]

        logger.debug("Metrics group %s: %s Datapoints" % (group, len(put)))
        logger.debug(put)
        try:
            cw.put_metric_data(
                Namespace="Redshift",
                MetricData=put
            )
        except ParamValidationError:
            logger.error('Pushing metrics to CloudWatch failed: exception %s' % sys.exc_info()[1])

    cursor.close()
    conn.close()
    return 'Finished'


if __name__ == "__main__":
    if os.environ.get("IN_LAMBDA"):
        lambda_handler(sys.argv[0], None)
    else:
        # Run every minute if not in lambda
        while True:
            lambda_handler(sys.argv[0], None)
            sleep(60)
