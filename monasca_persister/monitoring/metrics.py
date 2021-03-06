# Copyright 2016 FUJITSU LIMITED
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

FLUSH_ERRORS = 'persister.flush_errors'
""" errors occured when flushing a message (both on persister and TSDB side) """
MESSAGES_DROPPED = 'persister.messages_rejected'
""" number of invalid messages received and dropped """
MESSAGES_CONSUMED = 'persister.messages_consumed'
""" number of valid messages processed """

INFLUXDB_INSERT_TIME = "influxdb.insert_time"
""" time for writing batches to InfluxDB (incl. communication) """
KAFKA_CONSUMER_ERRORS = 'kafka.consumer_errors'
""" errors occured when fetching messages from Kafka (incl. ZK) """
