# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

import monascastatsd
from oslo_log import log

from monasca_common.kafka.consumer import KafkaConsumer

STATSD_PORT = int(os.getenv("STATSD_PORT", "8125"))
STATSD_HOST = os.getenv("STATSD_HOST", "localhost")
LOG = log.getLogger(__name__)
statsd_connection = monascastatsd.Connection(host=STATSD_HOST, port=STATSD_PORT, max_buffer_size=50)
statsd_client = monascastatsd.Client('monasca.persister', connection=statsd_connection,
                                     dimensions={'service': 'monitoring', 'component': 'monasca-persister'})
statsd_timer = statsd_client.get_timer()
statsd_flush_error_count = statsd_client.get_counter('flush.errors')


class Persister(object):

    def __init__(self, kafka_conf, zookeeper_conf, repository):

        self._data_points = []

        self._kafka_topic = kafka_conf.topic

        self._database_batch_size = kafka_conf.database_batch_size

        self._consumer = KafkaConsumer(
                kafka_conf.uri,
                zookeeper_conf.uri,
                kafka_conf.zookeeper_path,
                kafka_conf.group_id,
                kafka_conf.topic,
                repartition_callback=self._flush,
                commit_callback=self._flush,
                commit_timeout=kafka_conf.max_wait_time_seconds)

        self.repository = repository()

        self.statsd_msg_count = statsd_client.get_counter('messages.consumed', dimensions={'type': self._kafka_topic})
        self.statsd_msg_dropped_count = statsd_client.get_counter('messages.dropped',
                                                                  dimensions={'type': self._kafka_topic})

    @statsd_timer.timed("flush.time", sample_rate=0.01)
    def _flush(self):
        if not self._data_points:
            return

        try:
            self.repository.write_batch(self._data_points)

            LOG.info("Processed %d messages from topic %s", len(self._data_points), self._kafka_topic)

            self._data_points = []
            self._consumer.commit()
        except Exception:
            LOG.exception("Error writing to database: %s", self._data_points)
            global statsd_flush_error_count
            statsd_flush_error_count += 1
            raise

    def run(self):
        try:
            for raw_message in self._consumer:
                message = None
                try:
                    message = raw_message[1]
                    data_point = self.repository.process_message(message)
                    self._data_points.append(data_point)
                    self.statsd_msg_count += 1
                except Exception:
                    LOG.exception('Error processing message. Message is '
                                  'being dropped. %s', message)
                    self.statsd_msg_dropped_count += 1

                if len(self._data_points) >= self._database_batch_size:
                    self._flush()
        except:
            LOG.exception(
                    'Persister encountered fatal exception processing '
                    'messages. '
                    'Shutting down all threads and exiting')
            os._exit(1)
