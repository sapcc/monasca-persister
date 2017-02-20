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

from monasca_common.repositories.exceptions import InvalidUpdateException
from oslo_log import log

from monasca_common.kafka import consumer
from monasca_persister.monitoring import client
from monasca_persister.monitoring.metrics import KAFKA_CONSUMER_ERRORS, FLUSH_ERRORS, MESSAGES_DROPPED, \
    MESSAGES_CONSUMED

LOG = log.getLogger(__name__)

STATSD_CLIENT = client.get_client()


class Persister(object):

    def __init__(self, kafka_conf, zookeeper_conf, repository):

        self._data_points = []

        self._kafka_topic = kafka_conf.topic

        self._database_batch_size = kafka_conf.database_batch_size

        self._consumer = consumer.KafkaConsumer(
                kafka_conf.uri,
                zookeeper_conf.uri,
                kafka_conf.zookeeper_path,
                kafka_conf.group_id,
                kafka_conf.topic,
                repartition_callback=self._flush,
                commit_callback=self._flush,
                commit_timeout=kafka_conf.max_wait_time_seconds)

        self.repository = repository()

        self.statsd_msg_count = STATSD_CLIENT.get_counter(MESSAGES_CONSUMED,
                                                          dimensions={'type': self._kafka_topic})
        self.statsd_msg_dropped_count = STATSD_CLIENT.get_counter(MESSAGES_DROPPED,
                                                                  dimensions={'type': self._kafka_topic})
        self.statsd_flush_error_count = STATSD_CLIENT.get_counter(FLUSH_ERRORS)
        self.statsd_kafka_consumer_error_count = STATSD_CLIENT.get_counter(KAFKA_CONSUMER_ERRORS,
                                                                           dimensions={'topic': self._kafka_topic})

    def _write_batch(self, data_points):
        try:
            self.repository.write_batch(data_points)
            self.statsd_msg_count.increment(len(data_points))
            LOG.info("Processed %d messages from topic %s", len(data_points), self._kafka_topic)
        except InvalidUpdateException:
            l = len(data_points)
            if l > 1:
                piv = int(l / 2)
                self._write_batch(data_points[0:piv])
                if piv < l:
                    self._write_batch(data_points[piv:])
            else:
                LOG.error("Error storing message. Message is being dropped: %s", data_points)
                self.statsd_msg_dropped_count.increment(1, sample_rate=1.0)

    def _flush(self):
        if not self._data_points:
            return

        try:
            self._write_batch(self._data_points)
            self._data_points = []
            self._consumer.commit()
        except Exception:
            LOG.exception("Error writing to database")
            self.statsd_flush_error_count.increment(1, sample_rate=1)
            raise

    def run(self):
        try:
            for raw_message in self._consumer:
                message = None
                try:
                    message = raw_message[1]
                    data_point = self.repository.process_message(message)
                    self._data_points.append(data_point)
                except Exception:
                    LOG.exception('Error processing message. Message is '
                                  'being dropped. %s', message)
                    self.statsd_msg_dropped_count.increment(1, sample_rate=1.0)

                if len(self._data_points) >= self._database_batch_size:
                    self._flush()
                else:
                    LOG.debug("buffering %d of %d", len(self._data_points), self._database_batch_size)
        except Exception:
            LOG.exception(
                    'Persister encountered fatal exception processing '
                    'messages. '
                    'Shutting down all threads and exiting')
            self.statsd_kafka_consumer_error_count.increment(1, sample_rate=1)
            os._exit(1)
