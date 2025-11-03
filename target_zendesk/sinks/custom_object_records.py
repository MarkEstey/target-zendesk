from target_zendesk.client import ZendeskSink
from singer_sdk.typing import *

import json
import time

class CustomObjectRecordSink(ZendeskSink):
    schema = PropertiesList(
        Property('custom_object_key', StringType, required=True),
        Property('custom_object_fields', ObjectType, required=True),
        Property('action', StringType),
        Property('id', StringType),
        Property('external_id', StringType),
        Property('name', StringType),
    )

    def start_batch(self, context):
        self._batch_records = {}

    @property
    def current_size(self):
        return sum(
            len(value)
            for action in self._batch_records.keys()
            for value in self._batch_records[action].values()
        )

    @property
    def is_full(self):
        return any(
            len(value) == 100
            for action in self._batch_records.keys()
            for value in self._batch_records[action].values()
        )

    def process_record(self, record, context):
        if record['action'] not in ('upsert', 'delete'):
            raise ValueError(f"record action must be either 'upsert' or 'delete': {record}")

        def enqueue_record(action, value):
            object = record['custom_object_key']

            if object not in self._batch_records:
                self._batch_records[object] = {}
            if action not in self._batch_records[object]:
                self._batch_records[object][action] = []
            self._batch_records[object][action].append(value)

        def format_record(record):
            body = {'custom_object_fields': json.loads(record['custom_object_fields'])}
            if record.get('id') is not None:
                body['id'] = record['id']
            if record.get('external_id') is not None:
                body['external_id'] = record['external_id']
            if record.get('name') is not None:
                body['name'] = record['name']
            return body

        if 'id' in record and record['id'] is not None:
            if record['action'] == 'upsert':
                enqueue_record('update', format_record(record))

            elif record['action'] == 'delete':
                enqueue_record('delete', record['id'])

        elif 'external_id' in record and record['external_id'] is not None:
            if record['action'] == 'upsert':
                enqueue_record('create_or_update_by_external_id', format_record(record))

            elif record['action'] == 'delete':
                enqueue_record('delete_by_external_id', record['external_id'])

        elif 'name' in record and record['name'] is not None:
            if record['action'] == 'upsert':
                enqueue_record('create_or_update_by_name', format_record(record))

            elif record['action'] == 'delete':
                raise ValueError(f"delete action requires id or external_id: {record}")

        else:
            if record['action'] == 'upsert':
                enqueue_record('create', format_record(record))

            elif record['action'] == 'delete':
                raise ValueError(f"delete action requires id or external_id: {record}")

            else:
                raise ValueError(f"could not determine how to process record: {record}")

    def process_batch(self, context):
        for object in self._batch_records.keys():
            for action in self._batch_records[object].keys():
                self.logger.debug(f"processing batch for object: {object}, action: {action}, size: {len(self._batch_records[object][action])}")
                assert len(self._batch_records[object][action]) <= 100, f"batch for object: {object}, action: {action} has more than 100 items"

                self.logger.info(f"debug: {dict({'job': {'action': action, 'items': self._batch_records[object][action]}})}")
                result = self._requests_session.post(
                    f"{self.config['url_base']}/api/v2/custom_objects/{object}/jobs",
                    json={'job': {'action': action, 'items': self._batch_records[object][action]}}
                )
                self.logger.info(f"debug: job result {result.json()}")
                job_status = result.json()['job_status']

                while job_status['status'] in ('queued', 'working'):
                    time.sleep(1)
                    result = self._requests_session.get(f"{self.config['url_base']}/api/v2/job_statuses/{job_status['id']}")
                    job_status = result.json()['job_status']
                    self.logger.info(f"debug: job result {result.json()}")

                self.logger.debug(f"finished batch for object: {object}, action: {action}, id: {job_status['id']}, status: {job_status['status']}")
                if job_status['status'] != 'completed':
                    raise Exception(f"failed batch status for object: {object}, action: {action}, id: {job_status['id']}, status: {job_status['status']}")
