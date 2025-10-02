from target_zendesk.client import ZendeskSink
from singer_sdk.typing import *

class CustomObjectRecordSink(ZendeskSink):
    schema = PropertiesList(
        Property('custom_object_key', StringType, required=True),
        Property('custom_object_fields', ObjectType, required=True),
        Property('action', StringType),
        Property('custom_object_record_id', StringType),
        Property('external_id', StringType),
        Property('name', StringType),
    )

    def process_record(self, record, context):
        if 'action' not in record:
            record['action'] = self.config['default_action']

        def create_body_from_record(record):
            body = {'custom_object_record': {'custom_object_fields': record['custom_object_fields']}}
            if 'external_id' in record: body['external_id'] = record['external_id']
            if 'name' in record: body['name'] = record['name']
            return body

        if record['action'] == 'upsert':
            if 'custom_object_record_id' in record and record['custom_object_record_id'] is not None:
                self._requests_session.patch(
                    f"{self.config['url_base']}/api/v2/custom_objects/{record['custom_object_key']}/records/{record['custom_object_record_id']}",
                    json=create_body_from_record(record),
                )

            elif 'external_id' in record and record['external_id'] is not None:
                self._requests_session.patch(
                    f"{self.config['url_base']}/api/v2/custom_objects/{record['custom_object_key']}/records",
                    params={'external_id': record['external_id']},
                    json=create_body_from_record(record),
                )

            elif 'name' in record and record['name'] is not None:
                self._requests_session.patch(
                    f"{self.config['url_base']}/api/v2/custom_objects/{record['custom_object_key']}/records",
                    params={'name': record['name']},
                    json=create_body_from_record(record),
                )

            else:
                raise ValueError('custom_object_record update action requires custom_object_record_id, external_id, or name in record')
