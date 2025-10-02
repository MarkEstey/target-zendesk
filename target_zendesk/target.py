from singer_sdk import Target
from singer_sdk.typing import *

from target_zendesk.sinks import *

class TargetZendesk(Target):
    name = 'target-zendesk'

    config_jsonschema = PropertiesList(
        Property(
            'url_base',
            StringType,
            default='https://zendesk.com',
            description='Hostname for the Zendesk API',
        ),
        Property(
            'api_username',
            StringType,
            description='Username/email for basic API token authentication',
        ),
        Property(
            'api_token',
            StringType,
            secret=True,
            description='Token for basic API token authentication',
        ),
        Property(
            'oauth_token',
            StringType,
            secret=True,
            description='OAuth token from a completed login flow for OAuth authentication',
        ),
        Property(
            'default_action',
            StringType,
            required=True,
            default='upsert',
            description='Default action for writing records, must be one of: insert, upsert, delete'
        ),
        Property(
            'validate_records',
            BooleanType,
            default=True,
            description='Disable target record schema validation'
        ),
    ).to_dict()

    def get_sink_class(self, stream_name):
        if stream_name == 'custom_object_records': return CustomObjectRecordSink

if __name__ == "__main__":
    TargetZendesk.cli()
