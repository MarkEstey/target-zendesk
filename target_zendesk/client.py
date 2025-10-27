import base64
import requests

from singer_sdk.sinks import BatchSink

class ZendeskSink(BatchSink):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._requests_session = requests.Session()

        if self.config.get('api_username') is not None and self.config.get('api_token') is not None:
            credentials = f"{self.config['api_username']}/token:{self.config['api_token']}".encode()
            auth_token = base64.b64encode(credentials).decode('ascii')
            self._requests_session.headers.update({'Authorization': f'Basic {auth_token}'})

        elif self.config.get('oauth_token') is not None:
            self._requests_session.headers.update({'Authorization': f"Bearer {self.config['oauth_token']}"})

        else:
            raise ValueError('Either api_username and api_token or oauth_token must be set for authentication')
