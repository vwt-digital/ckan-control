import config
import google.auth
from google.auth import iam
from google.auth.transport import requests as gcp_requests
from google.oauth2 import service_account


class GCPHelper:

    def request_auth_token(self):
        try:
            credentials, project_id = google.auth.default(
                scopes=["https://www.googleapis.com/auth/iam"]
            )

            request = gcp_requests.Request()
            credentials.refresh(request)

            signer = iam.Signer(request, credentials, config.DELEGATED_SA)
            token_url = "https://accounts.google.com/o/oauth2/token"  # nosec
            creds = service_account.Credentials(
                signer=signer,
                service_account_email=config.DELEGATED_SA,
                token_uri=token_url,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
                subject=config.DELEGATED_SA,
            )
        except Exception:
            raise

        return creds
