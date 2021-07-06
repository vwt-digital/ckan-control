import config
import google.auth
from google.auth import iam
from google.auth.transport import requests as gcp_requests
from google.oauth2 import service_account

TOKEN_URL = "https://accounts.google.com/o/oauth2/token"  # nosec


def request_auth_token():
    try:
        credentials, project_id = google.auth.default(
            scopes=["https://www.googleapis.com/auth/iam"]
        )

        request = gcp_requests.Request()
        credentials.refresh(request)

        signer = iam.Signer(request, credentials, config.DELEGATED_SA)
        creds = service_account.Credentials(
            signer=signer,
            service_account_email=config.DELEGATED_SA,
            token_uri=TOKEN_URL,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
            subject=config.DELEGATED_SA,
        )
    except Exception:
        raise

    return creds
