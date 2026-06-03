import json
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from  google.cloud import secretmanager
from typing import Optional

def get_project_id() -> str:
    """Gets the GCP project ID from default credentials and
    if not available, then set it to empty str.
    The latter is useful for running testing (not requiring
    credentials) from GitHub.

    Returns:
        str: project ID or empty string
    """
    try:
        _, project_id = default()
    except DefaultCredentialsError:
        project_id = ""
    finally:
        return project_id

        
def get_secrets(
    project_id: str, secret_name: str, version: Optional[str] = "latest"
) -> str:
    """Retrieves secret from GCP Secret Manager. In most cases it will be username and password.

    Args:
        project_id (str): GCP project idetifier
        secret_name (str): secret label
        version (str, optional): Secret version. Defaults to "latest".

    Returns:
        str: GCP secret content
    """

    client = secretmanager.SecretManagerServiceClient()
    name = "projects/" + project_id + "/secrets/" + secret_name + "/versions/" + version
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    try:
        return json.loads(payload)
    except:
        raise Exception(f'Could not get secret {secret_name} because payload is not a JSON')