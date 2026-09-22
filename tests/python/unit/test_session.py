import json
from unittest.mock import patch

import pytest
import rsa

from dku_googledrive import session


def create_session(credentials):
    return session.GoogleDriveSession({}, {"googledrive_connection": {"credentials": credentials}})


@pytest.fixture
def build():
    # Keep credential parsing and key validation real; never contact Google.
    with patch.object(session, "build") as mocked:
        yield mocked


@pytest.fixture(scope="module")
def keyfile():
    _, private_key = rsa.newkeys(1024)
    return {
        "type": "service_account",
        "client_email": "test@example.iam.gserviceaccount.com",
        "client_id": "test-client",
        "private_key_id": "test-key",
        "private_key": private_key.save_pkcs1().decode("ascii"),
    }


def test_credential_expression_does_not_execute(tmp_path, build):
    marker = tmp_path / "credential-executed"
    expression = "__import__('pathlib').Path({!r}).touch() or {{}}".format(str(marker))
    try:
        with pytest.raises(ValueError):
            create_session(expression)
    finally:
        assert not marker.exists()
    build.assert_not_called()


@pytest.mark.parametrize("credentials", [
    "", "{", "{'type': 'service_account'}", "[]", "null", "true", "42", '"text"',
])
def test_rejects_malformed_or_non_object_json(credentials, build):
    with pytest.raises(ValueError):
        create_session(credentials)
    build.assert_not_called()


@pytest.mark.parametrize("field", [
    "type", "client_email", "client_id", "private_key_id", "private_key",
])
def test_rejects_missing_service_account_fields(field, keyfile, build):
    incomplete = dict(keyfile)
    del incomplete[field]
    with pytest.raises((ValueError, KeyError)):
        create_session(json.dumps(incomplete))
    build.assert_not_called()


def test_valid_json_key_initializes_session(keyfile, build):
    # JSON booleans/null must be parsed as data too, even in optional fields.
    credentials = dict(keyfile, optional_enabled=True, optional_value=None)
    result = create_session(json.dumps(credentials))
    assert result.drive is build.return_value
    build.assert_called_once()
    assert build.call_args[0] == ("drive", "v3")


def test_oauth_still_initializes_session(build):
    config = {"auth_type": "oauth", "oauth_credentials": {"access_token": "test-token"}}
    with patch.object(session.ServiceAccountCredentials, "from_json_keyfile_dict") as factory:
        result = session.GoogleDriveSession(config, {})
    factory.assert_not_called()
    assert result.access_token == "test-token"
    assert result.drive is build.return_value
