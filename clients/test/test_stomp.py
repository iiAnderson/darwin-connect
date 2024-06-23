import os
from unittest import mock

import pytest
from src.stomp import Credentials, InvalidCredentials, StompClient


class TestStompClient:

    def test_connect(self) -> None:
        mock_client = mock.Mock()
        client = StompClient(mock_client)

        client.connect("username", "password", "topic")

        mock_client.connect.assert_called_once()
        mock_client.subscribe.assert_called_once()

    def test_disconnect(self) -> None:
        mock_client = mock.Mock()
        client = StompClient(mock_client)

        client.disconnect()

        mock_client.disconnect.assert_called_once()


class TestCredentials:

    @mock.patch.dict(
        os.environ, {"DARWIN_USERNAME": "NAME", "DARWIN_PASSWORD": "PASSWORD"}
    )
    def test_parse(self) -> None:

        assert Credentials("NAME", "PASSWORD") == Credentials.parse()

    def test_parse__no_env_vars(self) -> None:

        with pytest.raises(InvalidCredentials):
            Credentials.parse()
