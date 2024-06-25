import os
from unittest import mock

import pytest
from src.stomp import (
    Credentials,
    InvalidCredentials,
    InvalidMessage,
    RawMessage,
    StompClient,
)
from stomp.utils import Frame


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

    @mock.patch.dict(os.environ, {"DARWIN_USERNAME": "NAME", "DARWIN_PASSWORD": "PASSWORD"})
    def test_parse(self) -> None:

        assert Credentials("NAME", "PASSWORD") == Credentials.parse()

    def test_parse__no_env_vars(self) -> None:

        with pytest.raises(InvalidCredentials):
            Credentials.parse()


class TestRawMessage:

    def test(self) -> None:

        frame = Frame(
            cmd="MESSAGE",
            headers={
                "content-length": "492",
                "expires": "1719338621386",
                "destination": "/topic/darwin.pushport-v16",
                "CamelJmsDeliveryMode": "1",
                "subscription": "1",
                "priority": "4",
                "breadcrumbId": "ID-nrdp-prod-01-dsg-caci-co-uk-1714634665453-0-768527694",
                "Content_HYPHEN_Type": "application/xml",
                "Username": "thales",
                "SequenceNumber": "2970475",
                "message-id": "ID:nrdp-prod-01.dsg.caci.co.uk-45027-1714634666397-8:225:1:1:1557819",
                "PushPortSequence": "8957657",
                "MessageType": "TS",
                "timestamp": "1719338221386",
            },
            body=b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00\xbd\x95]o\x9b0\x14\x86\xff\x8a\xe5\xdb)16\x1f!(Pu\x8b:M\xea\xd6(a\xda\xe5d\x81\x05H\xc4 \x7f\x84\xf5\xdf\xef\x04\xd20\xa6V\x1d\x9d\xb6\x1b$\xdb\xe7}xl|\xc4\xe6\xe6\xc7\xb1F'\xa1t\xd5\xc8\x18\xd3\xa5\x83\x91\x90Y\x93W\xb2\x88\xf1\xd7\xf4n\x11b\xa4\r\x979\xaf\x1b)b\xfc(4\xbeI6\xbb\xb6Q\x06AV\xea\x18\x97\xc6\xb4\x11!]\xd7-M\xc9k\xa1\x0b\xd5\xd8v\x995G\xa2\x8c\xa9\xc8\xce\xear\x07\xf5\xe4D\x03<\x84\"\xa9\xd9\x8c\xe0!+En\xa1\x80\x9c\xdc\x91\xe0\xbe\x8d\xc0F\x827\x83p\xd7\xa8#7pLS\x84?\x0f!2\xae\xcdt\x1b\xc1\x1b%\xe8\x88X\xcd9\t\xd3\x03>\x0b\xady!\xa6\x9cp\x06'U\xbc\x92\xb7\xb5Pf\xcaX\xcfe<\xa8\\\xa8\t\x82:s\x18\xdb-7|\x9a\xa73\xf2\xb75W\xc7\xe9\x16\xe8x7\x7f\xcf\xee\xd3\xf4\xd35{>J\xab\x89j\x1a\xf3\x1d\xe2\x06z\x819\xcc[8\xc1\x82\xf9)\r#\x7f\x159t\xe9\x86\x94\xba\x8c\xbdsh\xe4@\x7f\x8d\xdd\x16@\xbb%\x1b\xbbG\xb6\xcd\xb9\x11\x0f\xaa**\x98\xdfr\xd5U\x12V\xd2\x03RU>@\x9d\x80\xf9\xa1\xb3^\xaf<\x1f#{\x9e\xdd]\x06Z\xe7\xbf\xbe\x17rp+\xa3\xfb&\xeb\xbf32m\x1d\xe3\x0f_\xd2\xf7\xdf0\xea\x0c\x94R\x90r/U\xb9h\x9100\x07\xae\xd0\xe9\xb9\xa8\xf9\xa3\x80\x1a\xa3\xac\x00\xb2\xca\xae6d\x08h\xdb\xb6*9/o\xc88\xee\x97j!\x0bS&\xde\xb0p\x19\r\x83'\x99\x17\xd5\xe8\xd9\x8d\x0fn\xfe\xe8I\x9d\x8b'Wj\xf0\\\xf7G\xf8\xba\xe7uc\x10\xa0\x7f\x12\xf8\x1b\xfb\x8f\xf7\xa3>e\xa3>\x0b\x9e\xd1w\xe7\xea{\xffZ\x7f\x94g\xe13\xc2\xc1\xff\xb8\x17$=\xc0\xc3\xee\xe1\xd1\xff[\x92\x9f\xbb\x91\xcb)\x9a\x06\x00\x00",
        )

        msg = RawMessage.create(frame)
        assert msg == RawMessage(
            "TS",
            {
                "Pport": {
                    "@xmlns": "http://www.thalesgroup.com/rtti/PushPort/v16",
                    "@xmlns:ns2": "http://www.thalesgroup.com/rtti/PushPort/Schedules/v3",
                    "@xmlns:ns3": "http://www.thalesgroup.com/rtti/PushPort/Schedules/v2",
                    "@xmlns:ns4": "http://www.thalesgroup.com/rtti/PushPort/Formations/v2",
                    "@xmlns:ns5": "http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3",
                    "@xmlns:ns6": "http://www.thalesgroup.com/rtti/PushPort/Formations/v1",
                    "@xmlns:ns7": "http://www.thalesgroup.com/rtti/PushPort/StationMessages/v1",
                    "@xmlns:ns8": "http://www.thalesgroup.com/rtti/PushPort/TrainAlerts/v1",
                    "@xmlns:ns9": "http://www.thalesgroup.com/rtti/PushPort/TrainOrder/v1",
                    "@xmlns:ns10": "http://www.thalesgroup.com/rtti/PushPort/TDData/v1",
                    "@xmlns:ns11": "http://www.thalesgroup.com/rtti/PushPort/Alarms/v1",
                    "@xmlns:ns12": "http://thalesgroup.com/RTTI/PushPortStatus/root_1",
                    "@ts": "2024-06-25T18:57:01.3811322+01:00",
                    "@version": "16.0",
                    "uR": {
                        "@updateOrigin": "Darwin",
                        "TS": {
                            "@rid": "202406258099745",
                            "@uid": "P99745",
                            "@ssd": "2024-06-25",
                            "ns5:Location": [
                                {
                                    "@tpl": "CNTBW",
                                    "@wtd": "17:03",
                                    "ns5:dep": {
                                        "@et": "18:58",
                                        "@delayed": "true",
                                        "@src": "Darwin",
                                    },
                                    "ns5:suppr": "true",
                                    "ns5:length": "4",
                                },
                                {
                                    "@tpl": "CNTBW1",
                                    "@wta": "17:05",
                                    "@wtd": "17:10",
                                    "ns5:arr": {
                                        "@et": "19:00",
                                        "@delayed": "true",
                                        "@src": "Darwin",
                                    },
                                    "ns5:dep": {
                                        "@et": "19:01",
                                        "@delayed": "true",
                                        "@src": "Darwin",
                                    },
                                    "ns5:length": "4",
                                },
                                {
                                    "@tpl": "CNTBWGL",
                                    "@wta": "17:12",
                                    "@wtd": "17:26",
                                    "ns5:arr": {
                                        "@et": "19:03",
                                        "@delayed": "true",
                                        "@src": "Darwin",
                                    },
                                    "ns5:dep": {
                                        "@et": "19:04",
                                        "@delayed": "true",
                                        "@src": "Darwin",
                                    },
                                    "ns5:length": "4",
                                },
                                {
                                    "@tpl": "CNTBW",
                                    "@wta": "17:28",
                                    "ns5:arr": {
                                        "@et": "19:06",
                                        "@delayed": "true",
                                        "@src": "Darwin",
                                    },
                                    "ns5:suppr": "true",
                                    "ns5:length": "4",
                                },
                            ],
                        },
                    },
                }
            },
        )

    def test__no_meeting_type(self) -> None:

        frame = Frame(cmd="MESSAGE", headers={}, body="")

        with pytest.raises(InvalidMessage):
            RawMessage.create(frame)
