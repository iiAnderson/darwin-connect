"""
Unit tests for parsing Kafka JSON messages.

Tests cover the three main message types:
- Schedule (SC) - Train schedule creation/updates
- Train Status (TS) - Train movement and timing updates
- Loading (LO) - Formation loading information
"""
import json

import pytest

from clients.stomp import InvalidMessage, RawMessage


class TestKafkaScheduleMessage:
    """Tests for Schedule (SC) messages from Kafka JSON topic."""

    def test_parse_schedule_message(self) -> None:
        """Test parsing a complete schedule message with association."""
        kafka_message = {
            'destination': {
                'name': 'Consumer.rdmportal.VirtualTopic.PushPort-v18',
                'destinationType': 'queue'
            },
            'messageID': 'ID:liv1-dwnpp102-49747-638894340902160133-1:22:1:1:189977554',
            'type': None,
            'priority': 5,
            'redelivered': False,
            'messageType': 'bytes',
            'deliveryMode': 1,
            'bytes': '{"ts":"2025-11-01T16:55:16.897896+00:00","version":"18.0","uR":{"updateOrigin":"CIS","requestSource":"kt02","requestID":"KeTech2321215190","schedule":{"rid":"202511018750847","uid":"W50847","trainId":"2W20","ssd":"2025-11-01","toc":"NT","OR":{"tpl":"MNCROXR","act":"TB","ptd":"15:27","wtd":"15:27"},"IP":[{"tpl":"MNCRDGT","act":"T ","pta":"15:29","ptd":"15:33","wta":"15:28:30","wtd":"15:33"},{"tpl":"SLFDCT","act":"T ","pta":"15:38","ptd":"15:38","wta":"15:37:30","wtd":"15:38:30"},{"tpl":"BOLTON","act":"T ","pta":"15:49","ptd":"15:49","wta":"15:48:30","wtd":"15:49:30"},{"tpl":"WSTHOTN","act":"T ","pta":"15:57","ptd":"15:57","wta":"15:56:30","wtd":"15:57"},{"tpl":"HINDLEY","act":"T ","pta":"16:01","ptd":"16:01","wta":"16:00:30","wtd":"16:01"},{"tpl":"INCE","act":"T ","pta":"16:04","ptd":"16:04","wta":"16:03:30","wtd":"16:04:30"},{"tpl":"WIGANWL","act":"T ","pta":"16:07","ptd":"16:09","wta":"16:07","wtd":"16:09"},{"tpl":"GATHRST","act":"T ","pta":"16:14","ptd":"16:14","wta":"16:13:30","wtd":"16:14:30"},{"tpl":"APLYBDG","act":"T ","pta":"16:18","ptd":"16:18","wta":"16:17:30","wtd":"16:18:30"},{"tpl":"PARBOLD","act":"T ","pta":"16:23","ptd":"16:23","wta":"16:22:30","wtd":"16:23:30"},{"tpl":"BRSCGHB","act":"T ","pta":"16:28","ptd":"16:28","wta":"16:27:30","wtd":"16:28:30"},{"tpl":"MEOLSCP","act":"T ","pta":"16:36","ptd":"16:37","wta":"16:36","wtd":"16:37"}],"PP":[{"tpl":"WATSTJN","wtp":"15:34"},{"tpl":"ORDSLLJ","wtp":"15:34:30"},{"tpl":"BDENJT","wtp":"15:47:30"},{"tpl":"LOSTCKJ","wtp":"15:53:30"},{"tpl":"CRWNSTJ","wtp":"15:59:30"}],"DT":{"tpl":"SOUTHPT","act":"TF","pta":"16:43","wta":"16:43"}},"association":{"tiploc":"MNCROXR","category":"NP","main":{"rid":"202511018749686","wta":"15:15","pta":"15:15"},"assoc":{"rid":"202511018750847","wtd":"15:27","ptd":"15:27"}}}}',
            'replyTo': None,
            'correlationID': None,
            'expiration': 1762016716898,
            'text': None,
            'map': None,
            'properties': {
                'Username': {
                    'boolean': None,
                    'string': 'thales',
                    'byte': None,
                    'double': None,
                    'bytes': None,
                    'propertyType': 'string',
                    'short': None,
                    'integer': None,
                    'float': None,
                    'long': None
                },
                'PushPortSequence': {
                    'boolean': None,
                    'string': '9977553',
                    'byte': None,
                    'double': None,
                    'bytes': None,
                    'propertyType': 'string',
                    'short': None,
                    'integer': None,
                    'float': None,
                    'long': None
                }
            },
            'timestamp': 1762016116898
        }

        # Parse the message
        raw_message = RawMessage.create_from_kafka_json(kafka_message)

        # Verify message type
        assert raw_message.message_type == "SC"

        # Verify the body contains the parsed JSON
        assert "ts" in raw_message.body
        assert "version" in raw_message.body
        assert "uR" in raw_message.body

        # Verify schedule data structure
        uR = raw_message.body["uR"]
        assert uR["updateOrigin"] == "CIS"
        assert uR["requestSource"] == "kt02"
        assert "schedule" in uR

        # Verify schedule details
        schedule = uR["schedule"]
        assert schedule["rid"] == "202511018750847"
        assert schedule["uid"] == "W50847"
        assert schedule["trainId"] == "2W20"
        assert schedule["ssd"] == "2025-11-01"
        assert schedule["toc"] == "NT"

        # Verify origin (OR)
        origin = schedule["OR"]
        assert origin["tpl"] == "MNCROXR"
        assert origin["act"] == "TB"
        assert origin["ptd"] == "15:27"

        # Verify intermediate points (IP)
        assert len(schedule["IP"]) == 12
        first_ip = schedule["IP"][0]
        assert first_ip["tpl"] == "MNCRDGT"
        assert first_ip["pta"] == "15:29"
        assert first_ip["ptd"] == "15:33"

        # Verify passing points (PP)
        assert len(schedule["PP"]) == 5
        first_pp = schedule["PP"][0]
        assert first_pp["tpl"] == "WATSTJN"
        assert first_pp["wtp"] == "15:34"

        # Verify destination (DT)
        destination = schedule["DT"]
        assert destination["tpl"] == "SOUTHPT"
        assert destination["act"] == "TF"
        assert destination["pta"] == "16:43"

        # Verify association
        assert "association" in uR
        association = uR["association"]
        assert association["tiploc"] == "MNCROXR"
        assert association["category"] == "NP"


class TestKafkaTrainStatusMessage:
    """Tests for Train Status (TS) messages from Kafka JSON topic."""

    def test_parse_train_status_message(self) -> None:
        """Test parsing a train status message with location updates."""
        kafka_message = {
            'destination': {
                'name': 'Consumer.rdmportal.VirtualTopic.PushPort-v18',
                'destinationType': 'queue'
            },
            'messageID': 'ID:liv1-dwnpp102-49747-638894340902160133-1:22:1:1:189977568',
            'type': None,
            'priority': 5,
            'redelivered': False,
            'messageType': 'bytes',
            'deliveryMode': 1,
            'bytes': '{"ts":"2025-11-01T16:55:17.776923+00:00","version":"18.0","uR":{"updateOrigin":"TD","TS":{"rid":"202511017156103","uid":"G56103","ssd":"2025-11-01","Location":[{"tpl":"CRDFCEN","wtd":"16:53","ptd":"16:53","dep":{"et":"16:57","src":"TD"},"plat":"2","length":"3"},{"tpl":"LNGDYKJ","wtp":"16:55:30","pass":{"et":"16:59","src":"Darwin"},"length":"3"},{"tpl":"MSHFILD","wtp":"16:59:30","pass":{"et":"17:03","src":"Darwin"},"length":"3"},{"tpl":"EBBWJ","wtp":"17:02:30","pass":{"et":"17:06","src":"Darwin"},"length":"3"},{"tpl":"NWPTRTG","wta":"17:05","wtd":"17:07","pta":"17:06","ptd":"17:07","arr":{"et":"17:08","src":"Darwin"},"dep":{"et":"17:09","src":"Darwin"},"plat":"4","length":"3"},{"tpl":"MAINDWJ","wtp":"17:08","pass":{"et":"17:10","src":"Darwin"},"length":"3"},{"tpl":"MAINDNJ","wtp":"17:10","pass":{"et":"17:12","src":"Darwin"},"length":"3"},{"tpl":"CWMBRAN","wta":"17:17","wtd":"17:18:30","pta":"17:18","ptd":"17:18","arr":{"et":"17:18","wet":"17:19","src":"Darwin"},"dep":{"et":"17:20","src":"Darwin"},"plat":"2","length":"3"},{"tpl":"PONTYPL","wta":"17:23:30","wtd":"17:25","pta":"17:24","ptd":"17:25","arr":{"et":"17:24","wet":"17:25","src":"Darwin"},"dep":{"et":"17:25","wet":"17:26","src":"Darwin"},"plat":"1","length":"3"},{"tpl":"LTTLMLJ","wtp":"17:27","pass":{"et":"17:28","src":"Darwin"},"length":"3"},{"tpl":"ABRGVNY","wta":"17:33:30","wtd":"17:35","pta":"17:35","ptd":"17:35","arr":{"et":"17:35","wet":"17:34","src":"Darwin"},"dep":{"et":"17:35","src":"Darwin"},"plat":"1","length":"3"},{"tpl":"CREWE","wta":"19:27","wtd":"19:30","pta":"19:28","ptd":"19:30","arr":{"et":"19:28","wet":"19:25","src":"Darwin"},"dep":{"et":"19:30","src":"Darwin"},"plat":"6","length":"3"},{"tpl":"GOOSTRY","wtp":"19:39","pass":{"et":"19:39","src":"Darwin"},"length":"3"},{"tpl":"CHELFD","wtp":"19:42:30","pass":{"et":"19:42","src":"Darwin"},"length":"3"}]}}}',
            'replyTo': None,
            'correlationID': None,
            'expiration': 1762016717776,
            'text': None,
            'map': None,
            'properties': {
                'Username': {
                    'boolean': None,
                    'string': 'thales',
                    'byte': None,
                    'double': None,
                    'bytes': None,
                    'propertyType': 'string',
                    'short': None,
                    'integer': None,
                    'float': None,
                    'long': None
                },
                'PushPortSequence': {
                    'boolean': None,
                    'string': '9977567',
                    'byte': None,
                    'double': None,
                    'bytes': None,
                    'propertyType': 'string',
                    'short': None,
                    'integer': None,
                    'float': None,
                    'long': None
                }
            },
            'timestamp': 1762016117776
        }

        # Parse the message
        raw_message = RawMessage.create_from_kafka_json(kafka_message)

        # Verify message type
        assert raw_message.message_type == "TS"

        # Verify the body contains the parsed JSON
        assert "ts" in raw_message.body
        assert raw_message.body["version"] == "18.0"
        assert "uR" in raw_message.body

        # Verify TS data structure
        uR = raw_message.body["uR"]
        assert uR["updateOrigin"] == "TD"
        assert "TS" in uR

        # Verify train status details
        ts = uR["TS"]
        assert ts["rid"] == "202511017156103"
        assert ts["uid"] == "G56103"
        assert ts["ssd"] == "2025-11-01"

        # Verify locations
        assert "Location" in ts
        locations = ts["Location"]
        assert len(locations) == 14

        # Check first location (departure)
        first_location = locations[0]
        assert first_location["tpl"] == "CRDFCEN"
        assert first_location["wtd"] == "16:53"
        assert first_location["ptd"] == "16:53"
        assert "dep" in first_location
        assert first_location["dep"]["et"] == "16:57"
        assert first_location["dep"]["src"] == "TD"
        assert first_location["plat"] == "2"
        assert first_location["length"] == "3"

        # Check a passing point
        passing_point = locations[1]
        assert passing_point["tpl"] == "LNGDYKJ"
        assert passing_point["wtp"] == "16:55:30"
        assert "pass" in passing_point
        assert passing_point["pass"]["et"] == "16:59"
        assert passing_point["pass"]["src"] == "Darwin"

        # Check a station with both arrival and departure
        station = locations[4]
        assert station["tpl"] == "NWPTRTG"
        assert "arr" in station
        assert "dep" in station
        assert station["arr"]["et"] == "17:08"
        assert station["dep"]["et"] == "17:09"
        assert station["plat"] == "4"

        # Check a station with working estimated times (wet)
        station_with_wet = locations[7]
        assert station_with_wet["tpl"] == "CWMBRAN"
        assert "arr" in station_with_wet
        assert station_with_wet["arr"]["et"] == "17:18"
        assert station_with_wet["arr"]["wet"] == "17:19"


class TestKafkaLoadingMessage:
    """Tests for Loading (LO) messages from Kafka JSON topic."""

    def test_parse_loading_message(self) -> None:
        """Test parsing a formation loading message."""
        kafka_message = {
            'destination': {
                'name': 'Consumer.rdmportal.VirtualTopic.PushPort-v18',
                'destinationType': 'queue'
            },
            'messageID': 'ID:liv1-dwnpp102-49747-638894340902160133-1:22:1:1:189928009',
            'type': None,
            'priority': 5,
            'redelivered': False,
            'messageType': 'bytes',
            'deliveryMode': 1,
            'bytes': '{"ts":"2025-11-01T16:25:40.4069799+00:00","version":"18.0","uR":{"updateOrigin":"CIS","requestSource":"at55","requestID":"0000000000023219","formationLoading":{"fid":"202511018006949-001","rid":"202511018006949","tpl":"ROMFORD","wta":"16:24","wtd":"16:25","pta":"16:24","ptd":"16:25","loading":[{"coachNumber":"1","":"3"},{"coachNumber":"2","":"14"},{"coachNumber":"3","":"14"},{"coachNumber":"4","":"13"},{"coachNumber":"5","":"5"},{"coachNumber":"6","":"14"},{"coachNumber":"7","":"9"},{"coachNumber":"8","":"8"},{"coachNumber":"9","":"10"}]}}}',
            'replyTo': None,
            'correlationID': None,
            'expiration': 1762014940407,
            'text': None,
            'map': None,
            'properties': {
                'Username': {
                    'boolean': None,
                    'string': 'thales',
                    'byte': None,
                    'double': None,
                    'bytes': None,
                    'propertyType': 'string',
                    'short': None,
                    'integer': None,
                    'float': None,
                    'long': None
                },
                'PushPortSequence': {
                    'boolean': None,
                    'string': '9928008',
                    'byte': None,
                    'double': None,
                    'bytes': None,
                    'propertyType': 'string',
                    'short': None,
                    'integer': None,
                    'float': None,
                    'long': None
                }
            },
            'timestamp': 1762014340407
        }

        # Parse the message
        raw_message = RawMessage.create_from_kafka_json(kafka_message)

        # Verify message type
        assert raw_message.message_type == "LO"

        # Verify the body contains the parsed JSON
        assert "ts" in raw_message.body
        assert raw_message.body["version"] == "18.0"
        assert "uR" in raw_message.body

        # Verify formationLoading data structure
        uR = raw_message.body["uR"]
        assert uR["updateOrigin"] == "CIS"
        assert uR["requestSource"] == "at55"
        assert uR["requestID"] == "0000000000023219"
        assert "formationLoading" in uR

        # Verify formation loading details
        formation_loading = uR["formationLoading"]
        assert formation_loading["fid"] == "202511018006949-001"
        assert formation_loading["rid"] == "202511018006949"
        assert formation_loading["tpl"] == "ROMFORD"
        assert formation_loading["wta"] == "16:24"
        assert formation_loading["wtd"] == "16:25"
        assert formation_loading["pta"] == "16:24"
        assert formation_loading["ptd"] == "16:25"

        # Verify loading data
        assert "loading" in formation_loading
        loading = formation_loading["loading"]
        assert len(loading) == 9

        # Check individual coach loading
        coach_1 = loading[0]
        assert coach_1["coachNumber"] == "1"
        assert coach_1[""] == "3"  # Loading percentage/value

        coach_2 = loading[1]
        assert coach_2["coachNumber"] == "2"
        assert coach_2[""] == "14"

        # Verify all coaches are present
        coach_numbers = [coach["coachNumber"] for coach in loading]
        assert coach_numbers == ["1", "2", "3", "4", "5", "6", "7", "8", "9"]


class TestKafkaMessageErrors:
    """Tests for error handling in Kafka message parsing."""

    def test_missing_bytes_field(self) -> None:
        """Test that missing 'bytes' field raises InvalidMessage."""
        kafka_message = {
            'messageID': 'test-id',
            'messageType': 'bytes',
            # 'bytes' field is missing
        }

        with pytest.raises(InvalidMessage, match="Bytes not found"):
            RawMessage.create_from_kafka_json(kafka_message)

    def test_invalid_json_in_bytes(self) -> None:
        """Test that invalid JSON in bytes field raises InvalidMessage."""
        kafka_message = {
            'bytes': 'this is not valid json {{{',
        }

        with pytest.raises(InvalidMessage, match="Invalid JSON"):
            RawMessage.create_from_kafka_json(kafka_message)

    def test_missing_uR_field(self) -> None:
        """Test that missing 'uR' field raises InvalidMessage."""
        kafka_message = {
            'bytes': '{"ts":"2025-11-01T16:55:16.897896+00:00","version":"18.0"}',
        }

        with pytest.raises(InvalidMessage, match="MessageType not found"):
            RawMessage.create_from_kafka_json(kafka_message)

    def test_unknown_message_type(self) -> None:
        """Test that unknown message type in uR raises InvalidMessage."""
        kafka_message = {
            'bytes': '{"ts":"2025-11-01T16:55:16.897896+00:00","version":"18.0","uR":{"updateOrigin":"Test","unknownField":"value"}}',
        }

        with pytest.raises(InvalidMessage, match="Unknown message type"):
            RawMessage.create_from_kafka_json(kafka_message)

    def test_empty_bytes_field(self) -> None:
        """Test that empty bytes field raises InvalidMessage."""
        kafka_message = {
            'bytes': '',
        }

        with pytest.raises(InvalidMessage, match="Invalid JSON"):
            RawMessage.create_from_kafka_json(kafka_message)


class TestKafkaMessageTypes:
    """Tests to verify correct message type detection."""

    def test_detects_schedule_message_type(self) -> None:
        """Verify that messages with 'schedule' in uR are detected as SC."""
        kafka_message = {
            'bytes': '{"uR":{"schedule":{"rid":"test"}}}',
        }

        raw_message = RawMessage.create_from_kafka_json(kafka_message)
        assert raw_message.message_type == "SC"

    def test_detects_train_status_message_type(self) -> None:
        """Verify that messages with 'TS' in uR are detected as TS."""
        kafka_message = {
            'bytes': '{"uR":{"TS":{"rid":"test"}}}',
        }

        raw_message = RawMessage.create_from_kafka_json(kafka_message)
        assert raw_message.message_type == "TS"

    def test_detects_loading_message_type(self) -> None:
        """Verify that messages with 'formationLoading' in uR are detected as LO."""
        kafka_message = {
            'bytes': '{"uR":{"formationLoading":{"fid":"test"}}}',
        }

        raw_message = RawMessage.create_from_kafka_json(kafka_message)
        assert raw_message.message_type == "LO"

    def test_ts_takes_precedence_over_schedule(self) -> None:
        """
        Verify that TS detection happens before schedule detection.
        This ensures correct priority in message type detection.
        """
        kafka_message = {
            'bytes': '{"uR":{"TS":{"rid":"test"},"schedule":{"rid":"other"}}}',
        }

        raw_message = RawMessage.create_from_kafka_json(kafka_message)
        # TS is checked first in the code
        assert raw_message.message_type == "TS"
