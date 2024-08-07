

from datetime import datetime
from unittest.mock import Mock
from unittest import mock

from freezegun import freeze_time

import models.src.schedule as mod
from database.src.repo import DatabaseRepository, LocationUpdate, ServiceUpdate


class TestDatabaseRepository:

    @freeze_time("2024-07-13")
    @mock.patch("uuid.uuid4")
    @mock.patch("sqlalchemy.orm.Session")
    def test(self, mock_session: Mock, mock_uuid: Mock) -> None:

        mock_context = Mock()
        mock_uuid.return_value = "abc-def"

        mock_session.begin.return_value.__enter__.return_value = mock_context

        repo = DatabaseRepository(mock_session)

        msg = mod.ScheduleMessage(
            locations=[
                mod.LocationUpdate(tpl="GLGC", type=mod.LocationType.ARR, timestamp=datetime(1900, 1, 1, 0, 29)),
                mod.LocationUpdate(tpl="EKILBRD", type=mod.LocationType.DEP, timestamp=datetime(1900, 1, 1, 23, 57)),
                mod.LocationUpdate(tpl="HARMYRS", type=mod.LocationType.ARR, timestamp=datetime(1900, 1, 1, 0, 1)),
            ],
            service=mod.ServiceUpdate("202406258080789", "P80789", datetime.now(), False),
        )

        repo.write(msg)

        mock_context.add.assert_any_call(ServiceUpdate(update_id="abc-def", rid="202406258080789", uid="P80789", ts=datetime.now()))
        mock_context.add.assert_any_call(LocationUpdate(update_id="abc-def", tpl="GLGC", type="ARR", ts=datetime(1900, 1, 1, 0, 29)))
        mock_context.add.assert_any_call(LocationUpdate(update_id="abc-def", tpl="EKILBRD", type="DEP", ts=datetime(1900, 1, 1, 23, 57)))
        mock_context.add.assert_any_call(LocationUpdate(update_id="abc-def", tpl="HARMYRS", type="ARR", ts=datetime(1900, 1, 1, 0, 1)))

        mock_context.commit.assert_called()