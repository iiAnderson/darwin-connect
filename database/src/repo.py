from __future__ import annotations

import uuid
from datetime import datetime
from datetime import time as dt_time

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    String,
    Time,
    create_engine,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    sessionmaker,
)

import models.src.common as mod
from clients.src.stomp import WriterInterface


class Base(DeclarativeBase):
    pass


class ServiceUpdate(Base):
    __tablename__ = "service_updates"

    update_id: Mapped[str] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rid: Mapped[str] = mapped_column(String(10))
    uid: Mapped[str] = mapped_column(String(10))
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    passenger: Mapped[bool] = mapped_column(Boolean())

    def __repr__(self) -> str:
        return f"ServiceUpdate(update_id={self.update_id!r}, rid={self.rid!r}, ts={self.ts!r})"

    @classmethod
    def from_model(cls, model: mod.ServiceUpdate) -> ServiceUpdate:

        return cls(
            update_id=str(uuid.uuid4()), rid=model.rid, uid=model.uid, ts=model.ts, passenger=model.is_passenger_service
        )

    def __eq__(self, obj: object) -> bool:

        if type(obj) is not ServiceUpdate:
            return False

        return True


class LocationUpdate(Base):
    __tablename__ = "locations"

    update_id: Mapped[str] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_update_id: Mapped[str] = mapped_column(ForeignKey("service_updates.update_id"))

    tpl: Mapped[str] = mapped_column(String(10))
    type: Mapped[str] = mapped_column(String(10))
    time_type: Mapped[str] = mapped_column(String(10))
    time: Mapped[dt_time] = mapped_column(Time())

    def __repr__(self) -> str:
        return f"Location(update_id={self.update_id!r}, tpl={self.tpl!r}, type={self.type!r}, time_type={self.time_type!r} ts={self.ts!r})"

    @classmethod
    def from_model(cls, model: mod.LocationUpdate, service_update_id: str) -> LocationUpdate:

        return cls(
            update_id=str(uuid.uuid4()),
            service_update_id=service_update_id,
            tpl=model.tpl,
            type=model.type.value,
            time_type=model.time_type.value,
            time=model.timestamp,
        )

    def __eq__(self, obj: object) -> bool:

        if type(obj) is not ServiceUpdate:
            return False

        return True


class DatabaseRepository(WriterInterface):

    def __init__(self, session: Session) -> None:
        self._session = session

    def write(self, msg: mod.WritableMessage) -> None:

        print(f"Saving message for {msg.service}")
        with self._session.begin() as session:

            service_update = ServiceUpdate.from_model(msg.service)
            session.add(service_update)

            session.flush()

            for loc in msg.locations:
                session.add(LocationUpdate.from_model(loc, service_update.update_id))

            session.flush()
            print("Transaction committed")

    @classmethod
    def create(cls, password: str) -> DatabaseRepository:
        return cls(session=sessionmaker(create_engine(f"postgresql://postgres:{password}@127.0.0.1:5436/postgres")))
