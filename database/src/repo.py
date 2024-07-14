from __future__ import annotations

from datetime import datetime, time
import uuid
from sqlalchemy import ForeignKey, create_engine
from sqlalchemy import String, DateTime, Boolean, Time
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import UUID
from clients.src.stomp import WriterInterface
from sqlalchemy.orm import sessionmaker, Session

import models.src.schedule as mod

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
            update_id= str(uuid.uuid4()),
            rid=model.rid,
            uid=model.uid,
            ts=model.ts,
            passenger=model.is_passenger_service
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
    time: Mapped[time] = mapped_column(Time())

    def __repr__(self) -> str:
        return f"Location(update_id={self.update_id!r}, tpl={self.tpl!r}, type={self.type!r}, time_type={self.time_type!r} ts={self.ts!r})"

    @classmethod
    def from_model(cls, model: mod.LocationUpdate, service_update_id: str) -> LocationUpdate:

        return cls(
            update_id= str(uuid.uuid4()),
            service_update_id=service_update_id,
            tpl=model.tpl,
            type=model.type.value,
            time_type=model.time_type.value,
            time=model.timestamp
        )
    
    def __eq__(self, obj: object) -> bool:

        if type(obj) is not ServiceUpdate:
            return False
        
        return True

class DatabaseRepository(WriterInterface):

    def __init__(self, session: Session) -> None:
        self._session = session

    def write(self, msg: mod.ScheduleMessage) -> None:

        print(f"Saving message for {msg.service}")
        with self._session.begin() as session:
            
            service_update = ServiceUpdate.from_model(msg.service)
            session.add(service_update)

            session.flush()

            for loc in msg.locations:
                session.add(LocationUpdate.from_model(loc, service_update.update_id))

            session.flush()
            print(f"Transaction committed")

    @classmethod
    def create(cls, password: str) -> DatabaseRepository:
        return cls(
            session=sessionmaker(
                create_engine(f"postgresql://postgres:{password}@127.0.0.1:5436/postgres")
            )
        )