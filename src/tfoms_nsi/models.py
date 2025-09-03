from datetime import date
from typing import Optional

from sqlalchemy import Date, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .constants import SCHEMA


class Base(DeclarativeBase):
    pass


class RefBookState(Base):
    __tablename__ = "_CATALOG"
    __table_args__ = {"schema": SCHEMA}

    code: Mapped[str] = mapped_column(String(50), primary_key=True)
    table_name: Mapped[Optional[str]] = mapped_column(String(50))
    version: Mapped[Optional[str]] = mapped_column(String(50))
    update_date: Mapped[Optional[date]] = mapped_column(Date)
    comment: Mapped[Optional[str]] = mapped_column(String(255))

    def __repr__(self):
        return (
            f"<RefBookState(code='{self.code}', table_name='{self.table_name}', "
            f"version='{self.version}', update_date={self.update_date}, comment='{self.comment}')>"
        )
