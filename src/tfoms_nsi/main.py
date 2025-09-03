import logging

import requests
import urllib3
from sqlalchemy import (
    Column,
    Date,
    Integer,
    Numeric,
    String,
    Table,
    Text,
    create_engine,
    update,
)
from sqlalchemy.types import TypeEngine

from .config import settings
from .constants import (
    API_BASE_URL,
    DEFAULT_PAGE_SIZE,
    IGNORED_COLUMNS,
    SCHEMA,
)
from .models import Base, RefBookState
from .schemas import ColumnMetadata, RefBookMetadata

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)


def get_refbook_metadata(refbook_code: str) -> RefBookMetadata:
    log.info(f"Fetching refbook metadata for {refbook_code}...")
    # Метаданные таблицы
    response = requests.get(
        f"{API_BASE_URL}/versions",
        params={"identifier": refbook_code, "size": 1},
        verify=False,
    )
    response.raise_for_status()

    data = response.json()

    table_name = data["list"][0]["passport"]["type"].split("-")[0]
    table_comment = (
        data["list"][0]["passport"]["name"]
        + "\n\n"
        + data["list"][0]["passport"]["law"]
    ).rstrip()
    version = data["list"][0]["version"]
    update_date = data["list"][0]["createDate"]

    # Метаданные столбцов таблицы
    response = requests.get(
        f"{API_BASE_URL}/structure",
        params={"identifier": refbook_code},
        verify=False,
    )
    response.raise_for_status()

    data = response.json()

    ## Helper function
    def parse_column_type(
        data_type: str, length: int | None, fract_part_len: int | None
    ) -> type[TypeEngine] | TypeEngine:
        match data_type:
            case "INTEGER" | "REFERENCE":
                return Integer
            case "NUMERIC":
                return Numeric(length, fract_part_len)
            case "VARCHAR" | "INTEGERVARCHAR":
                return String(length)
            case "DATE":
                return Date
            case _:
                return Text

    columns_metadata = [
        ColumnMetadata(
            name=column["Name"],
            type_=parse_column_type(
                column["DataType"],
                column.get("MaxLength"),
                column.get("MaxIntPartLength"),
            ),
            nullable=True,  # column["EmptyAllowed"],  # if column["DataType"] != "DATE" else True,
            comment=column["Description"],
        )
        for column in data["Columns"]
    ]

    for key in data["Keys"]:
        if key["type"] == "PRIMARY":
            for column in columns_metadata:
                if column.name == key["field"]:
                    column.primary_key = True
                    break

    return RefBookMetadata(
        table_name=table_name,
        table_comment=table_comment,
        version=version,
        update_date=update_date,
        columns_metadata=columns_metadata,
    )


def get_refbook_data(refbook_code: str, size: int) -> list[list[dict[str, str]]]:
    all_rows = []
    page = 0

    while True:
        page += 1

        response = requests.get(
            f"{API_BASE_URL}/data",
            params={
                "identifier": refbook_code,
                "page": page,
                "size": size,
            },
            verify=False,
        )
        response.raise_for_status()

        data = response.json()

        pages = data["total"] // size + 1

        all_rows.extend(data["list"])

        log.info(f"page: {page}/{pages}")

        if page >= pages:
            break

    return all_rows


def load_refbook_to_db(code: str, request_size: int = DEFAULT_PAGE_SIZE):
    refbook_metadata = get_refbook_metadata(code)

    # check version

    # EXTRACT
    refbook_data = get_refbook_data(code, size=request_size)

    # TRANSFORM
    refbook_data = [
        {
            item["column"]: (None if item["value"] == "" else item["value"])
            for item in row
            if item["column"] not in IGNORED_COLUMNS
        }
        for row in refbook_data
    ]

    # Создаем таблицу
    columns = [
        Column(**column_metadata.model_dump())
        for column_metadata in refbook_metadata.columns_metadata
    ]
    table = Table(
        refbook_metadata.table_name,
        Base.metadata,
        *columns,
        schema=SCHEMA,
        comment=refbook_metadata.table_comment,
    )

    # LOAD
    engine = create_engine(
        settings.DATABASE_URL,
        echo=settings.DEBUG,
    )

    with engine.connect() as conn:
        table.drop(conn, checkfirst=True)
        table.create(conn)
        conn.execute(table.insert(), refbook_data)
        stmt = (
            update(RefBookState)
            .where(RefBookState.code == code)
            .values(
                table_name=refbook_metadata.table_name,
                version=refbook_metadata.version,
                update_date=refbook_metadata.update_date,
                comment=refbook_metadata.table_comment,
            )
        )
        conn.execute(stmt)
        conn.commit()


if __name__ == "__main__":
    load_refbook_to_db("F003", 50_000)

# Features:
#   Get all refbooks
#   Get one or many refbooks (str | Iterable)
#   FFOMS api & Rosminzdrav api
#   Logging coverage
#   Test coverage xd
#   CLI client?
#   Refbook diffs?
#
# Exceptions:
#   Invalid refbook code
#   Unavailable database
#   Unavailable api
