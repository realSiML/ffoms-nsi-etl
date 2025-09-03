from pydantic import BaseModel, ConfigDict
from sqlalchemy.types import TypeEngine


class ColumnMetadata(BaseModel):
    name: str
    type_: type[TypeEngine] | TypeEngine
    nullable: bool
    primary_key: bool = False
    comment: str
    # for 'type_'
    model_config = ConfigDict(arbitrary_types_allowed=True)


class RefBookMetadata(BaseModel):
    table_name: str
    table_comment: str
    version: str
    update_date: str
    columns_metadata: list[ColumnMetadata]
