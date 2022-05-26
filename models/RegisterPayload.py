# generated by datamodel-codegen:
#   filename:  RegisterPayload.json
#   timestamp: 2022-05-26T14:29:25+00:00

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Enum(BaseModel):
    __root__: Any = Field(
        ...,
        description='Generic enumeration.\n\nDerive from this class to define new enumerations.',
        title='Enum',
    )


class RegisterPayload(BaseModel):
    channel: Enum
    destination_bq: str = Field(..., title='Destination Bq')
