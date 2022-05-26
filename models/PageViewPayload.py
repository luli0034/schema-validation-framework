# generated by datamodel-codegen:
#   filename:  PageViewPayload.json
#   timestamp: 2022-05-26T14:29:03+00:00

from __future__ import annotations

from pydantic import UUID4, BaseModel, Field


class PageViewPayload(BaseModel):
    doc_id: UUID4 = Field(..., title='Doc Id')
    destination_bq: str = Field(..., title='Destination Bq')
