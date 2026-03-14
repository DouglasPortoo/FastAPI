from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional

class UserSchema(BaseModel):
    username: str
    email: str
    password: str
    admin: Optional[bool]

    class Config:
        from_attributes = True

class LoginSchema(BaseModel):
    email: str
    password: str

    class Config:
        from_attributes = True


class ReportGenerateRequestSchema(BaseModel):
    ports: Optional[list[str]] = Field(default=None)
    send_email: bool = False
    recipients: Optional[list[str]] = Field(default=None)


class ReportGenerateResponseSchema(BaseModel):
    report_id: str
    file_name: str
    download_url: str
    generated_at: datetime
    emailed: bool
    warnings: list[str]
    selected_ports: list[str]