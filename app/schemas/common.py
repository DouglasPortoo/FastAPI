from pydantic import BaseModel


class MessageResponse(BaseModel):
    message: str


class ErrorResponse(BaseModel):
    error: str
    message: str
    details: list[dict] | None = None


class HealthResponse(BaseModel):
    status: str
    app_name: str
    version: str
