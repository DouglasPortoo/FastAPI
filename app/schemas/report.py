from pydantic import BaseModel


class ReportSourceSummary(BaseModel):
    source: str
    configured: bool
    details: dict[str, str | int | bool]


class ReportBootstrapResponse(BaseModel):
    status: str
    output_dir: str
    collectors: list[ReportSourceSummary]
    email_enabled: bool
