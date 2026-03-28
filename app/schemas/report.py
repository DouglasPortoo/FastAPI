from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ReportSourceSummary(BaseModel):
    source: str
    configured: bool
    details: dict[str, Any]


class ReportBootstrapResponse(BaseModel):
    status: str
    output_dir: str
    collectors: list[ReportSourceSummary]
    email_enabled: bool


class ReportDatabaseSnapshot(BaseModel):
    database: str
    port: str
    collector_status: str
    details: dict[str, Any] = Field(default_factory=dict)


class ReportResult(BaseModel):
    report_id: str | None = None
    status: str
    generated_at: datetime
    report_path: str | None = None
    run_email: bool
    email_attempted: bool
    email_sent: bool
    sources: list[ReportSourceSummary] = Field(default_factory=list)
    databases: list[ReportDatabaseSnapshot] = Field(default_factory=list)
    problems: list[str] = Field(default_factory=list)


class GenerateReportRequest(BaseModel):
    run_email: bool = True


class ReportGenerationResponse(BaseModel):
    report: ReportResult


class ReportMetadataResponse(BaseModel):
    report_id: str
    status: str
    generated_at: datetime
    report_path: str
    report_exists: bool
    run_email: bool
    email_attempted: bool
    email_sent: bool
    sources: list[ReportSourceSummary] = Field(default_factory=list)
    databases: list[ReportDatabaseSnapshot] = Field(default_factory=list)
    problems: list[str] = Field(default_factory=list)


class ReportEmailResponse(BaseModel):
    report_id: str
    email_sent: bool
    message: str


class ReportJobStatusResponse(BaseModel):
    job_id: str
    status: str
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    run_email: bool
    report_id: str | None = None
    error: str | None = None


class ReportAsyncAcceptedResponse(BaseModel):
    message: str
    job: ReportJobStatusResponse
