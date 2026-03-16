import json
from functools import lru_cache

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ReportDatabaseConfig(BaseModel):
    user: str
    password: str = Field(alias="pass")
    port: str
    hostid: int
    mysql_banco: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = Field(default="Relatório Folha API", validation_alias="APP_NAME")
    app_version: str = Field(default="1.0.0", validation_alias="APP_VERSION")
    api_prefix: str = Field(default="/api", validation_alias="API_PREFIX")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    database_url: str = Field(default="sqlite:///banco.db", validation_alias="DATABASE_URL")
    secret_key: str = Field(default="change-me", validation_alias="SECRET_KEY")
    algorithm: str = Field(default="HS256", validation_alias="ALGORITHM")
    access_token_expire_minutes: int = Field(
        default=30,
        validation_alias="ACCESS_TOKEN_EXPIRE_MINUTES",
    )

    report_output_dir: str = Field(default="generated_reports", validation_alias="REPORT_OUTPUT_DIR")
    report_logo_path: str = Field(default="", validation_alias="REPORT_LOGO_PATH")
    report_server_hostid: int = Field(default=10636, validation_alias="REPORT_SERVER_HOSTID")
    report_mssql_host: str = Field(default="", validation_alias="REPORT_MSSQL_HOST")
    report_db_list: list[ReportDatabaseConfig] = Field(
        default_factory=list,
        validation_alias="REPORT_DB_LIST",
    )
    report_zabbix_host: str = Field(default="", validation_alias="REPORT_ZABBIX_HOST")
    report_zabbix_user: str = Field(default="", validation_alias="REPORT_ZABBIX_USER")
    report_zabbix_pass: str = Field(default="", validation_alias="REPORT_ZABBIX_PASS")
    report_zabbix_db: str = Field(default="zabbix_db", validation_alias="REPORT_ZABBIX_DB")
    report_aux_db: str = Field(default="coleta_bancos", validation_alias="REPORT_AUX_DB")
    report_smtp_server: str = Field(default="", validation_alias="REPORT_SMTP_SERVER")
    report_smtp_port: int = Field(default=587, validation_alias="REPORT_SMTP_PORT")
    report_smtp_user: str = Field(default="", validation_alias="REPORT_SMTP_USER")
    report_smtp_pass: str = Field(default="", validation_alias="REPORT_SMTP_PASS")
    report_from_email: str = Field(default="", validation_alias="REPORT_FROM_EMAIL")
    report_email_recipients: list[str] = Field(
        default_factory=list,
        validation_alias="REPORT_EMAIL_RECIPIENTS",
    )
    report_smtp_timeout_seconds: int = Field(
        default=20,
        validation_alias="REPORT_SMTP_TIMEOUT_SECONDS",
    )
    report_schedule_enabled: bool = Field(default=False, validation_alias="REPORT_SCHEDULE_ENABLED")
    report_schedule_time: str = Field(default="07:00", validation_alias="REPORT_SCHEDULE_TIME")
    report_schedule_run_email: bool = Field(
        default=False,
        validation_alias="REPORT_SCHEDULE_RUN_EMAIL",
    )

    @field_validator("report_db_list", mode="before")
    @classmethod
    def parse_report_db_list(cls, value: object) -> object:
        if value in (None, ""):
            return []
        if isinstance(value, str):
            return json.loads(value)
        return value

    @field_validator("report_email_recipients", mode="before")
    @classmethod
    def parse_recipients(cls, value: object) -> object:
        if value in (None, ""):
            return []
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.startswith("["):
                return json.loads(cleaned)
            return [item.strip() for item in cleaned.split(",") if item.strip()]
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
