import logging
import re


class SensitiveDataFilter(logging.Filter):
    _rules = [
        re.compile(r"(Authorization\s*:\s*Bearer\s+)[^\s]+", re.IGNORECASE),
        re.compile(r"(X-API-Key\s*:\s*)[^\s]+", re.IGNORECASE),
        re.compile(r"(password=)[^\s&]+", re.IGNORECASE),
        re.compile(r"(pwd=)[^\s&]+", re.IGNORECASE),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True

        for pattern in self._rules:
            message = pattern.sub(r"\1***", message)

        record.msg = message
        record.args = ()
        return True


def configure_logging(level: str) -> None:
    root_logger = logging.getLogger()
    sensitive_filter = SensitiveDataFilter()

    for handler in root_logger.handlers:
        handler.addFilter(sensitive_filter)

    if root_logger.handlers:
        root_logger.setLevel(level.upper())
        return

    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    for handler in root_logger.handlers:
        handler.addFilter(sensitive_filter)
