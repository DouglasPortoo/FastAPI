import threading
from datetime import datetime

from fastapi import Depends, Request

from app.core.config import get_settings
from app.core.exceptions import AppError
from app.dependencies import get_current_user
from app.models.user import User


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._bucket: dict[str, tuple[str, int]] = {}

    def hit(self, key: str, max_per_minute: int) -> bool:
        minute_key = datetime.utcnow().strftime("%Y%m%d%H%M")
        with self._lock:
            previous = self._bucket.get(key)
            if previous is None or previous[0] != minute_key:
                self._bucket[key] = (minute_key, 1)
                return True

            count = previous[1] + 1
            self._bucket[key] = (minute_key, count)
            return count <= max_per_minute


_rate_limiter = InMemoryRateLimiter()


def _get_client_ip(request: Request) -> str:
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def require_report_security(
    request: Request,
    current_user: User = Depends(get_current_user),
) -> None:
    settings = get_settings()

    if settings.report_require_admin_user and not current_user.admin:
        raise AppError(
            message="Acesso negado: usuário sem permissão administrativa",
            status_code=403,
            code="forbidden",
        )

    if settings.report_rate_limit_per_minute > 0:
        ip_address = _get_client_ip(request)
        if not _rate_limiter.hit(ip_address, settings.report_rate_limit_per_minute):
            raise AppError(
                message="Limite de requisições excedido, tente novamente em instantes",
                status_code=429,
                code="rate_limited",
            )
