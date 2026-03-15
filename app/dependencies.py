from collections.abc import Generator

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.security import decode_access_token, oauth2_scheme
from app.db.session import SessionLocal
from app.models.user import User


def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def get_current_user(
    token: str = Depends(oauth2_scheme),
    session: Session = Depends(get_session),
) -> User:
    user_id = decode_access_token(token)
    user = session.query(User).filter(User.id == user_id).first()
    if user is None:
        from app.core.exceptions import AppError

        raise AppError("Token inválido", status_code=401, code="invalid_token")
    return user


getSession = get_session
verify_token = get_current_user
