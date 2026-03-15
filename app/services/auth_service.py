from datetime import timedelta

from sqlalchemy.orm import Session

from app.core.exceptions import AppError
from app.core.security import create_access_token, hash_password, verify_password
from app.models.user import User
from app.schemas.auth import TokenPairResponse, UserCreateSchema


class AuthService:
    def __init__(self, session: Session):
        self.session = session

    def authenticate_user(self, email: str, password: str) -> User:
        user = self.session.query(User).filter(User.email == email).first()
        if user is None or not verify_password(password, user.password):
            raise AppError("Email ou senha incorretos", status_code=400, code="invalid_credentials")
        return user

    def create_user(self, payload: UserCreateSchema, current_user: User) -> User:
        if not current_user.admin:
            raise AppError(
                "Acesso negado: apenas administradores podem criar novos usuários",
                status_code=403,
                code="forbidden",
            )

        existing_user = self.session.query(User).filter(User.email == payload.email).first()
        if existing_user is not None:
            raise AppError("Email já cadastrado", status_code=400, code="duplicate_email")

        user = User(
            username=payload.username,
            email=payload.email,
            password=hash_password(payload.password),
            admin=payload.admin,
        )
        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        return user

    def login(self, email: str, password: str) -> TokenPairResponse:
        user = self.authenticate_user(email=email, password=password)
        return TokenPairResponse(
            access_token=create_access_token(user.id),
            refresh_token=create_access_token(user.id, expires_delta=timedelta(days=7)),
        )

    @staticmethod
    def refresh(user_id: int) -> TokenPairResponse:
        return TokenPairResponse(access_token=create_access_token(user_id))
