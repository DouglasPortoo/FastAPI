from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from app.dependencies import get_current_user, get_session
from app.models.user import User
from app.schemas.auth import LoginSchema, TokenPairResponse, UserCreateSchema
from app.schemas.common import MessageResponse
from app.services.auth_service import AuthService

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/signup", response_model=MessageResponse, status_code=201)
async def signup(
    payload: UserCreateSchema,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> MessageResponse:
    AuthService(session).create_user(payload=payload, current_user=current_user)
    return MessageResponse(message="Usuário criado com sucesso")


@router.post("/login", response_model=TokenPairResponse)
async def login(
    payload: LoginSchema,
    session: Session = Depends(get_session),
) -> TokenPairResponse:
    return AuthService(session).login(email=payload.email, password=payload.password)


@router.post("/login-form", response_model=TokenPairResponse)
async def login_form(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: Session = Depends(get_session),
) -> TokenPairResponse:
    return AuthService(session).login(email=form_data.username, password=form_data.password)


@router.get("/refresh", response_model=TokenPairResponse)
async def refresh_token(
    current_user: User = Depends(get_current_user),
) -> TokenPairResponse:
    return AuthService.refresh(current_user.id)
