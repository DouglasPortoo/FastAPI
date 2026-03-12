from fastapi import APIRouter, Depends, HTTPException
from models import User
from dependencies import getSession, verify_token
from main import bcrypt_context, SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES
from schemas import UserSchema, LoginSchema
from sqlalchemy.orm import Session
from jose import JWTError, jwt
from datetime import datetime, timedelta, timezone
from fastapi.security import OAuth2PasswordRequestForm

auth_router = APIRouter(prefix="/auth", tags=["auth"])

def authenticate_user(email: str, password: str, session: Session):
    user = session.query(User).filter(User.email == email).first()
    if not user:
        return False
    if not bcrypt_context.verify(password, user.password):
        return False
    return user

def create_access_token(user_id: int, token_duration= timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)):
    expire = datetime.now(timezone.utc) + token_duration
    info = {"sub":str(user_id), "exp": expire}
    return jwt.encode(info, SECRET_KEY, algorithm=ALGORITHM)

@auth_router.post("/signup", status_code=201)
async def signup(user_schema: UserSchema, session: Session = Depends(getSession),user: User = Depends(verify_token)):

    if not user.admin:
        raise HTTPException(status_code=403, detail="Acesso negado: apenas administradores podem criar novos usuários")

    if len(user_schema.password) < 6:
        raise HTTPException(status_code=400, detail="Senha muito curta, mínimo 6 caracteres")
    
    user = session.query(User).filter(User.email == user_schema.email).first()
    
    if user:
        raise HTTPException(status_code=400, detail="Email já cadastrado")
    else:
        encrypted_password = bcrypt_context.hash(user_schema.password)
        newUser = User(username=user_schema.username, email=user_schema.email, password=encrypted_password, admin=user_schema.admin)
        session.add(newUser)
        session.commit()
        return {"detail": "Usuário criado com sucesso!"}
    
@auth_router.post("/login")
async def login(login_schema: LoginSchema, session: Session = Depends(getSession)):
    user = authenticate_user(login_schema.email, login_schema.password, session)
    if not user:
        raise HTTPException(status_code=400, detail="Email ou senha incorretos")
    else:
        access_token = create_access_token(user.id)
        refresh_token = create_access_token(user.id, token_duration=timedelta(days=7))
        return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "Bearer"}

@auth_router.post("/login-form")
async def login_form(form_data: OAuth2PasswordRequestForm = Depends(), session: Session = Depends(getSession)):
    user = authenticate_user(form_data.username, form_data.password, session)
    if not user:
        raise HTTPException(status_code=400, detail="Email ou senha incorretos")
    else:
        access_token = create_access_token(user.id)
        return {"access_token": access_token, "token_type": "Bearer"}
    
@auth_router.get("/refresh")
async def refresh_token(user: User = Depends(verify_token)):
    new_access_token = create_access_token(user.id)
    return {"access_token": new_access_token, "token_type": "Bearer"}
