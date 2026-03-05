from fastapi import APIRouter, Depends, HTTPException
from models import User
from dependencies import getSession
from main import bcrypt_context
from schemas import UserSchema
from sqlalchemy.orm import Session

auth_router = APIRouter(prefix="/auth", tags=["auth"])

@auth_router.post("/signup")
async def signup(user_schema: UserSchema, session: Session = Depends(getSession)):

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
        raise HTTPException(status_code=201, detail="Usuário criado com sucesso!")