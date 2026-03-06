from fastapi import FastAPI
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from dotenv import load_dotenv
import os

load_dotenv()

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))

app = FastAPI( 
  title="Relatório Folha API",
  version="1.0")

bcrypt_context = CryptContext(schemes=["bcrypt"], deprecated="auto",bcrypt__rounds=12)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login-form")

from routers.auth_routes import auth_router

app.include_router(auth_router)

#.\venv\Scripts\pip install -r requirements.txt
#.\venv\Scripts\python.exe

#uvicorn main:app --reload