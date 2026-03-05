from fastapi import FastAPI
from passlib.context import CryptContext
from dotenv import load_dotenv
import os

load_dotenv()

SECRET_KEY = os.getenv("SECRET_KEY")

app = FastAPI( 
  title="Relatório Folha API",
  version="1.0")

bcrypt_context = CryptContext(schemes=["bcrypt"], deprecated="auto",bcrypt__rounds=12)


from routers.auth_routes import auth_router

app.include_router(auth_router)

#.\venv\Scripts\pip install -r requirements.txt
#.\venv\Scripts\python.exe