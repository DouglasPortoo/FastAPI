from sqlalchemy.orm import sessionmaker, Session
from models import User
from models import db
from fastapi import Depends, HTTPException
from jose import JWTError, jwt
from main import SECRET_KEY, ALGORITHM,oauth2_scheme

def getSession():
  try:
    Session = sessionmaker(bind=db)
    session = Session()
    yield session
  finally:
    session.close()

def verify_token(token: str = Depends(oauth2_scheme), SESSION: Session = Depends(getSession)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
        if user_id is None:
            raise HTTPException(status_code=401, detail="Token inválido")
        user = SESSION.query(User).filter(User.id == user_id).first()
        if user is None:
            raise HTTPException(status_code=401, detail="Token inválido")
        return user
    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido")