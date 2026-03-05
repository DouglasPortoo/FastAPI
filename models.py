from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base

db = create_engine("sqlite:///banco.db") #futuramente colocar o caminho do banco de dados em uma variável de ambiente

Base = declarative_base()

class User(Base):
  __tablename__ = "users"

  id = Column("id", Integer, primary_key=True, autoincrement=True, index=True)
  username = Column("username", String)
  email = Column("email", String, nullable=False, unique=True, index=True)
  password = Column("password", String)
  admin = Column("admin", Boolean, default=False)

  def __init__(self, username: str, email: str, password: str, admin: bool = False):
    self.username = username
    self.email = email
    self.password = password
    self.admin = admin