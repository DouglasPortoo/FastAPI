from sqlalchemy import Boolean, Column, Integer, String

from app.db.base import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    username = Column(String, nullable=False)
    email = Column(String, nullable=False, unique=True, index=True)
    password = Column(String, nullable=False)
    admin = Column(Boolean, default=False, nullable=False)

    def __init__(self, username: str, email: str, password: str, admin: bool = False):
        self.username = username
        self.email = email
        self.password = password
        self.admin = admin
