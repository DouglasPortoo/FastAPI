from pydantic import BaseModel
from typing import Optional

class UserSchema(BaseModel):
    username: str
    email: str
    password: str
    admin: Optional[bool]

    class Config:
        from_attributes = True