from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from .database import Base

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)

class Video(Base):
    __tablename__ = "videos"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String)
    status = Column(String, default="uploaded")  # uploaded, processing, completed, error
    user_id = Column(Integer, ForeignKey("users.id"))
    user = relationship("User")
    created_at = Column(DateTime, default=datetime.utcnow)
