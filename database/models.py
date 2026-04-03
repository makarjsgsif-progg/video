from sqlalchemy import Column, Integer, String, Boolean, DateTime, BigInteger, Text, ForeignKey, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True)
    language = Column(String(2), default="en")
    is_premium = Column(Boolean, default=False)
    premium_until = Column(DateTime, nullable=True)
    is_banned = Column(Boolean, default=False)
    registered_at = Column(DateTime, server_default=func.now())


class Download(Base):
    __tablename__ = "downloads"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.id"))
    platform = Column(String(50))
    created_at = Column(DateTime, server_default=func.now())


class Ad(Base):
    __tablename__ = "ads"

    id = Column(Integer, primary_key=True)
    message_text = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
