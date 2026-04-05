"""
database/models.py

Changes vs previous version:
- Added `last_turbo_used` column to User — tracks when a free user last
  used their weekly Turbo-Download so the 7-day cooldown can be enforced
  at the DB level (atomic SELECT … FOR UPDATE in UserRepo.use_turbo).
"""

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey,
    Index, Integer, String, Text, UniqueConstraint, func,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id              = Column(BigInteger, primary_key=True)
    language        = Column(String(5), default="ru", nullable=False)
    is_premium      = Column(Boolean, default=False, nullable=False)
    premium_until   = Column(DateTime, nullable=True)
    is_banned       = Column(Boolean, default=False, nullable=False)
    registered_at   = Column(DateTime, server_default=func.now(), nullable=False)

    # Referral system
    referral_code   = Column(String(16), unique=True, nullable=True)
    referred_by     = Column(BigInteger, ForeignKey("users.id"), nullable=True)
    referral_count  = Column(Integer, default=0, nullable=False)

    # Turbo-Demo: weekly single free download for non-premium users.
    # NULL means "never used" (turbo is available).
    last_turbo_used = Column(DateTime, nullable=True)

    downloads       = relationship("Download", back_populates="user", lazy="noload")

    __table_args__ = (
        UniqueConstraint("referral_code", name="uq_users_referral_code"),
        Index("idx_users_referral_code", "referral_code"),
        Index("idx_users_is_premium", "is_premium"),
        Index("idx_users_registered_at", "registered_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<User id={self.id} lang={self.language} "
            f"premium={self.is_premium} banned={self.is_banned} "
            f"turbo={self.last_turbo_used}>"
        )


class Download(Base):
    __tablename__ = "downloads"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    user_id    = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    platform   = Column(String(50), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    user = relationship("User", back_populates="downloads", lazy="noload")

    __table_args__ = (
        Index("idx_downloads_user_created", "user_id", "created_at"),
        Index("idx_downloads_platform", "platform"),
        Index("idx_downloads_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<Download id={self.id} user={self.user_id} platform={self.platform}>"


class Ad(Base):
    __tablename__ = "ads"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    message_text = Column(Text, nullable=False)
    is_active    = Column(Boolean, default=True, nullable=False)
    created_at   = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_ads_is_active", "is_active"),
    )

    def __repr__(self) -> str:
        preview = self.message_text[:40].replace("\n", " ")
        return f"<Ad id={self.id} active={self.is_active} text='{preview}…'>"


class BroadcastLog(Base):
    __tablename__ = "broadcast_logs"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    admin_id    = Column(BigInteger, nullable=False)
    message     = Column(Text, nullable=False)
    total       = Column(Integer, default=0)
    success     = Column(Integer, default=0)
    blocked     = Column(Integer, default=0)
    failed      = Column(Integer, default=0)
    cancelled   = Column(Boolean, default=False)
    created_at  = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_broadcast_logs_admin_id", "admin_id"),
        Index("idx_broadcast_logs_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<BroadcastLog id={self.id} admin={self.admin_id} "
            f"success={self.success}/{self.total}>"
        )