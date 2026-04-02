"""
db.py — async PostgreSQL setup via SQLAlchemy + asyncpg
"""
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Integer,
    String,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+asyncpg://")

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


# ── Tables ─────────────────────────────────────────────────────────────────────

class Device(Base):
    """Registered devices — one row per device_id."""
    __tablename__ = "devices"

    device_id     = Column(String, primary_key=True)
    device_name   = Column(String, nullable=False)
    platform      = Column(String, nullable=False)
    push_token    = Column(String, nullable=True)
    push_platform = Column(String, nullable=True)
    created_at    = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at    = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))


class RoomMember(Base):
    """
    Devices currently active in a discovery room.
    Rows are soft-deleted by last_seen expiry; the cleanup task hard-deletes them.
    """
    __tablename__ = "room_members"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    room_code  = Column(String(16), nullable=False, index=True)
    device_id  = Column(String, nullable=False)
    relay_host = Column(String, nullable=True)
    relay_port = Column(Integer, nullable=True)
    last_seen  = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class TransferRecord(Base):
    """Relay transfer log — one row per session, tracks success and timing only."""
    __tablename__ = "transfer_records"

    id         = Column(BigInteger, primary_key=True, autoincrement=True)
    session_id = Column(String, nullable=False, index=True)
    success    = Column(Boolean, default=False)
    started_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    ended_at   = Column(DateTime(timezone=True), nullable=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

async def init_db():
    """Create all tables if they don't exist."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncSession:
    """FastAPI dependency — yields a session and commits/rolls back."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise