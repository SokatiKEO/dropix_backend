"""
Dropix Backend  (PostgreSQL edition)
=====================================
  1. Device Discovery Relay
  2. Transfer Relay (WebSocket proxy)
  3. Push Notifications

Run:
    cp .env.example .env          # fill in DATABASE_URL
    pip install -r requirements.txt
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Annotated, Optional

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db import (
    AsyncSessionLocal,
    Device,
    RoomMember,
    TransferRecord,
    get_db,
    init_db,
)

# Relay sessions stay in-memory — they're ephemeral, seconds-long, no need to persist
_relay_sessions: dict[str, "RelaySession"] = {}

_TTL_SECONDS = 120


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    task = asyncio.create_task(_cleanup_loop())
    yield
    task.cancel()


async def _cleanup_loop():
    """Hard-delete stale room_members rows every 30 s."""
    while True:
        await asyncio.sleep(30)
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=_TTL_SECONDS)
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(RoomMember).where(RoomMember.last_seen < cutoff)
            )
            await session.commit()


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Dropix Backend", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB = Annotated[AsyncSession, Depends(get_db)]


# ══════════════════════════════════════════════════════════════════════════════
# 1. DEVICE DISCOVERY RELAY
# ══════════════════════════════════════════════════════════════════════════════

class RegisterRequest(BaseModel):
    room_code: str
    device_id: str
    device_name: str
    platform: str
    relay_host: Optional[str] = None
    relay_port: Optional[int] = None


class DeviceInfo(BaseModel):
    device_id: str
    device_name: str
    platform: str
    relay_host: Optional[str]
    relay_port: Optional[int]


@app.post("/discovery/register", summary="Register device in a room")
async def register_device(req: RegisterRequest, db: DB):
    # Upsert device
    device = await db.get(Device, req.device_id)
    if device:
        device.device_name = req.device_name
        device.platform = req.platform
        device.updated_at = datetime.now(timezone.utc)
    else:
        db.add(Device(
            device_id=req.device_id,
            device_name=req.device_name,
            platform=req.platform,
        ))

    # Upsert room membership
    result = await db.execute(
        select(RoomMember).where(
            RoomMember.room_code == req.room_code,
            RoomMember.device_id == req.device_id,
        )
    )
    member = result.scalar_one_or_none()
    now = datetime.now(timezone.utc)

    if member:
        member.relay_host = req.relay_host
        member.relay_port = req.relay_port
        member.last_seen = now
    else:
        db.add(RoomMember(
            room_code=req.room_code,
            device_id=req.device_id,
            relay_host=req.relay_host,
            relay_port=req.relay_port,
            last_seen=now,
        ))

    return {"status": "registered", "room_code": req.room_code}


@app.post("/discovery/heartbeat/{room_code}/{device_id}", summary="Keep registration alive")
async def heartbeat(room_code: str, device_id: str, db: DB):
    result = await db.execute(
        select(RoomMember).where(
            RoomMember.room_code == room_code,
            RoomMember.device_id == device_id,
        )
    )
    member = result.scalar_one_or_none()
    if not member:
        raise HTTPException(404, "Device not found — re-register")
    member.last_seen = datetime.now(timezone.utc)
    return {"status": "ok"}


@app.get("/discovery/devices/{room_code}", summary="List live devices in a room")
async def list_devices(
    room_code: str, db: DB, exclude_id: Optional[str] = None
) -> list[DeviceInfo]:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=_TTL_SECONDS)
    q = (
        select(RoomMember, Device)
        .join(Device, Device.device_id == RoomMember.device_id)
        .where(
            RoomMember.room_code == room_code,
            RoomMember.last_seen >= cutoff,
        )
    )
    if exclude_id:
        q = q.where(RoomMember.device_id != exclude_id)

    rows = (await db.execute(q)).all()
    return [
        DeviceInfo(
            device_id=member.device_id,
            device_name=device.device_name,
            platform=device.platform,
            relay_host=member.relay_host,
            relay_port=member.relay_port,
        )
        for member, device in rows
    ]


@app.delete("/discovery/leave/{room_code}/{device_id}", summary="Unregister device")
async def leave_room(room_code: str, device_id: str, db: DB):
    await db.execute(
        delete(RoomMember).where(
            RoomMember.room_code == room_code,
            RoomMember.device_id == device_id,
        )
    )
    return {"status": "left"}


# ══════════════════════════════════════════════════════════════════════════════
# 2. TRANSFER RELAY  (WebSocket proxy — ephemeral, stays in-memory)
# ══════════════════════════════════════════════════════════════════════════════

class RelaySession:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.created_at = time.time()
        self.sender_ws: Optional[WebSocket] = None
        self.receiver_ws: Optional[WebSocket] = None
        # Fired once both sides have attached — only the sender waits on this
        self._both_ready = asyncio.Event()
        # Fired when the pipe is fully done — receiver waits on this
        self._pipe_done = asyncio.Event()

    def attach_sender(self, ws: WebSocket):
        self.sender_ws = ws
        self._check_ready()

    def attach_receiver(self, ws: WebSocket):
        self.receiver_ws = ws
        self._check_ready()

    def _check_ready(self):
        if self.sender_ws is not None and self.receiver_ws is not None:
            self._both_ready.set()

    async def wait_until_ready(self, timeout: float = 60.0):
        """Wait until both sides are connected. Raises asyncio.TimeoutError on timeout."""
        await asyncio.wait_for(self._both_ready.wait(), timeout=timeout)

    async def run_pipe(self):
        """Bidirectional byte pipe. Called once by the sender after both sides ready."""
        async def pipe(src: WebSocket, dst: WebSocket):
            try:
                while True:
                    data = await src.receive_bytes()
                    await dst.send_bytes(data)
            except (WebSocketDisconnect, Exception):
                pass

        await asyncio.gather(
            pipe(self.sender_ws, self.receiver_ws),
            pipe(self.receiver_ws, self.sender_ws),
            return_exceptions=True,
        )
        self._pipe_done.set()

    async def wait_until_done(self, timeout: float = 360.0):
        """Wait for the pipe to finish. Called by the receiver."""
        await asyncio.wait_for(self._pipe_done.wait(), timeout=timeout)


@app.post("/relay/session", summary="Create a relay session")
async def create_relay_session(db: DB):
    session_id = str(uuid.uuid4())
    _relay_sessions[session_id] = RelaySession(session_id)
    db.add(TransferRecord(session_id=session_id))
    return {"session_id": session_id, "expires_in": 300}


@app.websocket("/relay/ws/{session_id}/{role}")
async def relay_ws(websocket: WebSocket, session_id: str, role: str):
    if role not in ("sender", "receiver"):
        await websocket.close(code=4000)
        return

    session = _relay_sessions.get(session_id)
    if not session:
        await websocket.close(code=4004)
        return

    await websocket.accept()

    if role == "sender":
        session.attach_sender(websocket)

        # Sender owns the pipe: wait for receiver to connect, then run the pipe
        try:
            await session.wait_until_ready(timeout=300.0)
        except asyncio.TimeoutError:
            await websocket.close(code=4008)
            _relay_sessions.pop(session_id, None)
            return

        await session.run_pipe()

        # Mark complete in DB
        async with AsyncSessionLocal() as s:
            result = await s.execute(
                select(TransferRecord).where(TransferRecord.session_id == session_id)
            )
            record = result.scalar_one_or_none()
            if record:
                record.success = True
                record.ended_at = datetime.now(timezone.utc)
            await s.commit()

        _relay_sessions.pop(session_id, None)

    else:  # receiver
        session.attach_receiver(websocket)

        # Receiver just waits for the pipe to finish (sender drives it)
        try:
            await session.wait_until_done(timeout=360.0)
        except asyncio.TimeoutError:
            await websocket.close(code=4008)  # Session expired
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# 3. PUSH NOTIFICATIONS
# ══════════════════════════════════════════════════════════════════════════════

class PushTokenRequest(BaseModel):
    device_id: str
    token: str
    platform: str   # android | ios


class NotifyRequest(BaseModel):
    target_device_id: str
    sender_name: str
    file_count: int
    room_code: str


@app.post("/push/register", summary="Register / update push token")
async def register_push_token(req: PushTokenRequest, db: DB):
    device = await db.get(Device, req.device_id)
    if not device:
        raise HTTPException(404, "Device not registered — call /discovery/register first")
    device.push_token = req.token
    device.push_platform = req.platform
    device.updated_at = datetime.now(timezone.utc)
    return {"status": "registered"}


@app.post("/push/notify", summary="Send push notification to a device")
async def send_push_notification(req: NotifyRequest, db: DB):
    device = await db.get(Device, req.target_device_id)
    if not device or not device.push_token:
        raise HTTPException(404, "No push token for this device")

    payload = {
        "title": f"📦 Incoming from {req.sender_name}",
        "body": f"{req.file_count} file{'s' if req.file_count != 1 else ''} waiting",
        "data": {
            "room_code": req.room_code,
            "sender_name": req.sender_name,
            "file_count": req.file_count,
        },
    }

    if device.push_platform == "android":
        # TODO: firebase_admin.messaging.send(...)
        print(f"[FCM]  → {device.push_token[:20]}… {payload}")
    else:
        # TODO: httpx APNs request
        print(f"[APNs] → {device.push_token[:20]}… {payload}")

    return {"status": "sent", "platform": device.push_platform}


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health(db: DB):
    device_count = (await db.execute(select(func.count()).select_from(Device))).scalar()
    return {
        "status": "ok",
        "devices_registered": device_count,
        "relay_sessions_live": len(_relay_sessions),
    }