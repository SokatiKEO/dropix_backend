"""
Dropix Backend  (PostgreSQL edition)
=====================================
  1. Device Discovery Relay
  2. Transfer Relay (WebSocket proxy)
  3. Push Notifications
"""

import asyncio
import json
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
    while True:
        await asyncio.sleep(30)
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=_TTL_SECONDS)
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(RoomMember).where(RoomMember.last_seen < cutoff)
            )
            await session.commit()


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Dropix Backend", version="2.1.0", lifespan=lifespan)

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
    relay_sessions: Optional[dict[str, str]] = None


class DeviceInfo(BaseModel):
    device_id: str
    device_name: str
    platform: str
    relay_session_for_me: Optional[str] = None


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

    relay_host_val = json.dumps(req.relay_sessions) if req.relay_sessions else None

    if member:
        member.relay_host = relay_host_val
        member.relay_port = None
        member.last_seen = now
    else:
        db.add(RoomMember(
            room_code=req.room_code,
            device_id=req.device_id,
            relay_host=relay_host_val,
            relay_port=None,
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

    result = []
    for member, device in rows:
        relay_session_for_me: Optional[str] = None
        if member.relay_host and exclude_id:
            try:
                sessions_map: dict = json.loads(member.relay_host)
                relay_session_for_me = sessions_map.get(exclude_id)
            except (json.JSONDecodeError, TypeError):
                relay_session_for_me = member.relay_host

        result.append(DeviceInfo(
            device_id=member.device_id,
            device_name=device.device_name,
            platform=device.platform,
            relay_session_for_me=relay_session_for_me,
        ))

    return result


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
# 2. TRANSFER RELAY  (WebSocket proxy)
# ══════════════════════════════════════════════════════════════════════════════

class RelaySession:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.created_at = time.time()
        self.sender_ws: Optional[WebSocket] = None
        self.receiver_ws: Optional[WebSocket] = None
        self._both_ready = asyncio.Event()
        self._pipe_done = asyncio.Event()

        # Transfer metadata — populated when sender/receiver connect
        self.sender_id: Optional[str] = None
        self.receiver_id: Optional[str] = None
        self.file_count: int = 0
        self.total_bytes: int = 0

    def attach_sender(self, ws: WebSocket, device_id: Optional[str] = None):
        self.sender_ws = ws
        if device_id:
            self.sender_id = device_id
        self._check_ready()

    def attach_receiver(self, ws: WebSocket, device_id: Optional[str] = None):
        self.receiver_ws = ws
        if device_id:
            self.receiver_id = device_id
        self._check_ready()

    def _check_ready(self):
        if self.sender_ws is not None and self.receiver_ws is not None:
            print(f"[SESSION {self.session_id}] BOTH CONNECTED")
            self._both_ready.set()

    async def wait_until_ready(self, timeout: float = 300.0):
        await asyncio.wait_for(self._both_ready.wait(), timeout=timeout)

    async def run_pipe(self):
        """
        Pipe bytes between sender and receiver, counting file_count and
        total_bytes from the Dropix binary protocol framing.

        Dropix wire format (sender → receiver):
          4 bytes  magic (0x44 0x52 0x4F 0x50)
          1 byte   version
          4 bytes  file_count  (uint32 big-endian)
          2 bytes  device_name_len
          N bytes  device_name (UTF-8)
          for each file:
            2 bytes  name_len
            N bytes  name (UTF-8)
            8 bytes  file_size (uint64 big-endian)
          ... file data follows ...

        We parse just the header to capture file_count and total_bytes,
        then forward everything (including the already-read header bytes)
        to the receiver unchanged.
        """
        buf = bytearray()

        async def _read_exactly(ws: WebSocket, n: int) -> bytes:
            """
            Pull exactly n bytes, buffering across WebSocket messages.
            Returns the bytes and leaves any remainder in buf.
            """
            nonlocal buf
            while len(buf) < n:
                chunk = await ws.receive_bytes()
                buf.extend(chunk)
            result = bytes(buf[:n])
            buf = buf[n:]
            return result

        # ── Parse header from sender ───────────────────────────────────────
        try:
            magic    = await _read_exactly(self.sender_ws, 4)
            version  = await _read_exactly(self.sender_ws, 1)
            fc_bytes = await _read_exactly(self.sender_ws, 4)
            file_count = int.from_bytes(fc_bytes, "big")

            # Skip device name
            dn_len_bytes = await _read_exactly(self.sender_ws, 2)
            dn_len = int.from_bytes(dn_len_bytes, "big")
            dn_bytes = await _read_exactly(self.sender_ws, dn_len)

            total_bytes = 0
            file_meta_parts = bytearray()
            for _ in range(file_count):
                name_len_bytes = await _read_exactly(self.sender_ws, 2)
                name_len = int.from_bytes(name_len_bytes, "big")
                name_bytes = await _read_exactly(self.sender_ws, name_len)
                size_bytes = await _read_exactly(self.sender_ws, 8)
                file_size = int.from_bytes(size_bytes, "big")
                total_bytes += file_size

                file_meta_parts += name_len_bytes + name_bytes + size_bytes

            self.file_count = file_count
            self.total_bytes = total_bytes
            print(
                f"[SESSION {self.session_id}] "
                f"files={file_count}, total_bytes={total_bytes}"
            )

            # Re-assemble the full header and forward it to the receiver
            reassembled = (
                magic
                + version
                + fc_bytes
                + dn_len_bytes
                + dn_bytes
                + bytes(file_meta_parts)
                + bytes(buf)   # any extra bytes already buffered
            )
            buf = bytearray()  # reset — everything is in reassembled now
            await self.receiver_ws.send_bytes(reassembled)

        except Exception as e:
            print(f"[SESSION {self.session_id}] Header parse error: {e}")
            self._pipe_done.set()
            return

        # ── Pipe remaining bytes bidirectionally ───────────────────────────
        async def pipe(src: WebSocket, dst: WebSocket, label: str):
            try:
                while True:
                    data = await src.receive_bytes()
                    print(f"[PIPE {label}] {len(data)} bytes")
                    await dst.send_bytes(data)
            except (WebSocketDisconnect, Exception) as e:
                print(f"[PIPE {label}] closed: {e}")

        await asyncio.gather(
            pipe(self.sender_ws, self.receiver_ws, "SENDER → RECEIVER"),
            pipe(self.receiver_ws, self.sender_ws, "RECEIVER → SENDER"),
            return_exceptions=True,
        )
        print("[PIPE] done")
        self._pipe_done.set()

    async def wait_until_done(self, timeout: float = 360.0):
        await asyncio.wait_for(self._pipe_done.wait(), timeout=timeout)


@app.post("/relay/session", summary="Create a relay session")
async def create_relay_session(db: DB):
    session_id = str(uuid.uuid4())
    _relay_sessions[session_id] = RelaySession(session_id)
    db.add(TransferRecord(session_id=session_id))
    return {"session_id": session_id, "expires_in": 300}


@app.websocket("/relay/ws/{session_id}/{role}")
async def relay_ws(
    websocket: WebSocket,
    session_id: str,
    role: str,
    device_id: Optional[str] = None,   # e.g. ?device_id=<uuid>
):
    if role not in ("sender", "receiver"):
        await websocket.close(code=4000)
        return

    session = _relay_sessions.get(session_id)
    if not session:
        await websocket.close(code=4004)
        return

    await websocket.accept()

    if role == "sender":
        print(f"[WS] Sender connected: {session_id} (device_id={device_id})")
        session.attach_sender(websocket, device_id)
        try:
            await session.wait_until_ready(timeout=300.0)
        except asyncio.TimeoutError:
            await websocket.close(code=4008)
            _relay_sessions.pop(session_id, None)
            return

        await session.run_pipe()

        # Update the transfer record with full metadata
        async with AsyncSessionLocal() as s:
            result = await s.execute(
                select(TransferRecord).where(TransferRecord.session_id == session_id)
            )
            record = result.scalar_one_or_none()
            if record:
                record.success     = True
                record.ended_at    = datetime.now(timezone.utc)
                record.sender_id   = session.sender_id
                record.receiver_id = session.receiver_id
                record.file_count  = session.file_count
                record.total_bytes = session.total_bytes
            await s.commit()

        _relay_sessions.pop(session_id, None)

    else:  # receiver
        print(f"[WS] Receiver connected: {session_id} (device_id={device_id})")
        session.attach_receiver(websocket, device_id)
        try:
            await session.wait_until_done(timeout=360.0)
        except asyncio.TimeoutError:
            await websocket.close(code=4008)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# 3. PUSH NOTIFICATIONS
# ══════════════════════════════════════════════════════════════════════════════

class PushTokenRequest(BaseModel):
    device_id: str
    token: str
    platform: str


class NotifyRequest(BaseModel):
    target_device_id: str
    sender_name: str
    file_count: int
    room_code: str


@app.post("/push/register")
async def register_push_token(req: PushTokenRequest, db: DB):
    device = await db.get(Device, req.device_id)
    if not device:
        raise HTTPException(404, "Device not registered")
    device.push_token = req.token
    device.push_platform = req.platform
    device.updated_at = datetime.now(timezone.utc)
    return {"status": "registered"}


@app.post("/push/notify")
async def send_push_notification(req: NotifyRequest, db: DB):
    device = await db.get(Device, req.target_device_id)
    if not device or not device.push_token:
        raise HTTPException(404, "No push token for this device")

    payload = {
        "title": f"📦 Incoming from {req.sender_name}",
        "body": f"{req.file_count} file{'s' if req.file_count != 1 else ''} waiting",
        "data": {"room_code": req.room_code, "sender_name": req.sender_name, "file_count": req.file_count},
    }

    if device.push_platform == "android":
        print(f"[FCM]  → {device.push_token[:20]}… {payload}")
    else:
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