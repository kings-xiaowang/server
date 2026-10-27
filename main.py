# -*- coding: utf-8 -*-
"""
Furry Talking IM 后端服务（第一阶段）
- 账号云化：注册 / 登录，签发 JWT（HMAC-SHA256，零第三方依赖）
- 好友系统：申请 / 同意 / 拒绝 / 列表 / 用户搜索
- 单聊 + 群聊统一会话模型：conversations(type: single/group) + members
- 消息可靠性：客户端生成 msg_id，服务端 (sender_id, msg_id) 幂等去重；
  每会话消息 seq 自增；消息落库后再广播；客户端可按 after_seq 增量拉取
- 存储双模式：DATABASE_URL 环境变量存在则用 PostgreSQL（psycopg2），否则 SQLite（WAL + 索引）

启动：python main.py （默认 0.0.0.0:8000；PORT 环境变量可覆盖端口；DATABASE_URL 指向 PostgreSQL）
"""
import base64
import hashlib
import hmac
import json
import os
import random
import sqlite3
import string
import time
from pathlib import Path
from typing import Optional

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None

from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DB_PATH = Path(__file__).parent / "furrytalk.db"
IS_PG = bool(DATABASE_URL)
if IS_PG and psycopg2 is None:
    raise RuntimeError("DATABASE_URL 已设置但未安装 psycopg2，请先执行 pip install psycopg2-binary")
SECRET = os.environ.get("JWT_SECRET", "furry-talking-im-secret-2026")
TOKEN_TTL = 7 * 24 * 3600

app = FastAPI(title="Furry Talking IM Server")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- 存储双模式（PostgreSQL / SQLite） ----------------

def _pg_sql(sql: str) -> str:
    """把统一 SQL 适配为 PostgreSQL：'?' 占位符 -> '%s'，字面量 '%' 转义为 '%%'，
    INSERT OR IGNORE 转为 INSERT ... ON CONFLICT DO NOTHING。"""
    sql = sql.replace("%", "%%")
    sql = sql.replace("?", "%s")
    if "INSERT OR IGNORE INTO" in sql:
        sql = sql.replace("INSERT OR IGNORE INTO", "INSERT INTO", 1)
        if not sql.endswith(";"):
            sql += ";"
        sql = sql[:-1] + " ON CONFLICT DO NOTHING;"
    return sql


def _execute(conn, sql: str, params=()):
    """统一执行：SQLite 直接执行，PostgreSQL 走 cursor + 适配后的 SQL。"""
    if IS_PG:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(_pg_sql(sql), params)
        return cur
    return conn.execute(sql, params)


def _insert(conn, sql: str, params=()):
    """执行 INSERT 并返回自增主键 id（两种数据库兼容）。"""
    if IS_PG:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        adapted = _pg_sql(sql).rstrip()
        if adapted.endswith(";"):
            adapted = adapted[:-1]
        cur.execute(adapted + " RETURNING id", params)
        row = cur.fetchone()
        return int(row["id"]) if row else 0
    cur = conn.execute(sql, params)
    return cur.lastrowid


def _begin_immediate(conn):
    """事务加锁：SQLite 用 BEGIN IMMEDIATE，PostgreSQL 用 BEGIN。"""
    if IS_PG:
        conn.execute("BEGIN")
    else:
        conn.execute("BEGIN IMMEDIATE")


def get_db():
    if IS_PG:
        return psycopg2.connect(DATABASE_URL)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


_PG_DDL = [
    """CREATE TABLE IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        phone TEXT NOT NULL UNIQUE,
        password TEXT NOT NULL,
        nickname TEXT NOT NULL,
        created_at TEXT NOT NULL
    );""",
    "CREATE INDEX IF NOT EXISTS idx_users_phone ON users(phone);",
    """CREATE TABLE IF NOT EXISTS friendships (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        friend_id BIGINT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL,
        UNIQUE(user_id, friend_id)
    );""",
    "CREATE INDEX IF NOT EXISTS idx_friendships_user ON friendships(user_id, status);",
    "CREATE INDEX IF NOT EXISTS idx_friendships_friend ON friendships(friend_id, status);",
    """CREATE TABLE IF NOT EXISTS conversations (
        id BIGSERIAL PRIMARY KEY,
        type TEXT NOT NULL DEFAULT 'group',
        name TEXT NOT NULL DEFAULT '',
        description TEXT NOT NULL DEFAULT '',
        join_code TEXT UNIQUE,
        owner_id BIGINT NOT NULL,
        created_at TEXT NOT NULL
    );""",
    "CREATE INDEX IF NOT EXISTS idx_conversations_type ON conversations(type);",
    """CREATE TABLE IF NOT EXISTS conversation_members (
        id BIGSERIAL PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(conversation_id, user_id)
    );""",
    "CREATE INDEX IF NOT EXISTS idx_cm_conversation ON conversation_members(conversation_id);",
    "CREATE INDEX IF NOT EXISTS idx_cm_user ON conversation_members(user_id);",
    """CREATE TABLE IF NOT EXISTS messages (
        id BIGSERIAL PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        sender_id BIGINT NOT NULL,
        sender_name TEXT NOT NULL,
        msg_id TEXT NOT NULL,
        seq BIGINT NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(sender_id, msg_id),
        UNIQUE(conversation_id, seq)
    );""",
    "CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id, seq);",
]


def init_db():
    conn = get_db()
    if IS_PG:
        for stmt in _PG_DDL:
            conn.execute(stmt)
    else:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                nickname TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_users_phone ON users(phone);

            CREATE TABLE IF NOT EXISTS friendships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                friend_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                UNIQUE(user_id, friend_id)
            );
            CREATE INDEX IF NOT EXISTS idx_friendships_user ON friendships(user_id, status);
            CREATE INDEX IF NOT EXISTS idx_friendships_friend ON friendships(friend_id, status);

            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL DEFAULT 'group',
                name TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                join_code TEXT UNIQUE,
                owner_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_conversations_type ON conversations(type);

            CREATE TABLE IF NOT EXISTS conversation_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(conversation_id, user_id)
            );
            CREATE INDEX IF NOT EXISTS idx_cm_conversation ON conversation_members(conversation_id);
            CREATE INDEX IF NOT EXISTS idx_cm_user ON conversation_members(user_id);

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id INTEGER NOT NULL,
                sender_id INTEGER NOT NULL,
                sender_name TEXT NOT NULL,
                msg_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(sender_id, msg_id),
                UNIQUE(conversation_id, seq)
            );
            CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id, seq);
            """
        )
    conn.commit()
    conn.close()


def generate_join_code(length: int = 6) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=length))


# ---------------- JWT ----------------

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def make_token(user_id: int, phone: str, nickname: str) -> str:
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64url(
        json.dumps({
            "uid": user_id,
            "phone": phone,
            "nick": nickname,
            "exp": int(time.time()) + TOKEN_TTL,
        }).encode()
    )
    sig = _b64url(
        hmac.new(SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()
    )
    return f"{header}.{payload}.{sig}"


def verify_token(token: str) -> Optional[dict]:
    try:
        header, payload, sig = token.split(".")
        expected = _b64url(
            hmac.new(SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()
        )
        if not hmac.compare_digest(sig, expected):
            return None
        data = json.loads(_b64url_decode(payload))
        if data.get("exp", 0) < time.time():
            return None
        return data
    except Exception:
        return None


def auth_user(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未登录")
    payload = verify_token(authorization[7:].strip())
    if not payload:
        raise HTTPException(status_code=401, detail="登录已过期")
    return payload


def hash_password(password: str, salt: str = "") -> str:
    if not salt:
        salt = "".join(random.choices(string.ascii_letters + string.digits, k=8))
    digest = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}${digest}"


def check_password(password: str, stored: str) -> bool:
    try:
        salt, digest = stored.split("$", 1)
        return hmac.compare_digest(hash_password(password, salt), stored)
    except Exception:
        return False


# ---------------- 数据模型 ----------------

class RegisterBody(BaseModel):
    phone: str
    password: str
    nickname: str


class LoginBody(BaseModel):
    phone: str
    password: str


class ApplyBody(BaseModel):
    user_id: int


class RespondBody(BaseModel):
    request_id: int
    action: str  # accept | reject


class JoinBody(BaseModel):
    join_code: str


class CreateChannelBody(BaseModel):
    name: str
    description: str = "私密群，凭群码加入"


class CreateSingleBody(BaseModel):
    user_id: int


class SendBody(BaseModel):
    conversation_id: int
    sender_id: int
    sender_name: str
    msg_id: str = ""
    content: str


# ---------------- 工具函数 ----------------

def user_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "phone": row["phone"],
        "nickname": row["nickname"],
        "created_at": row["created_at"],
    }


def conversation_to_dict(conn, row, me: Optional[int] = None) -> dict:
    d = {
        "id": row["id"],
        "type": row["type"],
        "name": row["name"],
        "description": row["description"],
        "join_code": row["join_code"] or "",
        "owner_id": row["owner_id"],
        "created_at": row["created_at"],
        "member_count": _execute(conn, 
            "SELECT COUNT(*) FROM conversation_members WHERE conversation_id = ?",
            (row["id"],),
        ).fetchone()[0],
        "display_name": row["name"],
        "peer_id": None,
        "peer_nickname": None,
    }
    if row["type"] == "single" and me is not None:
        peer = _execute(conn, 
            "SELECT u.* FROM conversation_members cm JOIN users u ON u.id = cm.user_id "
            "WHERE cm.conversation_id = ? AND cm.user_id != ? LIMIT 1",
            (row["id"], me),
        ).fetchone()
        if peer:
            d["peer_id"] = peer["id"]
            d["peer_nickname"] = peer["nickname"]
            d["display_name"] = peer["nickname"]
    last = _execute(conn, 
        "SELECT seq, content, created_at FROM messages WHERE conversation_id = ? "
        "ORDER BY seq DESC LIMIT 1",
        (row["id"],),
    ).fetchone()
    d["last_seq"] = last["seq"] if last else 0
    d["last_content"] = last["content"] if last else ""
    return d


def message_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "conversation_id": row["conversation_id"],
        "sender_id": row["sender_id"],
        "sender_name": row["sender_name"],
        "msg_id": row["msg_id"],
        "seq": row["seq"],
        "content": row["content"],
        "created_at": row["created_at"],
    }


def insert_message(conn, conversation_id: int, sender_id: int, sender_name: str,
                   msg_id: str, content: str) -> Optional[dict]:
    """事务内分配 seq 并落库；msg_id 重复时返回 None（幂等丢弃）。"""
    if not msg_id:
        return None
    try:
        _begin_immediate(conn)
    except Exception:
        pass  # 已在事务中
    cur = _execute(conn, 
        "SELECT COALESCE(MAX(seq), 0) + 1 FROM messages WHERE conversation_id = ?",
        (conversation_id,),
    )
    seq = cur.fetchone()[0]
    cur = _execute(conn, 
        "INSERT OR IGNORE INTO messages "
        "(conversation_id, sender_id, sender_name, msg_id, seq, content, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (conversation_id, sender_id, sender_name, msg_id, seq, content,
         time.strftime("%Y-%m-%d %H:%M:%S")),
    )
    if cur.rowcount == 0:
        conn.rollback()
        return None
    conn.commit()
    return _execute(conn, "SELECT * FROM messages WHERE sender_id = ? AND msg_id = ?",
                        (sender_id, msg_id)).fetchone()


# ---------------- HTTP API ----------------

@app.get("/api/health")
def health():
    return {"status": "ok", "time": time.time()}


# ---- 账号云化 ----

@app.post("/api/register")
def register(body: RegisterBody):
    phone = body.phone.strip()
    nickname = body.nickname.strip()
    if len(phone) != 11 or not phone.isdigit():
        return {"ok": False, "error": "手机号需为 11 位数字"}
    if len(body.password) < 6:
        return {"ok": False, "error": "密码至少 6 位"}
    if not nickname:
        return {"ok": False, "error": "昵称不能为空"}
    conn = get_db()
    if _execute(conn, "SELECT 1 FROM users WHERE phone = ?", (phone,)).fetchone():
        conn.close()
        return {"ok": False, "error": "该手机号已注册"}
    new_id = _insert(conn, 
        "INSERT INTO users (phone, password, nickname, created_at) VALUES (?, ?, ?, ?)",
        (phone, hash_password(body.password), nickname, time.strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    row = _execute(conn, "SELECT * FROM users WHERE id = ?", (new_id,)).fetchone()
    conn.close()
    user = user_to_dict(row)
    return {"ok": True, "token": make_token(user["id"], user["phone"], user["nickname"]),
            "user": user}


@app.post("/api/login")
def login(body: LoginBody):
    phone = body.phone.strip()
    conn = get_db()
    row = _execute(conn, "SELECT * FROM users WHERE phone = ?", (phone,)).fetchone()
    conn.close()
    if row is None or not check_password(body.password, row["password"]):
        return {"ok": False, "error": "手机号或密码错误"}
    user = user_to_dict(row)
    return {"ok": True, "token": make_token(user["id"], user["phone"], user["nickname"]),
            "user": user}


# ---- 用户搜索 / 好友 ----

@app.get("/api/users/search")
def search_users(keyword: str = "", me: dict = Depends(auth_user)):
    conn = get_db()
    kw = f"%{keyword.strip()}%"
    rows = _execute(conn, 
        "SELECT * FROM users WHERE id != ? AND (nickname LIKE ? OR phone LIKE ?) "
        "ORDER BY id ASC LIMIT 50",
        (me["uid"], kw, kw),
    ).fetchall()
    conn.close()
    return {"users": [user_to_dict(r) for r in rows]}


@app.get("/api/friends")
def list_friends(me: dict = Depends(auth_user)):
    conn = get_db()
    rows = _execute(conn, 
        "SELECT u.* FROM friendships f JOIN users u ON "
        "  (f.user_id = ? AND f.friend_id = u.id) OR (f.friend_id = ? AND f.user_id = u.id) "
        "WHERE f.status = 'accepted' AND u.id != ? ORDER BY u.id ASC",
        (me["uid"], me["uid"], me["uid"]),
    ).fetchall()
    conn.close()
    return {"friends": [user_to_dict(r) for r in rows]}


@app.get("/api/friend-requests")
def list_friend_requests(me: dict = Depends(auth_user)):
    conn = get_db()
    rows = _execute(conn, 
        "SELECT f.id AS request_id, u.id AS user_id, u.nickname, u.phone, f.created_at "
        "FROM friendships f JOIN users u ON u.id = f.user_id "
        "WHERE f.friend_id = ? AND f.status = 'pending' ORDER BY f.id DESC",
        (me["uid"],),
    ).fetchall()
    conn.close()
    return {"requests": [dict(r) for r in rows]}


@app.post("/api/friends/apply")
def apply_friend(body: ApplyBody, me: dict = Depends(auth_user)):
    if body.user_id == me["uid"]:
        return {"ok": False, "error": "不能添加自己"}
    conn = get_db()
    _execute(conn, 
        "INSERT OR IGNORE INTO friendships (user_id, friend_id, status, created_at) "
        "VALUES (?, ?, 'pending', ?)",
        (me["uid"], body.user_id, time.strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/friends/respond")
def respond_friend(body: RespondBody, me: dict = Depends(auth_user)):
    action = body.action.strip().lower()
    if action not in ("accept", "reject"):
        return {"ok": False, "error": "action 必须为 accept 或 reject"}
    conn = get_db()
    row = _execute(conn, 
        "SELECT * FROM friendships WHERE id = ? AND friend_id = ? AND status = 'pending'",
        (body.request_id, me["uid"]),
    ).fetchone()
    if row is None:
        conn.close()
        return {"ok": False, "error": "申请不存在或已处理"}
    _execute(conn, 
        "UPDATE friendships SET status = ? WHERE id = ?",
        ("accepted" if action == "accept" else "rejected", body.request_id),
    )
    conn.commit()
    conn.close()
    return {"ok": True}


# ---- 会话（单聊 + 群聊统一模型） ----

@app.get("/api/conversations")
def list_conversations(me: dict = Depends(auth_user)):
    conn = get_db()
    rows = _execute(conn, 
        "SELECT c.* FROM conversations c "
        "JOIN conversation_members cm ON cm.conversation_id = c.id "
        "WHERE cm.user_id = ? ORDER BY c.id DESC",
        (me["uid"],),
    ).fetchall()
    result = [conversation_to_dict(conn, r, me["uid"]) for r in rows]
    conn.close()
    return {"conversations": result}


@app.post("/api/conversations/create_single")
def create_single(body: CreateSingleBody, me: dict = Depends(auth_user)):
    if body.user_id == me["uid"]:
        return {"ok": False, "error": "不能和自己单聊"}
    conn = get_db()
    # 查找已有单聊会话（双方互为成员）
    row = _execute(conn, 
        "SELECT c.id FROM conversations c "
        "WHERE c.type = 'single' AND EXISTS (SELECT 1 FROM conversation_members m "
        "  WHERE m.conversation_id = c.id AND m.user_id = ?) AND "
        "EXISTS (SELECT 1 FROM conversation_members m "
        "  WHERE m.conversation_id = c.id AND m.user_id = ?) LIMIT 1",
        (me["uid"], body.user_id),
    ).fetchone()
    if row is None:
        cid = _insert(conn, 
            "INSERT INTO conversations (type, name, description, join_code, owner_id, created_at) "
            "VALUES ('single', '', '', NULL, ?, ?)",
            (me["uid"], time.strftime("%Y-%m-%d %H:%M:%S")),
        )
        _execute(conn, 
            "INSERT INTO conversation_members (conversation_id, user_id, created_at) VALUES (?, ?, ?)",
            (cid, me["uid"], time.strftime("%Y-%m-%d %H:%M:%S")),
        )
        _execute(conn, 
            "INSERT INTO conversation_members (conversation_id, user_id, created_at) VALUES (?, ?, ?)",
            (cid, body.user_id, time.strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
        row = _execute(conn, "SELECT * FROM conversations WHERE id = ?", (cid,)).fetchone()
    else:
        row = _execute(conn, "SELECT * FROM conversations WHERE id = ?", (row["id"],)).fetchone()
    result = conversation_to_dict(conn, row, me["uid"])
    conn.close()
    return {"ok": True, "conversation": result}


@app.get("/api/channels")
def list_channels():
    """公开群频道列表（兼容旧客户端；全部 group 会话）。"""
    conn = get_db()
    rows = _execute(conn, 
        "SELECT * FROM conversations WHERE type = 'group' ORDER BY id ASC"
    ).fetchall()
    result = [conversation_to_dict(conn, r) for r in rows]
    conn.close()
    return {"channels": result}


@app.post("/api/channels/join")
def join_channel(body: JoinBody, me: dict = Depends(auth_user)):
    code = body.join_code.strip().upper()
    if not code:
        return {"ok": False, "error": "群码为空"}
    conn = get_db()
    row = _execute(conn, 
        "SELECT * FROM conversations WHERE type = 'group' AND join_code = ?", (code,)
    ).fetchone()
    if row is None:
        conn.close()
        return {"ok": False, "error": "群码无效"}
    _execute(conn, 
        "INSERT OR IGNORE INTO conversation_members (conversation_id, user_id, created_at) "
        "VALUES (?, ?, ?)",
        (row["id"], me["uid"], time.strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    result = conversation_to_dict(conn, row, me["uid"])
    conn.close()
    return {"ok": True, "channel": result}


@app.post("/api/channels/create")
def create_channel(body: CreateChannelBody, me: dict = Depends(auth_user)):
    name = body.name.strip()
    if not name:
        return {"ok": False, "error": "群名不能为空"}
    conn = get_db()
    code = generate_join_code()
    while _execute(conn, "SELECT 1 FROM conversations WHERE join_code = ?", (code,)).fetchone():
        code = generate_join_code()
    cid = _insert(conn, 
        "INSERT INTO conversations (type, name, description, join_code, owner_id, created_at) "
        "VALUES ('group', ?, ?, ?, ?, ?)",
        (name, body.description, code, me["uid"], time.strftime("%Y-%m-%d %H:%M:%S")),
    )
    _execute(conn, 
        "INSERT INTO conversation_members (conversation_id, user_id, created_at) VALUES (?, ?, ?)",
        (cid, me["uid"], time.strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    row = _execute(conn, "SELECT * FROM conversations WHERE id = ?", (cid,)).fetchone()
    result = conversation_to_dict(conn, row, me["uid"])
    conn.close()
    return {"ok": True, "channel": result}


# ---- 消息 ----

@app.get("/api/messages")
def get_messages(conversation_id: int, after_seq: int = 0, me: dict = Depends(auth_user)):
    conn = get_db()
    if after_seq > 0:
        rows = _execute(conn, 
            "SELECT * FROM messages WHERE conversation_id = ? AND seq > ? ORDER BY seq ASC",
            (conversation_id, after_seq),
        ).fetchall()
    else:
        rows = _execute(conn, 
            "SELECT * FROM messages WHERE conversation_id = ? ORDER BY seq ASC LIMIT 500",
            (conversation_id,),
        ).fetchall()
    conn.close()
    return {"messages": [message_to_dict(r) for r in rows]}


@app.post("/api/messages")
async def send_message_http(body: SendBody, me: dict = Depends(auth_user)):
    content = body.content.strip()
    if not content:
        return {"ok": False, "error": "消息不能为空"}
    conn = get_db()
    row = insert_message(conn, body.conversation_id, me["uid"], body.sender_name,
                         body.msg_id, content)
    conn.close()
    if row is None:
        return {"ok": False, "error": "msg_id 重复或无效"}
    payload = {"type": "message", "message": message_to_dict(row)}
    await manager.broadcast(body.conversation_id, payload)
    return {"ok": True, "message": message_to_dict(row)}


# ---------------- WebSocket ----------------

class ConnectionManager:
    def __init__(self):
        self.rooms: dict[int, set[WebSocket]] = {}

    async def connect(self, conversation_id: int, ws: WebSocket):
        await ws.accept()
        self.rooms.setdefault(conversation_id, set()).add(ws)

    def disconnect(self, conversation_id: int, ws: WebSocket):
        room = self.rooms.get(conversation_id)
        if room:
            room.discard(ws)
            if not room:
                self.rooms.pop(conversation_id, None)

    async def broadcast(self, conversation_id: int, payload: dict):
        room = self.rooms.get(conversation_id, set())
        if not room:
            return
        text = json.dumps(payload, ensure_ascii=False)
        dead = []
        for ws in room:
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(conversation_id, ws)


manager = ConnectionManager()


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, conversation_id: int, user_id: int,
                      nickname: str = "", token: str = ""):
    if token:
        payload = verify_token(token)
        if not payload or payload.get("uid") != user_id:
            await ws.close(code=4401)
            return
    conn = get_db()
    member = _execute(conn, 
        "SELECT 1 FROM conversation_members WHERE conversation_id = ? AND user_id = ?",
        (conversation_id, user_id),
    ).fetchone()
    conn.close()
    if member is None:
        await ws.close(code=4403)
        return
    await manager.connect(conversation_id, ws)
    try:
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if data.get("type") != "send":
                continue
            content = (data.get("content") or "").strip()
            msg_id = (data.get("msg_id") or "").strip()
            if not content or not msg_id:
                continue
            conn = get_db()
            row = insert_message(conn, conversation_id, user_id, nickname, msg_id, content)
            conn.close()
            if row is None:
                continue  # 幂等：重复 msg_id 不广播
            payload = {"type": "message", "message": message_to_dict(row)}
            await manager.broadcast(conversation_id, payload)
    except WebSocketDisconnect:
        manager.disconnect(conversation_id, ws)


if __name__ == "__main__":
    init_db()
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
