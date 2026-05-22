from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import math
import os
import re
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


def _load_dotenv(path: str = ".env") -> None:
    """Minimal .env loader without external dependencies.

    Lines must use KEY=VALUE. Quotes around values are optional.
    Existing environment variables are not overwritten.
    """
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv()


_REQUIRED_PACKAGES: dict[str, str] = {
    "aiogram": "aiogram>=3.6,<4",
    "aiohttp": "aiohttp>=3.9,<4",
    "aiosqlite": "aiosqlite>=0.19",
}


def _truthy_env(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in {"0", "false", "no", "off", "нет"}


def _ensure_runtime_dependencies() -> None:
    """Install missing runtime dependencies when AUTO_INSTALL_DEPS is enabled.

    Disable with AUTO_INSTALL_DEPS=0 in production if dependencies are installed by
    Docker/venv/requirements.txt.
    """
    if not _truthy_env("AUTO_INSTALL_DEPS", "1"):
        return
    missing = [pkg for module, pkg in _REQUIRED_PACKAGES.items() if importlib.util.find_spec(module) is None]
    if not missing:
        return
    print(f"[bootstrap] Installing missing Python packages: {', '.join(missing)}", file=sys.stderr)
    subprocess.check_call([
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        *missing,
    ])


_ensure_runtime_dependencies()

import aiosqlite
import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "bot.db")
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
ADMIN_IDS: set[int] = {int(x) for x in re.findall(r"\d+", ADMIN_IDS_RAW)}

MIN_DEPOSIT = float(os.getenv("MIN_DEPOSIT", "10"))
MIN_WITHDRAW = float(os.getenv("MIN_WITHDRAW", "30"))

REF_PERCENT_DEFAULT = int(os.getenv("REF_PERCENT", "10"))

TGRASS_API_TOKEN = os.getenv("TGRASS_API_TOKEN", "").strip()
TGRASS_API_URL = "https://tgrass.space"

SUBGRAM_API_KEY = os.getenv("SUBGRAM_API_KEY", "").strip()
SUBGRAM_URL = "https://api.subgram.org/get-sponsors"
SUBGRAM_CHECK_URL = "https://api.subgram.org/get-user-subscriptions"

BOTOHUB_API_KEY = os.getenv("BOTOHUB_API_KEY", "").strip()
BOTOHUB_URL = "https://botohub.me/get-tasks"

_last_auto_check: dict[int, int] = {}  
    
async def get_tgrass_offers(user_id: int, username: str | None, lang: str, is_premium: bool | None = False, limit: int = 5) -> dict:
    """Получает офферы из Tgrass API."""
    token = await db.cfg_get("tgrass_api_token", "")
    if not token:
        token = TGRASS_API_TOKEN
    if not token:
        return {"status": "no_token"}
    
    async with aiohttp.ClientSession() as s:
        try:
            payload = {
                "tg_user_id": int(user_id),
                "tg_login": username,
                "lang": lang or "ru",
                "is_premium": is_premium or False,
            }
            if limit:
                payload["offers_limit"] = limit
            
            async with s.post(
                f"{TGRASS_API_URL}/offers",
                json=payload,
                headers={
                    "accept": "application/json",
                    "Content-Type": "application/json",
                    "Auth": token,
                },
                timeout=10
            ) as r:
                data = await r.json()
                log.info("Tgrass response: status=%s offers=%s", data.get("status"), len(data.get("offers", [])))
                return data
        except Exception as e:
            log.warning("Tgrass API error: %s", e)
            return {"status": "error"}
        
async def get_botohub_tasks(user_id: int) -> dict | None:
    """Получает задания из Botohub API."""
    token = await db.cfg_get("botohub_api_token", "")
    if not token:
        token = BOTOHUB_API_KEY
    if not token:
        return None
    
    headers = {"Auth": token, "Content-Type": "application/json"}
    payload = {"chat_id": user_id}
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(BOTOHUB_URL, headers=headers, json=payload, timeout=10) as response:
                data = await response.json()
                log.info("Botohub response: tasks=%s completed=%s", len(data.get("tasks", [])), data.get("completed"))
                return data
        except Exception as e:
            log.warning("Botohub API error: %s", e)
            return None             
        
async def reset_tgrass_offers(user_id: int) -> bool:
    """Сбрасывает офферы для пользователя."""
    token = await db.cfg_get("tgrass_api_token", "")
    if not token:
        return False
    
    async with aiohttp.ClientSession() as s:
        try:
            async with s.post(
                f"{TGRASS_API_URL}/reset_offers",
                json={"tg_user_id": int(user_id)},
                headers={
                    "accept": "application/json",
                    "Content-Type": "application/json",
                    "Auth": token,
                },
                timeout=10
            ) as r:
                data = await r.json()
                return data.get("status") == "ok"
        except Exception as e:
            log.warning("Tgrass reset error: %s", e)
            return False        


async def get_ref_percent() -> int:
    v = await db.cfg_get("ref_percent", "")
    if v:
        try: return int(float(v))
        except ValueError: pass
    return REF_PERCENT_DEFAULT


async def get_min_deposit() -> float:
    v = await db.cfg_get("min_deposit", "")
    if v:
        try: return float(v)
        except ValueError: pass
    return MIN_DEPOSIT


async def get_min_withdraw() -> float:
    v = await db.cfg_get("min_withdraw", "")
    if v:
        try: return float(v)
        except ValueError: pass
    return MIN_WITHDRAW

async def get_max_resources() -> int:
    """Максимальное количество ресурсов на пользователя."""
    v = await db.cfg_get("max_resources", "3")
    try:
        return max(1, int(float(v)))
    except ValueError:
        return 3

async def get_min_order_qty(kind: str) -> int:
    """Минимальное кол-во для заказа: kind in {channel_sub, chat_join, post_view}."""
    key = {
        "channel_sub": "min_order_channel",
        "chat_join":   "min_order_chat",
        "post_view":   "min_order_view",
        "bot_start":   "min_order_bot",
    }.get(kind)
    if not key:
        return 1
    v = await db.cfg_get(key, "")
    if v:
        try: return max(1, int(float(v)))
        except ValueError: pass
    return 1


async def get_buy_price(key: str) -> float:
    """Цена за 1 единицу для покупателя трафика."""
    val = await db.cfg_get(f"buy_price_{key}", "")
    if val:
        try: return float(val)
        except ValueError: pass
    return await db.get_price(key)


async def get_sell_price(key: str) -> float:
    """Выплата за 1 единицу исполнителю (продавцу трафика)."""
    val = await db.cfg_get(f"sell_price_{key}", "")
    if val:
        try: return float(val)
        except ValueError: pass
    return await db.get_price(key)


async def get_ads_commission() -> float:
    """% комиссии владельца бота с заказов post_in_channel (0-100)."""
    val = await db.cfg_get("ads_commission_percent", "")
    if val:
        try:
            v = float(val)
            return max(0.0, min(100.0, v))
        except ValueError: pass
    return 0.0

DEFAULTS = {
    "view_post": 0.05,
    "chat_join": 0.30,
    "channel_sub": 0.50,
    "bot_start": 0.50,
    "reaction": 0.10,
    "auto_view": 0.10,
    "owner_post_per_hour": 0.50,
    "owner_chat_per_hour": 1.00,
    "price_view_post": 0.05,
    "price_auto_views": 0.10,
    "price_chat_join": 0.30,
    "price_channel_sub": 0.50,
    "price_bot_start": 0.50,
    "price_reaction": 0.10,
    "owner_post_payout": 0.50,
    "owner_chat_payout": 1.00,
}

CATEGORIES = [
    ("moderation", "🛡 Модерация"),
    ("bot",        "🤖 Бот"),
    ("pr",         "📣 Пиар/вз чат"),
    ("game",       "🎮 Игровой чат"),
    ("dating",     "🔞 18+/Знакомства"),
    ("misc",       "⚙️ Разное"),
]
CATEGORY_BY_KEY = dict(CATEGORIES)

BIND_TIMES_HOURS = [1, 3, 12, 24]
CHANNEL_COUNT_OPTIONS = [1, 2, 3, 4, 5]
RES_PAGE_SIZE = 5
PIC_PAGE_SIZE = 6 


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("traffic-bot")


DB_INIT_SQL = """
CREATE TABLE IF NOT EXISTS users (
    tg_id        INTEGER PRIMARY KEY,
    username     TEXT,
    first_name   TEXT,
    balance      REAL    NOT NULL DEFAULT 0,
    ref_earnings REAL    NOT NULL DEFAULT 0,
    ref_id       INTEGER,
    is_banned    INTEGER NOT NULL DEFAULT 0,
    created_at   INTEGER NOT NULL,
    -- настройки для подключённого чата:
    chat_channels_count INTEGER DEFAULT 5,
    chat_bind_hours     INTEGER DEFAULT 1,
    chat_self_category  TEXT    DEFAULT 'misc',
    chat_show_categories TEXT   DEFAULT 'moderation,bot,pr,game,dating,misc'
);

CREATE TABLE IF NOT EXISTS subgram_cache (
    user_id     INTEGER NOT NULL,
    chat_id     INTEGER NOT NULL,
    sponsors    TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (user_id, chat_id)
);

CREATE TABLE IF NOT EXISTS tgrass_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    offer_id    INTEGER NOT NULL,
    channel_name TEXT,
    channel_link TEXT,
    action      TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS auto_views_channels (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    chat_id     INTEGER NOT NULL,
    username    TEXT,
    title       TEXT,
    views_per_post INTEGER NOT NULL DEFAULT 10,
    daily_limit    INTEGER DEFAULT 0,
    is_active      INTEGER NOT NULL DEFAULT 1,
    created_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS auto_views_posts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  INTEGER NOT NULL,
    original_chat_id INTEGER,
    original_msg_id  INTEGER,
    our_chat_id  INTEGER,
    our_msg_id   INTEGER,
    created_at   INTEGER NOT NULL,
    UNIQUE(channel_id, original_chat_id, original_msg_id)
);

CREATE TABLE IF NOT EXISTS resources (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    type        TEXT    NOT NULL,        -- 'channel' | 'chat' | 'bot'
    tg_chat_id  INTEGER,                 -- numeric id (-100... for channels/groups)
    username    TEXT,
    title       TEXT,
    category    TEXT    DEFAULT 'misc',
    is_active   INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL,
    kind         TEXT    NOT NULL,       -- 'channel_sub' | 'chat_join' | 'bot_start' | 'post_view' | 'reaction'
    target_link  TEXT,                   -- ссылка/username/post link
    target_chat_id INTEGER,
    post_text    TEXT,                   -- для post_view (если был сгенерирован)
    post_id      INTEGER,                -- message_id поста для просмотров
    category     TEXT    DEFAULT 'misc',
    quantity     INTEGER NOT NULL,       -- сколько нужно действий
    completed    INTEGER NOT NULL DEFAULT 0,
    duration_h   INTEGER NOT NULL DEFAULT 1,  -- сколько часов держать в каналах/чатах
    expires_at   INTEGER,
    price_total  REAL    NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'active', -- active|paused|done|cancelled
    created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS placements (
    -- размещения постов в каналах партнёров
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    INTEGER NOT NULL,
    resource_id INTEGER NOT NULL,        -- канал, где размещён
    placed_msg_id INTEGER,
    placed_at   INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    payout      REAL    NOT NULL,
    paid        INTEGER NOT NULL DEFAULT 0,
    status      TEXT    NOT NULL DEFAULT 'live'  -- live|done|removed
);

CREATE TABLE IF NOT EXISTS completions (
    -- факты выполнения (юзер кликнул просмотрел / вступил в чат / подписался)
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id   INTEGER NOT NULL,
    user_id    INTEGER NOT NULL,
    kind       TEXT    NOT NULL,
    payout     REAL    NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(order_id, user_id)
);

CREATE TABLE IF NOT EXISTS transactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    amount      REAL    NOT NULL,         -- + поступление, - списание
    kind        TEXT    NOT NULL,         -- deposit|withdraw|spend|earn|ref|admin
    comment     TEXT,
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS start_op (
    -- ОП на /start: каналы, на которые надо подписаться чтобы начать
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id    INTEGER,
    username   TEXT,
    title      TEXT,
    invite_url TEXT
);

CREATE TABLE IF NOT EXISTS blacklist (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_chat_id  INTEGER,
    username    TEXT,
    reason      TEXT,
    created_at  INTEGER NOT NULL,
    UNIQUE(tg_chat_id),
    UNIQUE(username)
);

CREATE INDEX IF NOT EXISTS idx_blacklist_tg_chat_id ON blacklist(tg_chat_id);
CREATE INDEX IF NOT EXISTS idx_blacklist_username ON blacklist(username);

CREATE TABLE IF NOT EXISTS op_cards (
    -- карточки заданий (ОП) в подключённых чатах
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_id INTEGER NOT NULL,   -- chat resource id
    order_id    INTEGER NOT NULL,
    tg_chat_id  INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    UNIQUE(resource_id, order_id)
);
"""


@dataclass
class User:
    tg_id: int
    username: str | None
    first_name: str | None
    balance: float
    ref_earnings: float
    ref_id: int | None
    is_banned: int
    chat_channels_count: int
    chat_bind_hours: int
    chat_self_category: str
    chat_show_categories: str


class DB:
    def __init__(self, path: str):
        self.path = path

    @asynccontextmanager
    async def conn(self):
        async with aiosqlite.connect(self.path) as c:
            c.row_factory = aiosqlite.Row
            yield c

    async def init(self) -> None:
        async with self.conn() as c:
            await c.executescript(DB_INIT_SQL)
            async def cols(table: str) -> set[str]:
                cur = await c.execute(f"PRAGMA table_info({table})")
                return {r[1] for r in await cur.fetchall()}

            res_cols = await cols("resources")
            if "price_per_hour" not in res_cols:
                await c.execute("ALTER TABLE resources ADD COLUMN price_per_hour REAL DEFAULT 50")
            if "members_count" not in res_cols:
                await c.execute("ALTER TABLE resources ADD COLUMN members_count INTEGER DEFAULT 0")
            if "members_updated_at" not in res_cols:
                await c.execute("ALTER TABLE resources ADD COLUMN members_updated_at INTEGER DEFAULT 0")
            if "bind_hours" not in res_cols:
                await c.execute("ALTER TABLE resources ADD COLUMN bind_hours INTEGER DEFAULT 1")
            if "max_tasks" not in res_cols:
                await c.execute("ALTER TABLE resources ADD COLUMN max_tasks INTEGER DEFAULT 5")
            if "show_categories" not in res_cols:
                await c.execute(
                    "ALTER TABLE resources ADD COLUMN show_categories TEXT "
                    "DEFAULT 'moderation,bot,pr,game,dating,misc'"
                )
            if "max_placements" not in res_cols:
                await c.execute("ALTER TABLE resources ADD COLUMN max_placements INTEGER DEFAULT 3")

            ord_cols = await cols("orders")
            if "frozen_payout" not in ord_cols:
                await c.execute("ALTER TABLE orders ADD COLUMN frozen_payout REAL DEFAULT 0")
            if "target_resource_id" not in ord_cols:
                await c.execute("ALTER TABLE orders ADD COLUMN target_resource_id INTEGER")
            if "copy_chat_id" not in ord_cols:
                await c.execute("ALTER TABLE orders ADD COLUMN copy_chat_id INTEGER")
            if "copy_msg_id" not in ord_cols:
                await c.execute("ALTER TABLE orders ADD COLUMN copy_msg_id INTEGER")

            # Проверяем колонку language_code в users
            user_cols = await cols("users")
            if "language_code" not in user_cols:
                await c.execute("ALTER TABLE users ADD COLUMN language_code TEXT")

            # Проверяем колонку comment в completions
            comp_cols = await cols("completions")
            if "comment" not in comp_cols:
                await c.execute("ALTER TABLE completions ADD COLUMN comment TEXT")

            await c.executescript("""
            CREATE INDEX IF NOT EXISTS idx_users_ref_id ON users(ref_id);
            CREATE INDEX IF NOT EXISTS idx_resources_user_active ON resources(user_id, is_active);
            CREATE INDEX IF NOT EXISTS idx_resources_type_chat_active ON resources(type, tg_chat_id, is_active);
            CREATE INDEX IF NOT EXISTS idx_orders_user_status ON orders(user_id, status);
            CREATE INDEX IF NOT EXISTS idx_orders_status_kind_category ON orders(status, kind, category);
            CREATE INDEX IF NOT EXISTS idx_orders_target_chat ON orders(target_chat_id);
            CREATE INDEX IF NOT EXISTS idx_placements_status_expires ON placements(status, expires_at);
            CREATE INDEX IF NOT EXISTS idx_placements_resource_status ON placements(resource_id, status);
            CREATE INDEX IF NOT EXISTS idx_completions_user_created ON completions(user_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_transactions_user_created ON transactions(user_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_tgrass_logs_user_action_created ON tgrass_logs(user_id, action, created_at);
            CREATE INDEX IF NOT EXISTS idx_auto_views_channels_user_active ON auto_views_channels(user_id, is_active);
            CREATE INDEX IF NOT EXISTS idx_auto_views_posts_channel_msg ON auto_views_posts(channel_id, original_msg_id);
            """)

            await c.commit()

    async def get_price(self, key: str) -> float:
        val = await self.cfg_get(f"price_{key}", "")
        if val:
            try:
                return float(val)
            except ValueError:
                pass
        return float(DEFAULTS.get(f"price_{key}", DEFAULTS.get(key, 0)))

    async def get_user(self, tg_id: int) -> User | None:
        async with self.conn() as c:
            cur = await c.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
            row = await cur.fetchone()
            if not row:
                return None
            return User(**{k: row[k] for k in User.__dataclass_fields__})

    async def upsert_user(self, tg_id: int, username: str | None, first_name: str | None,
                          ref_id: int | None = None) -> User:
        now = int(time.time())
        async with self.conn() as c:
            cur = await c.execute("SELECT 1 FROM users WHERE tg_id=?", (tg_id,))
            exists = await cur.fetchone()
            if exists:
                await c.execute(
                    "UPDATE users SET username=?, first_name=? WHERE tg_id=?",
                    (username, first_name, tg_id),
                )
            else:
                await c.execute(
                    "INSERT INTO users(tg_id, username, first_name, ref_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (tg_id, username, first_name, ref_id, now),
                )
            await c.commit()
        u = await self.get_user(tg_id)
        assert u is not None
        return u

    async def update_user_settings(self, tg_id: int, **kwargs: Any) -> None:
        if not kwargs:
            return
        cols = ", ".join(f"{k}=?" for k in kwargs)
        vals = list(kwargs.values()) + [tg_id]
        async with self.conn() as c:
            await c.execute(f"UPDATE users SET {cols} WHERE tg_id=?", vals)
            await c.commit()

    async def add_balance(self, tg_id: int, amount: float, kind: str, comment: str = "") -> None:
        now = int(time.time())
        async with self.conn() as c:
            await c.execute("UPDATE users SET balance = balance + ? WHERE tg_id=?", (amount, tg_id))
            await c.execute(
                "INSERT INTO transactions(user_id, amount, kind, comment, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (tg_id, amount, kind, comment, now),
            )
            await c.commit()

    async def list_transactions(self, tg_id: int, limit: int = 10) -> list[dict]:
        async with self.conn() as c:
            cur = await c.execute(
                "SELECT * FROM transactions WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (tg_id, limit),
            )
            return [dict(r) for r in await cur.fetchall()]

    async def add_resource(self, user_id: int, type_: str, tg_chat_id: int | None,
                           username: str | None, title: str | None, category: str = "misc") -> int:
        now = int(time.time())
        async with self.conn() as c:
            cur = await c.execute(
                "INSERT INTO resources(user_id, type, tg_chat_id, username, title, category, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (user_id, type_, tg_chat_id, username, title, category, now),
            )
            await c.commit()
            return cur.lastrowid or 0

    async def list_resources(self, user_id: int) -> list[dict]:
        async with self.conn() as c:
            cur = await c.execute(
                "SELECT * FROM resources WHERE user_id=? AND is_active=1 ORDER BY id DESC",
                (user_id,),
            )
            return [dict(r) for r in await cur.fetchall()]

    async def get_resource(self, res_id: int) -> dict | None:
        async with self.conn() as c:
            cur = await c.execute("SELECT * FROM resources WHERE id=?", (res_id,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def deactivate_resource(self, res_id: int, user_id: int) -> None:
        async with self.conn() as c:
            await c.execute(
                "UPDATE resources SET is_active=0 WHERE id=? AND user_id=?",
                (res_id, user_id),
            )
            await c.commit()

    async def update_resource(self, res_id: int, user_id: int, **kwargs: Any) -> None:
        if not kwargs:
            return
        cols = ", ".join(f"{k}=?" for k in kwargs)
        vals = list(kwargs.values()) + [res_id, user_id]
        async with self.conn() as c:
            await c.execute(
                f"UPDATE resources SET {cols} WHERE id=? AND user_id=?", vals
            )
            await c.commit()

    async def create_order(self, **kwargs: Any) -> int:
        kwargs.setdefault("created_at", int(time.time()))
        cols = ", ".join(kwargs.keys())
        ph = ", ".join(["?"] * len(kwargs))
        async with self.conn() as c:
            cur = await c.execute(
                f"INSERT INTO orders({cols}) VALUES ({ph})", list(kwargs.values())
            )
            await c.commit()
            return cur.lastrowid or 0

    async def list_user_orders(self, user_id: int, limit: int = 200) -> list[dict]:
        async with self.conn() as c:
            cur = await c.execute(
                "SELECT * FROM orders WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            )
            return [dict(r) for r in await cur.fetchall()]

    async def get_order(self, order_id: int) -> dict | None:
        async with self.conn() as c:
            cur = await c.execute("SELECT * FROM orders WHERE id=?", (order_id,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def cancel_order(self, order_id: int, user_id: int) -> bool:
        async with self.conn() as c:
            cur = await c.execute(
                "UPDATE orders SET status='cancelled' WHERE id=? AND user_id=? AND status='active'",
                (order_id, user_id),
            )
            await c.commit()
            return (cur.rowcount or 0) > 0

    async def cfg_get(self, key: str, default: str = "") -> str:
        async with self.conn() as c:
            cur = await c.execute("SELECT value FROM config WHERE key=?", (key,))
            row = await cur.fetchone()
            return row["value"] if row else default

    async def cfg_set(self, key: str, value: str) -> None:
        async with self.conn() as c:
            await c.execute(
                "INSERT INTO config(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            await c.commit()

    async def list_start_op(self) -> list[dict]:
        async with self.conn() as c:
            cur = await c.execute("SELECT * FROM start_op ORDER BY id")
            return [dict(r) for r in await cur.fetchall()]

    async def add_start_op(self, chat_id: int | None, username: str | None,
                           title: str | None, invite_url: str | None) -> None:
        async with self.conn() as c:
            await c.execute(
                "INSERT INTO start_op(chat_id, username, title, invite_url) VALUES(?, ?, ?, ?)",
                (chat_id, username, title, invite_url),
            )
            await c.commit()

    async def del_start_op(self, op_id: int) -> None:
        async with self.conn() as c:
            await c.execute("DELETE FROM start_op WHERE id=?", (op_id,))
            await c.commit()

    async def update_start_op_chat_id(self, op_id: int, chat_id: int) -> None:
        async with self.conn() as c:
            await c.execute("UPDATE start_op SET chat_id=? WHERE id=?", (chat_id, op_id))
            await c.commit()

    async def stats(self) -> dict[str, Any]:
        async with self.conn() as c:
            async def one(sql: str) -> int:
                cur = await c.execute(sql)
                row = await cur.fetchone()
                return int(row[0]) if row else 0
            return {
                "users": await one("SELECT COUNT(*) FROM users"),
                "users_today": await one(
                    "SELECT COUNT(*) FROM users WHERE created_at>=strftime('%s','now','-1 day')"),
                "resources": await one("SELECT COUNT(*) FROM resources WHERE is_active=1"),
                "orders_active": await one("SELECT COUNT(*) FROM orders WHERE status='active'"),
                "orders_total": await one("SELECT COUNT(*) FROM orders"),
                "balance_total": float(
                    (await (await c.execute("SELECT IFNULL(SUM(balance),0) FROM users")).fetchone())[0] or 0
                ),
                "spent_total": float(
                    (await (await c.execute(
                        "SELECT IFNULL(SUM(-amount),0) FROM transactions WHERE kind='spend'")
                     ).fetchone())[0] or 0
                ),
            }


db = DB(DB_PATH)


def kb_main(is_admin: bool = False) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📲 Ваш кабинет")],
        [KeyboardButton(text="🛍️ Продать трафик"), KeyboardButton(text="🛒 Купить трафик")],
        [KeyboardButton(text="ℹ️ О нас")],  
    ]
    if is_admin:
        rows.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def ikb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=d) for t, d in row]
        for row in rows
    ])


def kb_sell_root() -> InlineKeyboardMarkup:
    return ikb([
        [("📂 Мои ресурсы", "sell:list")],
        [("📢 Подключить канал", "sell:add:channel"), ("💬 Подключить чат", "sell:add:chat")],
        [("◀️ Назад", "menu:main")],
    ])


def kb_buy_root() -> InlineKeyboardMarkup:
    return ikb([
        [("🤖 Бот", "buy:bot_start")],
        [("👥 Канал", "buy:channel_sub"), ("👥 Чат", "buy:chat_join")],
        [("👁 Просмотры поста", "buy:post_view"), ("👁 Пост в канале", "buy:post_in_channel")],
        [("👁‍🗨 Автопросмотры", "buy:auto_views")],
        [("Мои заказы", "buy:list")],
    ])


def kb_categories(prefix: str) -> InlineKeyboardMarkup:
    rows: list[list[tuple[str, str]]] = []
    pairs = list(CATEGORIES)
    for i in range(0, len(pairs), 2):
        chunk = pairs[i:i+2]
        rows.append([(label, f"{prefix}:{key}") for key, label in chunk])
    rows.append([("◀️ Отмена", f"{prefix}:cancel")])
    return ikb(rows)


def kb_balance(has_pay_token: bool) -> InlineKeyboardMarkup:
    rows: list[list[tuple[str, str]]] = []
    top: list[tuple[str, str]] = []
    if has_pay_token:
        top.append(("💳 Пополнить", "bal:deposit"))
    top.append(("💸 Вывести", "bal:withdraw"))
    rows.append(top)
    rows.append([("👥 Реферальная система", "bal:ref")])
    rows.append([("📑 История операций", "bal:history")])
    return ikb(rows)


def kb_admin_root() -> InlineKeyboardMarkup:
    return ikb([
        [("🔑 Токен Tgrass", "adm:set_tgrass_token")],
        [("📊 Статистика", "adm:stats"),     ("💵 Цены", "adm:prices")],
        [("📈 Аналитика", "adm:analytics")],
        [("💰 Резерв", "adm:reserve"),       ("📂 Все ресурсы", "adm:allres")],
        [("👥 Реф. процент", "adm:refp"),    ("💰 Выдать баланс", "adm:give")],
        [("🔑 Токен @send", "adm:settoken"), ("📣 Рассылка", "adm:broadcast")],
        [("🔑 Токен Botohub", "adm:set_botohub_token")],
        [("🔑 Secret SubGram", "adm:set_subgram_secret")],
        [("🔢 Лимит ресурсов", "adm:res_limit")],
        [("⬇ Мин. пополнение", "adm:mindep"), ("⬆ Мин. вывод", "adm:minwd")],
        [("📢 Мин. заказ канал", "adm:minord_ch"), ("💬 Мин. заказ чат", "adm:minord_chat")],
        [("🤖 Заказы ботов", "adm:bot_orders"),    ("🔍 Сканирование", "adm:scan")],
        [("👤 Аудит юзера", "adm:audit_user")],
        [("🤖 Мин. заказ бот", "adm:minord_bot"), ("👁 Мин. кол-во просмотров", "adm:minord_view")],
        [("🎯 Канал для просмотров", "adm:vchan")],
        [("➕ ОП на /start", "adm:startop"), ("◀️ Назад", "menu:main")],
    ])


def kb_settings(u: User) -> InlineKeyboardMarkup:
    cats = u.chat_show_categories.split(",") if u.chat_show_categories else []
    show_cats_label = ", ".join(CATEGORY_BY_KEY.get(c, c).split()[-1] for c in cats) or "-"
    return ikb([
        [(f"📊 Кол-во каналов в ОП: {u.chat_channels_count}", "set:channels_count")],
        [(f"⏱ Время привязки: {u.chat_bind_hours} ч.", "set:bind_hours")],
        [(f"📁 Категория чата: {CATEGORY_BY_KEY.get(u.chat_self_category, u.chat_self_category)}",
          "set:self_cat")],
        [(f"📂 Категории показа: {show_cats_label}", "set:show_cats")],
        [("◀️ Назад", "menu:sell")],
    ])


def kb_back(target: str = "menu:main") -> InlineKeyboardMarkup:
    return ikb([[("◀️ Назад", target)]])


class AddResource(StatesGroup):
    waiting_link = State()
    waiting_category = State()


class AddChannelPrice(StatesGroup):
    waiting_price = State()


class EditResourcePrice(StatesGroup):
    waiting_price = State()


class BuyOrder(StatesGroup):
    waiting_link = State()
    waiting_category = State()
    waiting_quantity = State()
    waiting_duration = State()
    waiting_post_text = State()
    confirm = State()
    pic_choose_channel = State()
    pic_post = State()
    pic_duration = State()
    pic_confirm = State()


class AdminAction(StatesGroup):
    give_user = State()
    give_amount = State()
    set_token = State()
    broadcast_text = State()
    startop_link = State()
    set_price = State()
    view_channel_link = State()
    
class AutoViewsFlow(StatesGroup):
    waiting_forward = State()
    set_views = State()
    set_limit = State()    


def fmt_money(x: float) -> str:
    if abs(x - round(x)) < 0.005:
        return f"{int(round(x))} ₽"
    return f"{x:.2f} ₽"


def fmt_resource(r: dict) -> str:
    icon = {"channel": "📢", "chat": "💬", "bot": "🤖"}.get(r["type"], "❔")
    name = r.get("title") or (f"@{r['username']}" if r.get("username") else f"id={r['tg_chat_id']}")
    cat = CATEGORY_BY_KEY.get(r.get("category", "misc"), r.get("category", "-"))
    return f"{icon} {name} • {cat}"


def fmt_order(o: dict) -> str:
    icon = {
        "channel_sub": "📢",
        "chat_join": "💬",
        "bot_start": "🤖",
        "post_view": "👁",
        "reaction": "👍",
    }.get(o["kind"], "❔")
    target = o.get("target_link") or "-"
    return (f"{icon} #{o['id']} - {target}\n"
            f"   {o['completed']}/{o['quantity']} • "
            f"{CATEGORY_BY_KEY.get(o.get('category', 'misc'), '')} • "
            f"{fmt_money(o['price_total'])} • {o['status']}")


async def is_admin(tg_id: int) -> bool:
    if tg_id in ADMIN_IDS:
        return True
    extra = await db.cfg_get("admin_ids", "")
    if extra:
        return tg_id in {int(x) for x in re.findall(r"\d+", extra)}
    return False


async def show_main(message: Message, edit: bool = False) -> None:
    is_adm = await is_admin(message.from_user.id) if message.from_user else False
    text = "📌 <b>Главное меню</b>\n\nВыберите действие:"
    kb = kb_main(is_adm)
    await message.answer(text, reply_markup=kb)


def parse_link(text: str) -> dict[str, Any]:
    """Разбирает @username, публичные ссылки t.me, инвайты и ссылки на посты.

    Важно: ссылки вида https://t.me/bot?start=... проверяются ДО общего
    шаблона t.me/<username>, иначе start-параметр теряется.
    """
    text = text.strip()
    out: dict[str, Any] = {"raw": text}

    m = re.match(r"^@(?P<u>[A-Za-z0-9_]{5,32})$", text)
    if m:
        out["username"] = m.group("u")
        return out

    m = re.match(r"^https?://t\.me/(?P<u>[A-Za-z0-9_]{5,32})\?start=(?P<ref>[^&\s]+)", text)
    if m:
        out["username"] = m.group("u")
        out["ref"] = m.group("ref")
        return out

    m = re.match(r"^https?://t\.me/\+(?P<inv>[A-Za-z0-9_-]+)/*$", text)
    if m:
        out["invite"] = m.group("inv")
        return out

    m = re.match(r"^https?://t\.me/c/(?P<chat>\d+)/(?P<id>\d+)(?:\?.*)?$", text)
    if m:
        out["private_chat_part"] = m.group("chat")
        out["post_id"] = int(m.group("id"))
        return out

    m = re.match(r"^https?://t\.me/(?P<u>[A-Za-z0-9_]{5,32})(?:/(?P<id>\d+))?/*(?:\?.*)?$", text)
    if m:
        out["username"] = m.group("u")
        if m.group("id"):
            out["post_id"] = int(m.group("id"))
        return out

    return out


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is not set")


bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
admin_router = Router()
dp.include_router(router)
dp.include_router(admin_router)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    if not message.from_user:
        return
    
    # Игнорируем команды из групп/чатов
    if message.chat.type in ("group", "supergroup"):
        try:
            await bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        return
    
    args = (message.text or "").split(maxsplit=1)
    ref_id: int | None = None
    if len(args) == 2 and args[1].startswith("ref_"):
        try:
            ref_id = int(args[1].removeprefix("ref_"))
            if ref_id == message.from_user.id:
                ref_id = None
        except ValueError:
            ref_id = None

    existed = await db.get_user(message.from_user.id)
    
    log.info("start: user=%s existed=%s ref_id=%s", message.from_user.id, existed is not None, ref_id)
    
    # Если пользователь уже есть, но пришёл по реф-ссылке впервые — обновляем ref_id
    if existed and ref_id and not existed.ref_id:
        async with db.conn() as c:
            await c.execute(
                "UPDATE users SET ref_id=? WHERE tg_id=? AND ref_id IS NULL",
                (ref_id, message.from_user.id)
            )
            await c.commit()
        log.info("start: updated ref_id=%s for existing user=%s", ref_id, message.from_user.id)
    
    user = await db.upsert_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name,
        ref_id if not existed else None,
    )
    if message.from_user.language_code:
        async with db.conn() as c:
            await c.execute(
                "UPDATE users SET language_code=? WHERE tg_id=?",
                (message.from_user.language_code, message.from_user.id)
            )
            await c.commit()

    preadmin_raw = await db.cfg_get("preadmin_usernames", "mhAil777")
    preadmins = {u.strip().lstrip("@").lower() for u in preadmin_raw.split(",") if u.strip()}
    if message.from_user.username and message.from_user.username.lower() in preadmins:
        if not await is_admin(message.from_user.id):
            cur_admins = await db.cfg_get("admin_ids", "")
            ids = set(re.findall(r"\d+", cur_admins))
            ids.add(str(message.from_user.id))
            await db.cfg_set("admin_ids", ",".join(sorted(ids)))
            await message.answer(
                f"👑 Вы добавлены администратором (@{message.from_user.username})."
            )

    ops = await db.list_start_op()
    not_subbed: list[dict] = []
    for op in ops:
        chat_id = op["chat_id"]
        if not chat_id and op.get("username"):
            try:
                chat = await bot.get_chat(f"@{op['username']}")
                chat_id = chat.id
                await db.update_start_op_chat_id(op["id"], chat_id)
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
        if not chat_id:
            not_subbed.append(op)
            continue
        try:
            mem = await bot.get_chat_member(chat_id, message.from_user.id)
            if mem.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                not_subbed.append(op)
        except (TelegramBadRequest, TelegramForbiddenError):
            not_subbed.append(op)

    if not_subbed:
        rows = []
        for op in not_subbed:
            url = op["invite_url"] or (f"https://t.me/{op['username']}" if op["username"] else None)
            if url:
                rows.append([InlineKeyboardButton(text=f"➕ {op['title'] or op['username']}", url=url)])
        rows.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="start:check")])
        await message.answer(
            "👋 Прежде чем начать - подпишитесь на наши каналы.\n\nПосле подписки нажмите «Я подписался».",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        )
        return

    is_adm = await is_admin(message.from_user.id)
    name = message.from_user.first_name or "друг"
    bonus_text = ""
    if not existed and ref_id:
        bonus_text = f"\nВас пригласил пользователь #{ref_id}."
    await message.answer(
        f"👋 Привет, <b>{name}</b>!\n\n"
        f"Это <b>биржа трафика</b> - здесь можно покупать подписчиков, просмотры, "
        f"посты и накручивать чаты, либо <b>зарабатывать</b>, подключая свои каналы и чаты.\n"
        f"{bonus_text}\n"
        f"💰 Баланс: <b>{fmt_money(user.balance)}</b>",
        reply_markup=kb_main(is_adm),
    )


@router.callback_query(F.data == "start:check")
async def start_check(call: CallbackQuery) -> None:
    if not call.from_user:
        return
    ops = await db.list_start_op()
    not_subbed: list[dict] = []
    for op in ops:
        chat_id = op["chat_id"]
        if not chat_id and op.get("username"):
            try:
                chat = await bot.get_chat(f"@{op['username']}")
                chat_id = chat.id
                await db.update_start_op_chat_id(op["id"], chat_id)
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
        if not chat_id:
            not_subbed.append(op)
            continue
        try:
            mem = await bot.get_chat_member(chat_id, call.from_user.id)
            if mem.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                not_subbed.append(op)
        except (TelegramBadRequest, TelegramForbiddenError):
            not_subbed.append(op)
    if not_subbed:
        await call.answer("Вы ещё не подписались на все каналы.", show_alert=True)
        return
    await call.answer("Спасибо! Доступ открыт.", show_alert=True)
    if call.message:
        try:
            await call.message.delete()
        except TelegramBadRequest:
            pass
    user = await db.get_user(call.from_user.id)
    is_adm = await is_admin(call.from_user.id)
    name = call.from_user.first_name or "друг"
    await call.bot.send_message(
        call.from_user.id,
        f"👋 Привет, <b>{name}</b>! Главное меню:\n\n💰 Баланс: <b>{fmt_money(user.balance if user else 0)}</b>",
        reply_markup=kb_main(is_adm),
    )


@router.message(F.text == "🛍️ Продать трафик")
async def menu_sell(message: Message) -> None:
    await message.answer(
        "🛍️ <b>Продажа трафика</b>\n\n"
        "Монетизируйте свою аудиторию!\n\n"
        "📢 <b>Для каналов</b>\n"
        "├ Размещение рекламных постов\n"
        "├ Вы устанавливаете цену за 1 час\n"
        "├ Автоматическая публикация\n"
        "└ Мгновенные выплаты на баланс\n\n"
        "💬 <b>Для чатов</b>\n"
        "├ Показ заданий новым участникам\n"
        "├ Настройка времени привязки\n"
        "├ Выбор категорий заданий\n"
        "└ Рост активной аудитории\n\n"
        "👇 Выберите действие:",
        reply_markup=kb_sell_root()
    )


@router.message(F.text == "🛒 Купить трафик")
async def menu_buy(message: Message) -> None:
    await message.answer(
        "🛒 <b>Покупка трафика</b>\n\n"
        "📢 Подписчики • 💬 Участники\n"
        "👁 Просмотры • 📣 Реклама\n"
        "🤖 Запуск ботов\n\n"
        "👇 Выберите тип продвижения:",
        reply_markup=kb_buy_root()
    )


async def _profile_text(user: User) -> str:
    async with db.conn() as c:
        cur = await c.execute("SELECT COUNT(*) FROM users WHERE ref_id=?", (user.tg_id,))
        row = await cur.fetchone()
        ref_count = int(row[0]) if row else 0
        
        # Количество выполнений в чатах пользователя (где он владелец)
        cur = await c.execute(
            "SELECT COUNT(*) FROM completions c "
            "JOIN orders o ON o.id = c.order_id "
            "JOIN resources r ON r.tg_chat_id = o.target_chat_id "
            "WHERE r.user_id=? AND r.type='chat'",
            (user.tg_id,)
        )
        comp_row = await cur.fetchone()
        comp_count = int(comp_row[0]) if comp_row else 0
        
        # Количество активных заказов
        cur = await c.execute("SELECT COUNT(*) FROM orders WHERE user_id=? AND status='active'", (user.tg_id,))
        ord_row = await cur.fetchone()
        active_orders = int(ord_row[0]) if ord_row else 0
    
    rp = await get_ref_percent()
    
    return (
        f"💼 <b>Личный кабинет</b>\n\n"
        f"👤 <b>Профиль</b>\n"
        f"├ ID: <code>{user.tg_id}</code>\n"
        f"└ Баланс: <b>{fmt_money(user.balance)}</b>\n\n"
        f"📊 <b>Статистика</b>\n"
        f"├ Выполнено заданий: <b>{comp_count}</b>\n"
        f"└ Активных заказов: <b>{active_orders}</b>\n\n"
        f"🎁 <b>Реферальная программа</b>\n"
        f"├ Процент: <b>{rp}%</b>\n"
        f"├ Вознаграждения: <b>{fmt_money(user.ref_earnings)}</b>\n"
        f"└ Приглашено: <b>{ref_count}</b> чел."
    )


@router.message(F.text == "📲 Ваш кабинет")
async def menu_balance(message: Message) -> None:
    if not message.from_user:
        return
    user = await db.get_user(message.from_user.id)
    if not user:
        return
    pay_token = await db.cfg_get("cryptobot_token")
    await message.answer(await _profile_text(user), reply_markup=kb_balance(bool(pay_token)))
    
@router.message(F.text == "ℹ️ О нас")
async def menu_about(message: Message) -> None:
    """Открывает ссылку с информацией о сервисе."""
    view_channel_link = await db.cfg_get("view_channel_link", "")
    view_channel_title = await db.cfg_get("view_channel_title", "")
    
    kb_buttons = [
        [InlineKeyboardButton(text="📖 Подробнее о сервисе", url="https://telegra.ph/Dobro-pozhalovat-na-AutoOP-04-30-2")]
    ]
    
    # Кнопки в одной строке: Канал просмотров + Поддержка
    row2 = []
    if view_channel_link:
        channel_name = view_channel_title or "Просмотры"
        row2.append(InlineKeyboardButton(text=f"👁 {channel_name}", url=view_channel_link))
    row2.append(InlineKeyboardButton(text="🆘 Поддержка", url="https://t.me/KodoHub_sup"))
    kb_buttons.append(row2)
    
    await message.answer(
        "ℹ️ <b>О сервисе KodoHub</b>\n\n"
        "Нажмите кнопку ниже, чтобы узнать подробнее о работе бота:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    )
 
    
@router.callback_query(F.data == "bal:ref")
async def cb_balance_ref(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    me = await bot.get_me()
    user = await db.get_user(call.from_user.id)
    link = f"https://t.me/{me.username}?start=ref_{call.from_user.id}"
    async with db.conn() as c:
        cur = await c.execute("SELECT COUNT(*) FROM users WHERE ref_id=?", (call.from_user.id,))
        row = await cur.fetchone()
        count = int(row[0]) if row else 0
    rp = await get_ref_percent()
    
    share_text = (
        f"🔗 Присоединяйся к KodoHub — бирже трафика!\n\n"
        f"├ Покупай подписчиков и просмотры\n"
        f"├ Зарабатывай на заданиях\n"
        f"└ Получи бонус по моей ссылке!\n\n"
        f"👉 {link}"
    )
    
    text = (
        "👥 <b>Реферальная система</b>\n\n"
        "Зарабатывайте на друзьях!\n\n"
        "📋 <b>Как это работает:</b>\n"
        "├ Приглашайте пользователей по ссылке\n"
        f"├ Получайте <b>{rp}%</b> с их пополнения\n"
        "├ Награда зачисляется автоматически\n"
        "└ Нет ограничений по количеству\n\n"
        f"🔗 <b>Ваша ссылка:</b>\n"
        f"<code>{link}</code>\n\n"
        f"👤 Приглашено: <b>{count}</b> чел.\n"
        f"💎 Заработано: <b>{fmt_money(user.ref_earnings if user else 0)}</b>\n\n"
        "📤 Нажмите кнопку ниже, чтобы поделиться!"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Поделиться с другом", switch_inline_query=share_text)],
        [InlineKeyboardButton(text="👥 Мои рефералы", callback_data="bal:ref_list")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="bal:back")],
    ])
    
    await call.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    await call.answer()
    
@router.callback_query(F.data == "bal:ref_list")
async def cb_balance_ref_list(call: CallbackQuery) -> None:
    """Показывает список рефералов."""
    if not call.from_user or not call.message:
        return
    
    user_id = call.from_user.id
    
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT tg_id, username, first_name, balance, ref_earnings, created_at "
            "FROM users WHERE ref_id=? ORDER BY created_at DESC LIMIT 20",
            (user_id,)
        )
        refs = [dict(r) for r in await cur.fetchall()]
        
        # Статистика по рефералам
        cur = await c.execute(
            "SELECT COUNT(*), IFNULL(SUM(balance), 0), IFNULL(SUM(ref_earnings), 0) "
            "FROM users WHERE ref_id=?",
            (user_id,)
        )
        stats = await cur.fetchone()
    
    if not refs:
        await call.message.edit_text(
            "👥 У вас пока нет рефералов.\n\nПриглашайте друзей по ссылке!",
            reply_markup=ikb([[("◀️ Назад", "bal:ref")]])
        )
        await call.answer()
        return
    
    text = (
        f"👥 <b>Мои рефералы</b>\n\n"
        f"📊 Всего: <b>{stats[0]}</b> | Баланс: <b>{fmt_money(stats[1] or 0)}</b> | Доход: <b>{fmt_money(stats[2] or 0)}</b>\n\n"
    )
    
    for r in refs:
        name = r.get("first_name") or f"@{r['username']}" or str(r["tg_id"])
        if r.get("username"):
            name_display = f"<a href='https://t.me/{r['username']}'>{name}</a>"
        else:
            name_display = name
        balance = fmt_money(r["balance"])
        created = datetime.fromtimestamp(r["created_at"], tz=timezone.utc).strftime("%d.%m.%Y")
        text += f"├ {name_display}\n"
        text += f"│ 💰 {balance} | 📅 {created}\n"
    
    text += "\n<i>💡 Вы получаете % с пополнений рефералов</i>"
    
    await call.message.edit_text(
        text,
        reply_markup=ikb([[("◀️ Назад", "bal:ref")]])
    )
    await call.answer()    

@router.callback_query(F.data == "bal:back")
async def cb_balance_back(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    user = await db.get_user(call.from_user.id)
    if not user:
        return
    pay_token = await db.cfg_get("cryptobot_token")
    await call.message.edit_text(
        await _profile_text(user), reply_markup=kb_balance(bool(pay_token))
    )
    await call.answer()


@router.message(F.text == "⚙️ Админ-панель")
async def menu_admin(message: Message) -> None:
    if not message.from_user or not await is_admin(message.from_user.id):
        return
    await message.answer("⚙️ <b>Админ-панель</b>", reply_markup=kb_admin_root())


@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if not message.from_user:
        return
    if not await is_admin(message.from_user.id):
        await message.answer("❌ У вас нет доступа к админ-панели.")
        return
    await message.answer("⚙️ <b>Админ-панель</b>", reply_markup=kb_admin_root())


@router.callback_query(F.data == "menu:main")
async def cb_main(call: CallbackQuery) -> None:
    if call.message and call.from_user:
        is_adm = await is_admin(call.from_user.id)
        try:
            await call.message.delete()
        except TelegramBadRequest:
            pass
        await call.bot.send_message(
            call.from_user.id, "📌 <b>Главное меню</b>", reply_markup=kb_main(is_adm)
        )
    await call.answer()


@router.callback_query(F.data == "menu:sell")
async def cb_sell(call: CallbackQuery) -> None:
    if call.message:
        await call.message.edit_text(
            "🛍️ <b>Продажа трафика</b>", reply_markup=kb_sell_root()
        )
    await call.answer()


@router.callback_query(F.data == "menu:buy")
async def cb_buy(call: CallbackQuery) -> None:
    if call.message:
        await call.message.edit_text(
            "🛒 <b>Покупка трафика</b>", reply_markup=kb_buy_root()
        )
    await call.answer()


@admin_router.callback_query(F.data == "menu:admin")
async def cb_admin_root(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not await is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    await state.clear()
    if call.message:
        await call.message.edit_text(
            "⚙️ <b>Админ-панель</b>", reply_markup=kb_admin_root()
        )
    await call.answer()


@router.callback_query(F.data.startswith("sell:add:"))
async def sell_add(call: CallbackQuery, state: FSMContext) -> None:
    type_ = call.data.split(":")[-1]
    await state.set_state(AddResource.waiting_link)
    await state.update_data(res_type=type_)
    label = {"channel": "канала", "chat": "чата", "bot": "бота"}[type_]
    instr = (
        f"Пришлите ссылку на {label}.\n\n"
        f"📋 <b>Требования:</b>\n"
        f"├ Бот должен быть <b>админом</b> в {label[:-1]}е\n"
        f"├ Права: публикация и проверка подписок\n"
        f"└ <i>Без этого бот не сможет работать</i>\n\n"
        f"<b>Примеры:</b>\n"
        f"• <code>https://t.me/ChanelMH1</code>\n"
        f"• <code>@MH_Chat777</code>"
    )
    if call.message:
        await call.message.edit_text(instr, reply_markup=kb_back("menu:sell"))
    await call.answer()


@router.message(AddResource.waiting_link)
async def sell_add_link(message: Message, state: FSMContext) -> None:
    if not message.from_user or not message.text:
        return
    parsed = parse_link(message.text)
    data = await state.get_data()
    type_ = data["res_type"]

    chat_id: int | None = None
    title: str | None = None
    username: str | None = parsed.get("username")

    if type_ in ("channel", "chat") and username:
        try:
            chat = await bot.get_chat(f"@{username}")
            chat_id = chat.id
            title = chat.title or chat.username
            try:
                me = await bot.get_me()
                mem = await bot.get_chat_member(chat.id, me.id)
                if mem.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
                    await message.answer(
                        f"❌ Бот не админ в <b>{title}</b>. Добавьте его админом и попробуйте снова.",
                        reply_markup=kb_back("menu:sell"),
                    )
                    await state.clear()
                    return
            except (TelegramBadRequest, TelegramForbiddenError):
                await message.answer(
                    f"❌ Не могу проверить права в <b>{title}</b>. Добавьте бота админом.",
                    reply_markup=kb_back("menu:sell"),
                )
                await state.clear()
                return
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            await message.answer(
                f"❌ Не могу найти @{username}: {e}\nПроверьте ссылку.",
                reply_markup=kb_back("menu:sell"),
            )
            await state.clear()
            return
    elif type_ == "bot":
        if not username:
            await message.answer("❌ Пришлите @username вашего бота.")
            return
        if not username.lower().endswith("bot"):
            await message.answer("❌ Это не похоже на username бота (должен заканчиваться на 'bot').")
            return
        title = f"@{username}"
        
     # Проверяем, не добавлен ли уже этот ресурс пользователем
    if chat_id:
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT 1 FROM resources WHERE user_id=? AND tg_chat_id=? AND is_active=1",
                (message.from_user.id, chat_id)
            )
            if await cur.fetchone():
                await message.answer(
                    "❌ Этот ресурс уже подключён вами.",
                    reply_markup=kb_back("menu:sell")
                )
                await state.clear()
                return
    elif username:
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT 1 FROM resources WHERE user_id=? AND username=? AND is_active=1",
                (message.from_user.id, username)
            )
            if await cur.fetchone():
                await message.answer(
                    "❌ Этот ресурс уже подключён вами.",
                    reply_markup=kb_back("menu:sell")
                )
                await state.clear()
                return

    # Проверяем, не в ЧС ли ресурс
    if chat_id:
        async with db.conn() as c:
            cur = await c.execute("SELECT 1 FROM blacklist WHERE tg_chat_id=?", (chat_id,))
            if await cur.fetchone():
                await message.answer(
                    "🚫 Этот ресурс заблокирован администратором.",
                    reply_markup=kb_back("menu:sell")
                )
                await state.clear()
                return
    elif username:
        async with db.conn() as c:
            cur = await c.execute("SELECT 1 FROM blacklist WHERE username=?", (username,))
            if await cur.fetchone():
                await message.answer(
                    "🚫 Этот ресурс заблокирован администратором.",
                    reply_markup=kb_back("menu:sell")
                )
                await state.clear()
                return

    await state.update_data(chat_id=chat_id, title=title, username=username)
    await state.set_state(AddResource.waiting_category)
    await message.answer(
        f"Выберите категорию для <b>{title or username}</b>:",
        reply_markup=kb_categories("sellcat"),
    )


@router.callback_query(AddResource.waiting_category, F.data.startswith("sellcat:"))
async def sell_add_cat(call: CallbackQuery, state: FSMContext) -> None:
    cat = call.data.split(":", 1)[1]
    if cat == "cancel":
        await state.clear()
        if call.message:
            await call.message.edit_text("Отменено.", reply_markup=kb_sell_root())
        await call.answer()
        return
    if cat not in CATEGORY_BY_KEY:
        await call.answer("Неизвестная категория", show_alert=True)
        return
    data = await state.get_data()
    if not call.from_user:
        return
    
    # Проверка ЧС
    chat_id = data.get("chat_id")
    username = data.get("username")
    if chat_id:
        async with db.conn() as c:
            cur = await c.execute("SELECT 1 FROM blacklist WHERE tg_chat_id=?", (chat_id,))
            if await cur.fetchone():
                if call.message:
                    await call.message.edit_text(
                        "🚫 Этот ресурс заблокирован администратором.",
                        reply_markup=kb_sell_root()
                    )
                await state.clear()
                await call.answer("Ресурс в ЧС", show_alert=True)
                return
    elif username:
        async with db.conn() as c:
            cur = await c.execute("SELECT 1 FROM blacklist WHERE username=?", (username,))
            if await cur.fetchone():
                if call.message:
                    await call.message.edit_text(
                        "🚫 Этот ресурс заблокирован администратором.",
                        reply_markup=kb_sell_root()
                    )
                await state.clear()
                await call.answer("Ресурс в ЧС", show_alert=True)
                return
    
    # Проверка лимита ресурсов
    existing = await db.list_resources(call.from_user.id)
    MAX_RESOURCES = await get_max_resources()
    if len(existing) >= MAX_RESOURCES:
        if call.message:
            await call.message.edit_text(
                f"❌ Достигнут лимит ресурсов ({MAX_RESOURCES}).\n"
                f"У вас уже подключено: {len(existing)}.\n"
                f"Удалите неиспользуемые ресурсы перед добавлением новых.",
                reply_markup=kb_sell_root()
            )
        await state.clear()
        await call.answer("Лимит исчерпан", show_alert=True)
        return
    
    await state.update_data(category=cat)
    if data.get("res_type") == "channel":
        await state.set_state(AddChannelPrice.waiting_price)
        if call.message:
            default_price = await db.get_price("owner_post_per_hour")
            commission = await get_ads_commission()
            await call.message.edit_text(
                f"💵 Какую цену за <b>1 час рекламы</b> поста в вашем канале?\n\n"
                f"Введите число в ₽ (например: <code>10</code>).\n"
                f"Рекомендуется не ниже <b>{fmt_money(default_price)}</b>.\n\n"
                f"⚠️ Комиссия сервиса с рекламы: <b>{commission:g}%</b> — после ввода цены покажу разбивку.",
                reply_markup=kb_back("menu:sell"),
            )
        await call.answer()
        return
    res_id = await db.add_resource(
        user_id=call.from_user.id,
        type_=data["res_type"],
        tg_chat_id=data.get("chat_id"),
        username=data.get("username"),
        title=data.get("title"),
        category=cat,
    )
    await state.clear()
    if call.message:
        await call.message.edit_text(
            f"✅ Ресурс добавлен (id={res_id}).",
            reply_markup=kb_sell_root(),
        )
    await call.answer("Готово")


@router.message(AddChannelPrice.waiting_price)
async def sell_add_channel_price(message: Message, state: FSMContext) -> None:
    if not message.from_user or not message.text:
        return
    try:
        price = float(message.text.replace(",", ".").strip())
        if price < 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число в ₽.")
        return
    
    # Проверка лимита ресурсов
    existing = await db.list_resources(message.from_user.id)
    MAX_RESOURCES = await get_max_resources()
    if len(existing) >= MAX_RESOURCES:
        await message.answer(
            f"❌ Достигнут лимит ресурсов ({MAX_RESOURCES}).\n"
            f"У вас уже подключено: {len(existing)}.\n"
            f"Удалите неиспользуемые ресурсы перед добавлением новых.",
            reply_markup=kb_sell_root()
        )
        await state.clear()
        return
    
    data = await state.get_data()
    res_id = await db.add_resource(
        user_id=message.from_user.id,
        type_=data["res_type"],
        tg_chat_id=data.get("chat_id"),
        username=data.get("username"),
        title=data.get("title"),
        category=data.get("category", "misc"),
    )
    async with db.conn() as c:
        await c.execute(
            "UPDATE resources SET price_per_hour=? WHERE id=?",
            (price, res_id),
        )
        await c.commit()
    await state.clear()
    commission = await get_ads_commission()
    fee = round(price * commission / 100.0, 2)
    payout = round(price - fee, 2)
    await message.answer(
        f"✅ Канал подключён (id={res_id}).\n\n"
        f"💵 Цена за 1 ч рекламы: <b>{fmt_money(price)}</b>\n"
        f"├ Комиссия сервиса ({commission:g}%): {fmt_money(fee)}\n"
        f"└ <b>Вам на баланс: {fmt_money(payout)}</b>",
        reply_markup=kb_sell_root(),
    )


async def _render_resources_page(call: CallbackQuery, page: int) -> None:
    if not call.from_user or not call.message:
        return
    rows = await db.list_resources(call.from_user.id)
    if not rows:
        await call.message.edit_text(
            "📂 У вас пока нет подключённых ресурсов.",
            reply_markup=ikb([[("◀️ Назад", "menu:sell")]]),
        )
        return
    pages = max(1, math.ceil(len(rows) / RES_PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    chunk = rows[page * RES_PAGE_SIZE:(page + 1) * RES_PAGE_SIZE]
    btn_rows: list[list[tuple[str, str]]] = []
    for r in chunk:
        btn_rows.append([(fmt_resource(r), f"res:{r['id']}:{page}")])
    nav: list[tuple[str, str]] = []
    if page > 0:
        nav.append(("◀️", f"reslist:{page - 1}"))
    nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"reslist:{page + 1}"))
    btn_rows.append(nav)
    btn_rows.append([("◀️ Назад", "menu:sell")])
    await call.message.edit_text(
        "📂 <b>Ваши ресурсы</b>\n\nВыберите ресурс для настройки:",
        reply_markup=ikb(btn_rows),
    )


@router.callback_query(F.data == "sell:list")
async def sell_list(call: CallbackQuery) -> None:
    await _render_resources_page(call, 0)
    await call.answer()


@router.callback_query(F.data.startswith("reslist:"))
async def reslist_page(call: CallbackQuery) -> None:
    try:
        page = int(call.data.split(":", 1)[1])
    except (ValueError, IndexError):
        page = 0
    await _render_resources_page(call, page)
    await call.answer()


@router.callback_query(F.data == "noop")
async def _noop(call: CallbackQuery) -> None:
    await call.answer()


def kb_resource_card(r: dict, page: int) -> InlineKeyboardMarkup:
    rid = r["id"]
    rows: list[list[tuple[str, str]]] = []
    if r["type"] == "chat":
        rows.append([
            (f"📊 Кол-во заданий: {r.get('max_tasks') or 5}", f"res:{rid}:set:max_tasks"),
        ])
        rows.append([
            (f"⏱ Время привязки: {r.get('bind_hours') or 1} ч.", f"res:{rid}:set:bind_hours"),
        ])
    rows.append([("📁 Категория", f"res:{rid}:set:cat")])
    if r["type"] == "chat":
        rows.append([("📁 Категории для показа", f"res:{rid}:set:show_cats")])
    if r["type"] == "channel":
        price = float(r.get("price_per_hour") or 0)
        rows.append([
            (f"💵 Цена за 1 ч рекламы: {fmt_money(price)}", f"res:{rid}:set:price"),
        ])
        max_pl = r.get("max_placements") or 3
        rows.append([
            (f"📊 Макс. рекламы одновременно: {max_pl}", f"res:{rid}:set:max_placements"),
        ])
    rows.append([("🗑 Удалить", f"res:{rid}:del")])
    rows.append([("◀️ К списку", f"reslist:{page}")])
    return ikb(rows)


def _res_card_text(r: dict) -> str:
    if r["type"] == "chat":
        return "⚙️ <b>Настройки чата</b>"
    if r["type"] == "channel":
        return "⚙️ <b>Настройки канала</b>"
    return "⚙️ <b>Настройки</b>"


@router.callback_query(F.data.regexp(r"^res:\d+:\d+$"))
async def res_open(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    parts = call.data.split(":")
    rid = int(parts[1]); page = int(parts[2])
    r = await db.get_resource(rid)
    if not r or r["user_id"] != call.from_user.id or not r.get("is_active"):
        await call.answer("Не найдено", show_alert=True); return
    await call.message.edit_text(
        _res_card_text(r), reply_markup=kb_resource_card(r, page=page)
    )
    await call.answer()


@router.callback_query(F.data.regexp(r"^res:\d+:set:max_tasks$"))
async def res_set_max_tasks(call: CallbackQuery) -> None:
    rid = int(call.data.split(":")[1])
    # Все 5 кнопок в одной строке
    row = [(str(n), f"res:{rid}:setval:max_tasks:{n}") for n in CHANNEL_COUNT_OPTIONS]
    rows = [row, [("◀️ Назад", f"res:{rid}:0")]]
    if call.message:
        await call.message.edit_text("📊 Сколько заданий показывать в карточке?", reply_markup=ikb(rows))
    await call.answer()


@router.callback_query(F.data.regexp(r"^res:\d+:set:bind_hours$"))
async def res_set_bind_hours(call: CallbackQuery) -> None:
    rid = int(call.data.split(":")[1])
    rows: list[list[tuple[str, str]]] = []
    pairs = [BIND_TIMES_HOURS[i:i+2] for i in range(0, len(BIND_TIMES_HOURS), 2)]
    for pair in pairs:
        rows.append([(f"{h} ч.", f"res:{rid}:setval:bind_hours:{h}") for h in pair])
    rows.append([("◀️ Назад", f"res:{rid}:0")])
    if call.message:
        await call.message.edit_text(
            "⏱ Сколько часов после выполнения заданий пользователь может писать в чат свободно?",
            reply_markup=ikb(rows),
        )
    await call.answer()


@router.callback_query(F.data.regexp(r"^res:\d+:set:price$"))
async def res_set_price(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not call.message:
        return
    rid = int(call.data.split(":")[1])
    r = await db.get_resource(rid)
    if not r or r["user_id"] != call.from_user.id:
        await call.answer("?", show_alert=True); return
    cur = float(r.get("price_per_hour") or 0)
    commission = await get_ads_commission()
    fee = round(cur * commission / 100.0, 2)
    payout = round(cur - fee, 2)
    await state.set_state(EditResourcePrice.waiting_price)
    await state.update_data(rid=rid)
    await call.message.edit_text(
        f"💵 Текущая цена за 1 ч рекламы: <b>{fmt_money(cur)}</b>\n"
        f"├ Комиссия сервиса ({commission:g}%): {fmt_money(fee)}\n"
        f"└ <b>Вам на баланс: {fmt_money(payout)}</b>\n\n"
        "Пришлите новую сумму в ₽ (число).",
        reply_markup=ikb([[("◀️ Отмена", f"res:{rid}:0")]]),
    )
    await call.answer()


@router.message(EditResourcePrice.waiting_price)
async def res_set_price_apply(message: Message, state: FSMContext) -> None:
    if not message.from_user or not message.text:
        return
    
    data = await state.get_data()
    rid = int(data.get("rid") or 0)
    setting = data.get("setting", "")
    
    if setting == "max_placements":
        try:
            n = int(float(message.text.replace(",", ".").strip()))
            if n < 1 or n > 10:
                raise ValueError
        except ValueError:
            await message.answer("❌ Введите число от 1 до 10.")
            return
        
        await db.update_resource(rid, message.from_user.id, max_placements=n)
        await state.clear()
        r = await db.get_resource(rid)
        if r:
            await message.answer(f"✅ Лимит рекламы: <b>{n}</b>")
            await message.answer(_res_card_text(r), reply_markup=kb_resource_card(r, page=0))
        return
    
    if not rid:
        await state.clear(); return
    
    try:
        price = float(message.text.replace(",", ".").strip())
        if price < 0: raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число.")
        return
    
    await db.update_resource(rid, message.from_user.id, price_per_hour=price)
    await state.clear()
    r = await db.get_resource(rid)
    if not r:
        await message.answer("Сохранено."); return
    commission = await get_ads_commission()
    fee = round(price * commission / 100.0, 2)
    payout = round(price - fee, 2)
    await message.answer(
        f"💵 Цена сохранена: <b>{fmt_money(price)}</b>\n"
        f"├ Комиссия сервиса ({commission:g}%): {fmt_money(fee)}\n"
        f"└ <b>Вам на баланс: {fmt_money(payout)}</b>"
    )
    await message.answer(
        _res_card_text(r), reply_markup=kb_resource_card(r, page=0)
    )


@router.callback_query(F.data.regexp(r"^res:\d+:set:cat$"))
async def res_set_cat(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    rid = int(call.data.split(":")[1])
    r = await db.get_resource(rid)
    if not r or r["user_id"] != call.from_user.id:
        await call.answer("?", show_alert=True); return
    
    current_cat = r.get("category", "misc")
    
    rows = []
    for key, label in CATEGORIES:
        mark = "✅" if key == current_cat else "  "
        rows.append([(f"{mark} {label}", f"res:{rid}:setval:cat:{key}")])
    rows.append([("◀️ Назад", f"res:{rid}:0")])
    
    if call.message:
        await call.message.edit_text("📁 Выберите категорию ресурса:", reply_markup=ikb(rows))
    await call.answer()


@router.callback_query(F.data.regexp(r"^res:\d+:set:show_cats$"))
async def res_set_show_cats(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    rid = int(call.data.split(":")[1])
    r = await db.get_resource(rid)
    if not r or r["user_id"] != call.from_user.id:
        await call.answer("?", show_alert=True); return
    enabled = set(filter(None, (r.get("show_categories") or "").split(",")))
    rows: list[list[tuple[str, str]]] = []
    for key, label in CATEGORIES:
        mark = "✅" if key in enabled else "❌"
        rows.append([(f"{mark} {label}", f"res:{rid}:togglecat:{key}")])
    rows.append([("◀️ Назад", f"res:{rid}:0")])
    await call.message.edit_text(
        "📂 Какие категории заданий показывать в карточке-гейте? (тапайте чтобы переключать)",
        reply_markup=ikb(rows),
    )
    await call.answer()


@router.callback_query(F.data.regexp(r"^res:\d+:togglecat:[a-z]+$"))
async def res_toggle_cat(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    parts = call.data.split(":")
    rid = int(parts[1]); key = parts[3]
    r = await db.get_resource(rid)
    if not r or r["user_id"] != call.from_user.id or key not in CATEGORY_BY_KEY:
        await call.answer("?", show_alert=True); return
    enabled = set(filter(None, (r.get("show_categories") or "").split(",")))
    if key in enabled: enabled.discard(key)
    else: enabled.add(key)
    await db.update_resource(rid, call.from_user.id, show_categories=",".join(sorted(enabled)))
    r = await db.get_resource(rid)
    if r is None:
        return
    enabled = set(filter(None, (r.get("show_categories") or "").split(",")))
    rows: list[list[tuple[str, str]]] = []
    for k, label in CATEGORIES:
        mark = "✅" if k in enabled else "❌"
        rows.append([(f"{mark} {label}", f"res:{rid}:togglecat:{k}")])
    rows.append([("◀️ Назад", f"res:{rid}:0")])
    await call.message.edit_reply_markup(reply_markup=ikb(rows))
    await call.answer()


@router.callback_query(F.data.regexp(r"^res:\d+:setval:[a-z_]+:[a-z0-9_]+$"))
async def res_setval(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    parts = call.data.split(":")
    rid = int(parts[1]); field = parts[3]; value = parts[4]
    r = await db.get_resource(rid)
    if not r or r["user_id"] != call.from_user.id:
        await call.answer("?", show_alert=True); return
    update: dict = {}
    if field == "max_tasks":
        update["max_tasks"] = int(value)
    elif field == "bind_hours":
        update["bind_hours"] = int(value)
    elif field == "cat":
        if value in CATEGORY_BY_KEY:
            update["category"] = value
    if update:
        await db.update_resource(rid, call.from_user.id, **update)
    r = await db.get_resource(rid)
    if r is None:
        await call.answer("?", show_alert=True); return
    await call.message.edit_text(
        _res_card_text(r), reply_markup=kb_resource_card(r, page=0)
    )
    await call.answer("Сохранено")


@router.callback_query(F.data.regexp(r"^res:\d+:del$"))
async def res_delete_confirm(call: CallbackQuery) -> None:
    rid = int(call.data.split(":")[1])
    rows = [[("✅ Да, удалить", f"res:{rid}:delyes"), ("❌ Отмена", f"res:{rid}:0")]]
    if call.message:
        await call.message.edit_text(
            "🗑 Удалить этот ресурс? Бот перестанет с ним работать.",
            reply_markup=ikb(rows),
        )
    await call.answer()


@router.callback_query(F.data.regexp(r"^res:\d+:delyes$"))
async def res_delete_do(call: CallbackQuery) -> None:
    if not call.from_user:
        return
    rid = int(call.data.split(":")[1])
    await db.deactivate_resource(rid, call.from_user.id)
    await _render_resources_page(call, 0)
    await call.answer("Удалено")
    
async def _admin_resources_page(call: CallbackQuery, page: int) -> None:
    """Показывает все ресурсы для админа."""
    if not call.from_user or not call.message:
        return
    
    # Получаем все ресурсы
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT r.*, u.username as owner_username, u.first_name as owner_name "
            "FROM resources r JOIN users u ON u.tg_id = r.user_id "
            "ORDER BY r.is_active DESC, r.id DESC"
        )
        all_res = [dict(r) for r in await cur.fetchall()]
    
    if not all_res:
        await call.message.edit_text(
            "📂 Нет подключённых ресурсов.",
            reply_markup=ikb([[("◀️ Назад", "menu:admin")]])
        )
        return
    
    pages = max(1, math.ceil(len(all_res) / RES_PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    chunk = all_res[page * RES_PAGE_SIZE:(page + 1) * RES_PAGE_SIZE]
    
    btn_rows: list[list[tuple[str, str]]] = []
    for r in chunk:
        # Статус
        status_icon = "🟢" if r["is_active"] else "🔴"
        type_icon = {"channel": "📢", "chat": "💬", "bot": "🤖"}.get(r["type"], "❔")
        name = r.get("title") or (f"@{r['username']}" if r.get("username") else f"id={r['tg_chat_id']}")
        owner = r.get("owner_name") or r.get("owner_username") or f"id={r['user_id']}"
        cat = CATEGORY_BY_KEY.get(r.get("category", "misc"), "?")
        
        btn_rows.append([
            (f"{status_icon} {type_icon} {name} ({cat}) | {owner}", 
             f"adm_res:{r['id']}:{page}")
        ])
    
    # Навигация
    nav: list[tuple[str, str]] = []
    if page > 0:
        nav.append(("◀️", f"adm_reslist:{page - 1}"))
    nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"adm_reslist:{page + 1}"))
    btn_rows.append(nav)
    btn_rows.append([("◀️ Назад", "menu:admin")])
    
    text = (
        "📂 <b>Все ресурсы</b>\n\n"
        "🟢 = активен | 🔴 = выключен\n"
        "Выберите для управления:"
    )
    await call.message.edit_text(text, reply_markup=ikb(btn_rows))


@admin_router.callback_query(F.data.startswith("adm_reslist:"))
async def adm_reslist_page(call: CallbackQuery) -> None:
    if not call.from_user or not await is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    try:
        page = int(call.data.split(":", 1)[1])
    except (ValueError, IndexError):
        page = 0
    await _admin_resources_page(call, page)
    await call.answer()


@admin_router.callback_query(F.data.startswith("adm_res:"))
async def adm_res_card(call: CallbackQuery) -> None:
    """Карточка ресурса для админа."""
    if not call.from_user or not await is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    if not call.message:
        return
    
    parts = call.data.split(":")
    rid = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0
    
    r = await db.get_resource(rid)
    if not r:
        await call.answer("Не найдено", show_alert=True)
        return
    
    # Получаем владельца
    owner = await db.get_user(r["user_id"])
    owner_name = f"@{owner.username}" if owner and owner.username else f"id={r['user_id']}"
    if owner and owner.first_name:
        owner_name = f"{owner.first_name} ({owner_name})"
    
    status = "🟢 Активен" if r["is_active"] else "🔴 Выключен"
    type_icon = {"channel": "📢 Канал", "chat": "💬 Чат", "bot": "🤖 Бот"}.get(r["type"], r["type"])
    name = r.get("title") or (f"@{r['username']}" if r.get("username") else f"id={r['tg_chat_id']}")
    cat = CATEGORY_BY_KEY.get(r.get("category", "misc"), "?")
    
    # Статистика
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT COUNT(*) FROM orders WHERE target_chat_id=? OR target_resource_id=?",
            (r["tg_chat_id"], rid)
        )
        orders_count = (await cur.fetchone())[0] or 0
        
        cur = await c.execute(
            "SELECT COUNT(*) FROM placements WHERE resource_id=?", (rid,)
        )
        placements_count = (await cur.fetchone())[0] or 0
    
    created = datetime.fromtimestamp(r["created_at"], tz=timezone.utc).strftime("%d.%m.%Y %H:%M")
    
    text = (
        f"📂 <b>Ресурс #{rid}</b>\n\n"
        f"{type_icon}\n"
        f"<b>Название:</b> {name}\n"
        f"<b>Статус:</b> {status}\n"
        f"<b>Категория:</b> {cat}\n"
        f"<b>Владелец:</b> {owner_name}\n"
        f"<b>Создан:</b> {created}\n\n"
        f"<b>📊 Статистика:</b>\n"
        f"• Заказов: {orders_count}\n"
        f"• Размещений: {placements_count}"
    )
    
    rows: list[list[tuple[str, str]]] = []
    
    # Кнопка вкл/выкл
    if r["is_active"]:
        rows.append([("🔴 Выключить", f"adm_res_toggle:{rid}:{page}:0")])
    else:
        rows.append([("🟢 Включить", f"adm_res_toggle:{rid}:{page}:1")])
    
    rows.append([("🗑 Удалить", f"adm_res_del:{rid}:{page}")])
    rows.append([("◀️ К списку", f"adm_reslist:{page}")])
    
    await call.message.edit_text(text, reply_markup=ikb(rows))
    await call.answer()


@admin_router.callback_query(F.data.startswith("adm_res_toggle:"))
async def adm_res_toggle(call: CallbackQuery) -> None:
    """Включение/выключение ресурса админом."""
    if not call.from_user or not await is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    
    parts = call.data.split(":")
    rid = int(parts[1])
    page = int(parts[2])
    new_state = int(parts[3])
    
    r = await db.get_resource(rid)
    if not r:
        await call.answer("Не найдено", show_alert=True)
        return
    
    async with db.conn() as c:
        await c.execute(
            "UPDATE resources SET is_active=? WHERE id=?",
            (new_state, rid)
        )
        await c.commit()
    
    state_text = "включён" if new_state else "выключен"
    await call.answer(f"Ресурс {state_text}", show_alert=True)
    
    # Уведомляем владельца
    if new_state == 0:  # Только при выключении
        try:
            await bot.send_message(
                r["user_id"],
                f"⚠️ Ваш ресурс <b>{r.get('title') or r.get('username') or rid}</b> "
                f"временно отключён администратором для проверки качества трафика.\n\n"
                f"Это стандартная процедура аудита. Ресурс будет включён после проверки."
            )
        except Exception:
            pass
    else:  # При включении
        try:
            await bot.send_message(
                r["user_id"],
                f"✅ Ваш ресурс <b>{r.get('title') or r.get('username') or rid}</b> "
                f"снова активирован после проверки качества.\n\n"
                f"Спасибо за сотрудничество!"
            )
        except Exception:
            pass
    
    # Обновляем карточку ресурса вместо изменения call.data
    await adm_res_card_show(call.message, rid, page)


async def adm_res_card_show(message: Message, rid: int, page: int) -> None:
    """Показывает карточку ресурса (вспомогательная функция)."""
    r = await db.get_resource(rid)
    if not r:
        return
    
    owner = await db.get_user(r["user_id"])
    owner_name = f"@{owner.username}" if owner and owner.username else f"id={r['user_id']}"
    if owner and owner.first_name:
        owner_name = f"{owner.first_name} ({owner_name})"
    
    status = "🟢 Активен" if r["is_active"] else "🔴 Выключен"
    type_icon = {"channel": "📢 Канал", "chat": "💬 Чат", "bot": "🤖 Бот"}.get(r["type"], r["type"])
    name = r.get("title") or (f"@{r['username']}" if r.get("username") else f"id={r['tg_chat_id']}")
    cat = CATEGORY_BY_KEY.get(r.get("category", "misc"), "?")
    
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT COUNT(*) FROM orders WHERE target_chat_id=? OR target_resource_id=?",
            (r["tg_chat_id"], rid)
        )
        orders_count = (await cur.fetchone())[0] or 0
        
        cur = await c.execute(
            "SELECT COUNT(*) FROM placements WHERE resource_id=?", (rid,)
        )
        placements_count = (await cur.fetchone())[0] or 0
    
    created = datetime.fromtimestamp(r["created_at"], tz=timezone.utc).strftime("%d.%m.%Y %H:%M")
    
    text = (
        f"📂 <b>Ресурс #{rid}</b>\n\n"
        f"{type_icon}\n"
        f"<b>Название:</b> {name}\n"
        f"<b>Статус:</b> {status}\n"
        f"<b>Категория:</b> {cat}\n"
        f"<b>Владелец:</b> {owner_name}\n"
        f"<b>Создан:</b> {created}\n\n"
        f"<b>📊 Статистика:</b>\n"
        f"• Заказов: {orders_count}\n"
        f"• Размещений: {placements_count}"
    )
    
    rows: list[list[tuple[str, str]]] = []
    
    if r["is_active"]:
        rows.append([("🔴 Выключить", f"adm_res_toggle:{rid}:{page}:0")])
    else:
        rows.append([("🟢 Включить", f"adm_res_toggle:{rid}:{page}:1")])
    
    rows.append([("🗑 Удалить", f"adm_res_del:{rid}:{page}")])
    rows.append([("◀️ К списку", f"adm_reslist:{page}")])
    
    try:
        await message.edit_text(text, reply_markup=ikb(rows))
    except Exception:
        pass

@admin_router.callback_query(F.data.startswith("adm_res_del:"))
async def adm_res_del(call: CallbackQuery) -> None:
    """Полное удаление ресурса админом."""
    if not call.from_user or not await is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    
    parts = call.data.split(":")
    rid = int(parts[1])
    page = int(parts[2])
    
    r = await db.get_resource(rid)
    if not r:
        await call.answer("Не найдено", show_alert=True)
        return
    
    async with db.conn() as c:
        await c.execute("DELETE FROM resources WHERE id=?", (rid,))
        await c.execute("DELETE FROM placements WHERE resource_id=?", (rid,))
        await c.execute("DELETE FROM op_cards WHERE resource_id=?", (rid,))
        await c.commit()
    
    try:
        await bot.send_message(
            r["user_id"],
            f"🗑 Администратор удалил ваш ресурс: <b>{r.get('title') or r.get('username') or rid}</b>"
        )
    except Exception:
        pass
    
    await call.answer("Ресурс удалён", show_alert=True)
    await _admin_resources_page(call, page)   


@router.message(F.text.regexp(r"^/del_\d+$"))
async def del_resource(message: Message) -> None:
    if not message.from_user or not message.text:
        return
    res_id = int(message.text.removeprefix("/del_"))
    await db.deactivate_resource(res_id, message.from_user.id)
    await message.answer(f"🗑 Ресурс #{res_id} удалён (если был ваш).")


@router.callback_query(F.data == "sell:settings")
async def settings_root(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    user = await db.get_user(call.from_user.id)
    if not user:
        return
    await call.message.edit_text(
        "⚙️ <b>Настройки чата</b>\n\nЭти настройки применяются к подключённым вами чатам.",
        reply_markup=kb_settings(user),
    )
    await call.answer()


@router.callback_query(F.data == "set:channels_count")
async def set_channels_count(call: CallbackQuery) -> None:
    rows = [[(str(n), f"setval:channels_count:{n}")] for n in CHANNEL_COUNT_OPTIONS]
    rows.append([("◀️ Назад", "sell:settings")])
    if call.message:
        await call.message.edit_text(
            "📊 Сколько каналов будет в ОП в вашем чате?", reply_markup=ikb(rows)
        )
    await call.answer()


@router.callback_query(F.data == "set:bind_hours")
async def set_bind_hours(call: CallbackQuery) -> None:
    rows = [[(f"{h} ч.", f"setval:bind_hours:{h}")] for h in BIND_TIMES_HOURS]
    rows.append([("◀️ Назад", "sell:settings")])
    if call.message:
        await call.message.edit_text(
            "⏱ На сколько времени привязывать пользователей?", reply_markup=ikb(rows)
        )
    await call.answer()


@router.callback_query(F.data == "set:self_cat")
async def set_self_cat(call: CallbackQuery) -> None:
    rows = [[(label, f"setval:self_cat:{key}")] for key, label in CATEGORIES]
    rows.append([("◀️ Назад", "sell:settings")])
    if call.message:
        await call.message.edit_text(
            "📁 Какая категория у вашего чата?", reply_markup=ikb(rows)
        )
    await call.answer()


@router.callback_query(F.data == "set:show_cats")
async def set_show_cats(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    user = await db.get_user(call.from_user.id)
    if not user:
        return
    enabled = set(user.chat_show_categories.split(","))
    rows = []
    for key, label in CATEGORIES:
        mark = "✅" if key in enabled else "❌"
        rows.append([(f"{mark} {label}", f"togglecat:{key}")])
    rows.append([("◀️ Назад", "sell:settings")])
    await call.message.edit_text(
        "📂 Какие категории ОП показывать в вашем чате? (тапайте чтобы включать/выключать)",
        reply_markup=ikb(rows),
    )
    await call.answer()


@router.callback_query(F.data.startswith("togglecat:"))
async def toggle_cat(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    key = call.data.split(":", 1)[1]
    user = await db.get_user(call.from_user.id)
    if not user or key not in CATEGORY_BY_KEY:
        return
    enabled = set(filter(None, user.chat_show_categories.split(",")))
    if key in enabled:
        enabled.discard(key)
    else:
        enabled.add(key)
    await db.update_user_settings(call.from_user.id, chat_show_categories=",".join(sorted(enabled)))
    user = await db.get_user(call.from_user.id)
    if user is None:
        return
    enabled = set(user.chat_show_categories.split(","))
    rows = []
    for k, label in CATEGORIES:
        mark = "✅" if k in enabled else "❌"
        rows.append([(f"{mark} {label}", f"togglecat:{k}")])
    rows.append([("◀️ Назад", "sell:settings")])
    await call.message.edit_reply_markup(reply_markup=ikb(rows))
    await call.answer()


@router.callback_query(F.data.startswith("setval:"))
async def set_val(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    _, field, value = call.data.split(":", 2)
    update: dict[str, Any] = {}
    if field == "channels_count":
        update["chat_channels_count"] = int(value)
    elif field == "bind_hours":
        update["chat_bind_hours"] = int(value)
    elif field == "self_cat":
        update["chat_self_category"] = value
    if update:
        await db.update_user_settings(call.from_user.id, **update)
    user = await db.get_user(call.from_user.id)
    if user:
        await call.message.edit_text(
            "⚙️ <b>Настройки чата</b>", reply_markup=kb_settings(user)
        )
    await call.answer("Сохранено")


KIND_LABEL = {
    "channel_sub": "📢 Канал",
    "chat_join":   "💬 Чат",
    "bot_start":   "🤖 Бот",
    "post_view":   "👁 Просмотры поста",
    "reaction":    "👍 Реакции",
}

KIND_PRICE_KEY = {
    "channel_sub": "price_channel_sub",
    "chat_join":   "price_chat_join",
    "bot_start":   "price_bot_start",
    "post_view":   "price_view_post",
    "reaction":    "price_reaction",
}


@router.callback_query(F.data.regexp(r"^buy:(channel_sub|chat_join|bot_start|post_view|reaction|auto_views)$"))
async def buy_kind(call: CallbackQuery, state: FSMContext) -> None:
    kind = call.data.split(":", 1)[1]
    await state.clear()
    await state.update_data(kind=kind)
    if kind == "auto_views":
        if call.message:
            await _show_auto_views_menu(call)
        await call.answer()
        return
    if kind == "post_view":
        await state.set_state(BuyOrder.waiting_post_text)
        instr = (
            "Перешлите или отправьте сюда ваш <b>пост</b> (любое сообщение - текст, фото, видео, кнопки).\n\n"
            "Бот опубликует его в общем <b>канале для просмотров</b> с кнопкой «👁 Просмотрел» и покажет задание в подключённых чатах.\n"
            "Форматирование, ссылки и медиа сохраняются полностью."
        )
        if not await db.cfg_get("view_channel_id", ""):
            instr += (
                "\n\n⚠️ <b>Внимание:</b> канал для просмотров ещё не настроен администратором бота — "
                "заказ создать можно, но пост опубликуется после настройки."
            )
        else:
            view_username = await db.cfg_get("view_channel_username", "")
            if view_username:
                instr += f"\n\n📢 Пост будет опубликован в канале: <b>@{view_username}</b>"
        if call.message:
            await call.message.edit_text(
                f"<b>{KIND_LABEL[kind]}</b>\n\n{instr}", reply_markup=kb_back("menu:buy")
            )
        await call.answer()
        return
    await state.set_state(BuyOrder.waiting_link)
    instructions = {
        "channel_sub": (
            "Пришлите ссылку на канал (@username или https://t.me/username).\n\n"
            "⚠️ Бот <b>@AutoOP_Bot</b> должен быть <b>администратором</b> в канале для проверки подписок."
        ),
        "chat_join": (
            "Пришлите ссылку на чат (@username или https://t.me/username).\n\n"
            "⚠️ Бот <b>@AutoOP_Bot</b> должен быть <b>администратором</b> в чате для проверки участников."
        ),
        "bot_start": (
            "Пришлите @username бота или реферальную ссылку (https://t.me/bot?start=ref).\n\n"
            "📋 <b>Выполнение заказа:</b>\n"
            "├ Старты распределяются по ресурсам\n"
            "├ Срок выполнения: от <b>1 часа</b>\n"
            "└ <i>Максимальное время — до 72 часов</i>"
        ),
        "reaction":    "Пришлите ссылку на пост, на который накручиваем реакции.",
    }[kind]
    if call.message:
        await call.message.edit_text(
            f"<b>{KIND_LABEL[kind]}</b>\n\n{instructions}",
            reply_markup=kb_back("menu:buy"),
        )
    await call.answer()


@router.message(BuyOrder.waiting_post_text)
async def buy_post_text(message: Message, state: FSMContext) -> None:
    if not message.from_user or not message.chat:
        return
    await state.update_data(
        copy_chat_id=message.chat.id,
        copy_msg_id=message.message_id,
        target_link=f"post by {message.from_user.id}",
        category="misc",
    )
    await state.set_state(BuyOrder.waiting_quantity)
    vtitle = await db.cfg_get("view_channel_title", "")
    vlink = await db.cfg_get("view_channel_link", "")
    if vtitle and vlink:
        chan_info = f"\n\n📢Пост будет опубликован в канале для просмотров: <b>{vtitle}</b>\n{vlink}"
    elif vtitle:
        chan_info = f"\n\n📢Пост будет опубликован в: <b>{vtitle}</b>"
    else:
        chan_info = (
            "\n\n⚠️ Канал для просмотров ещё не настроен администратором — "
            "пост опубликуется после настройки."
        )
    await message.answer(
        f"✅Пост сохранён.{chan_info}\n\nСколько просмотров нужно? Введите число.",
        reply_markup=kb_back("menu:buy"),
    )


@router.message(BuyOrder.waiting_link)
async def buy_link(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return

    target_link: str | None = None
    target_chat_id: int | None = None
    post_id: int | None = None
    username: str | None = None
    invite: str | None = None

    # Сначала получаем данные из сообщения
    if message.forward_from_chat:
        chat = message.forward_from_chat
        target_chat_id = chat.id
        username = chat.username
        target_link = f"https://t.me/{chat.username}" if chat.username else f"chat_id={chat.id}"
        if message.forward_from_message_id:
            post_id = message.forward_from_message_id
    elif message.forward_from and message.forward_from.is_bot:
        # Переслано сообщение от бота
        bot_user = message.forward_from
        username = bot_user.username
        target_chat_id = bot_user.id
        target_link = f"https://t.me/{username}" if username else f"bot:{bot_user.id}"
    elif message.text:
        parsed = parse_link(message.text)
        username = parsed.get("username")
        invite = parsed.get("invite")
        post_id = parsed.get("post_id")
        target_link = message.text.strip()
        if username:
            try:
                chat = await bot.get_chat(f"@{username}")
                target_chat_id = chat.id
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
    elif message.caption:
        parsed = parse_link(message.caption)
        username = parsed.get("username")
        invite = parsed.get("invite")
        target_link = message.caption.strip()
        if username:
            try:
                chat = await bot.get_chat(f"@{username}")
                target_chat_id = chat.id
            except (TelegramBadRequest, TelegramForbiddenError):
                pass

    data = await state.get_data()
    kind = data["kind"]
    
    # Обработка для bot_start
    if kind == "bot_start":
        if not username:
            await message.answer(
                "❌ Для заказа бота укажите его @username или реферальную ссылку.\n\n"
                "📌 Примеры:\n"
                "• @CashPrufit_bot\n"
                "• https://t.me/CashPrufit_bot?start=ref123\n\n"
                "Пользователи перейдут по ссылке и запустят бота."
            )
            return
        
        # Сохраняем ссылку как отправил заказчик
        target_link = message.text.strip() if message.text else f"https://t.me/{username}"
        
        await state.update_data(
            target_link=target_link,
            target_chat_id=target_chat_id,
            post_id=post_id,
        )
        await state.set_state(BuyOrder.waiting_category)
        await message.answer(
            "📁Выберите категорию заказа:",
            reply_markup=kb_categories("buycat"),
        )
        return

    # Обработка для каналов и чатов
    if not username and not invite and not target_chat_id:
        await message.answer(
            "❌Не удалось распознать ссылку.\n\n"
            "Примеры:\n"
            "• https://t.me/channel_name\n"
            "• @channel_name\n"
            "• Перешлите сообщение из канала/чата"
        )
        return

    if kind in ("channel_sub", "chat_join"):
        if invite:
            # Пытаемся получить chat_id через инвайт-ссылку
            if not target_chat_id:
                try:
                    chat_info = await bot.get_chat(f"https://t.me/+{invite}")
                    target_chat_id = chat_info.id
                    log.info("Got chat_id %s from invite %s", target_chat_id, invite)
                except Exception as e:
                    log.warning("Cannot get chat_id from invite %s: %s", invite, e)
                    # Продолжаем без target_chat_id — будет отчёт админу
            
            await state.update_data(
                target_link=target_link,
                target_chat_id=target_chat_id,
                post_id=post_id,
                invite=invite,
            )
            await state.set_state(BuyOrder.waiting_category)
            await message.answer(
                "📁Выберите категорию заказа:",
                reply_markup=kb_categories("buycat"),
            )
            return
        
        if not target_chat_id:
            await message.answer(
                "❌ Не удалось получить ID канала/чата по этой ссылке.\n"
                "Используйте публичную ссылку вида @username или https://t.me/username, "
                "или пригласительную ссылку https://t.me/+XXXXXX"
            )
            return
        try:
            me = await bot.get_me()
            mem = await bot.get_chat_member(target_chat_id, me.id)
            ok = mem.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
        except Exception:
            ok = False
        if not ok:
            what = "канал" if kind == "channel_sub" else "чат"
            await message.answer(
                f"❌ Бот не админ в этом {what}е.\n\n"
                f"Чтобы заказ можно было принять — добавьте <b>@{(await bot.get_me()).username}</b> "
                f"администратором в ваш {what} (минимум право «Видеть участников»), "
                f"и отправьте ссылку снова."
            )
            return
            
    await state.update_data(
        target_link=target_link,
        target_chat_id=target_chat_id,
        post_id=post_id,
    )
    if kind in ("channel_sub", "chat_join", "bot_start"):
        await state.set_state(BuyOrder.waiting_category)
        await message.answer(
            "📁Выберите категорию заказа:",
            reply_markup=kb_categories("buycat"),
        )
    else:
        await state.update_data(category="misc")
        await state.set_state(BuyOrder.waiting_quantity)
        await message.answer("Сколько действий нужно? Введите число.")


@router.callback_query(BuyOrder.waiting_category, F.data.startswith("buycat:"))
async def buy_cat(call: CallbackQuery, state: FSMContext) -> None:
    cat = call.data.split(":", 1)[1]
    if cat == "cancel":
        await state.clear()
        if call.message:
            await call.message.edit_text("Отменено.", reply_markup=kb_buy_root())
        await call.answer()
        return
    if cat not in CATEGORY_BY_KEY:
        await call.answer("?", show_alert=True)
        return
    await state.update_data(category=cat)
    await state.set_state(BuyOrder.waiting_quantity)
    if call.message:
        await call.message.edit_text(
            f"Категория: {CATEGORY_BY_KEY[cat]}\n\nСколько нужно? Введите число.",
            reply_markup=kb_back("menu:buy"),
        )
    await call.answer()


@router.message(BuyOrder.waiting_quantity)
async def buy_qty(message: Message, state: FSMContext) -> None:
    if not message.from_user or not message.text:
        return
    try:
        qty = int(message.text.strip())
        if qty < 1 or qty > 100000:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите целое число от 1 до 100000.")
        return
    data = await state.get_data()
    kind = data.get("kind", "")
    min_qty = await get_min_order_qty(kind)
    if qty < min_qty:
        await message.answer(f"❌ Минимум для этого заказа: <b>{min_qty}</b>.")
        return
    await state.update_data(quantity=qty, duration_h=0)
    await _show_confirm(message, state)


@router.callback_query(BuyOrder.waiting_duration, F.data.startswith("buydur:"))
async def buy_dur(call: CallbackQuery, state: FSMContext) -> None:
    h = int(call.data.split(":")[1])
    await state.update_data(duration_h=h)
    if call.message:
        await _show_confirm(call.message, state, edit=True)
    await call.answer()


async def _show_confirm(message: Message, state: FSMContext, edit: bool = False) -> None:
    data = await state.get_data()
    kind = data["kind"]
    qty = int(data["quantity"])
    cat = data.get("category", "misc")
    dur = int(data.get("duration_h", 0))
    price_key = {
        "channel_sub": "channel_sub",
        "chat_join":   "chat_join",
        "bot_start":   "bot_start",
        "post_view":   "view_post",
        "reaction":    "reaction",
    }[kind]
    price_per = await get_buy_price(price_key)
    total = qty * price_per
    await state.update_data(price_total=round(total, 2))

    user_id = message.chat.id if message.chat else None
    if user_id is None:
        return
    user = await db.get_user(user_id)
    target_str = data.get('target_link') or "(пост)"
    srok_line = f"Срок: {dur} ч.\n" if dur > 0 else "Срок: до набора нужного количества\n"
    text = (
        f"<b>Подтверждение заказа</b>\n\n"
        f"Тип: {KIND_LABEL[kind]}\n"
        f"Цель: {target_str}\n"
        f"Категория: {CATEGORY_BY_KEY.get(cat, cat)}\n"
        f"Кол-во: {qty}\n"
        f"{srok_line}"
        f"Цена за единицу: {fmt_money(price_per)}\n"
        f"<b>Итого: {fmt_money(total)}</b>\n\n"
        f"Ваш баланс: {fmt_money(user.balance if user else 0)}"
    )
    rows = [[("✅ Создать заказ", "buy:confirm"), ("❌ Отмена", "buy:cancel")]]
    if edit:
        await message.edit_text(text, reply_markup=ikb(rows))
    else:
        await message.answer(text, reply_markup=ikb(rows))
    await state.set_state(BuyOrder.confirm)


@router.callback_query(BuyOrder.confirm, F.data == "buy:confirm")
async def buy_confirm(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user:
        return
    data = await state.get_data()
    user = await db.get_user(call.from_user.id)
    if not user:
        await call.answer("Сначала /start", show_alert=True)
        return
    total = float(data["price_total"])
    if user.balance < total:
        if call.message:
            await call.message.edit_text(
                f"❌ Недостаточно средств. Нужно {fmt_money(total)}, у вас {fmt_money(user.balance)}.\n"
                f"Пополните баланс через «💰 Ваш баланс».",
                reply_markup=kb_back("menu:buy"),
            )
        await state.clear()
        await call.answer()
        return
    duration_h = int(data.get("duration_h", 0))
    expires_at = int(time.time()) + duration_h * 3600 if duration_h > 0 else None
    order_kwargs = dict(
        user_id=call.from_user.id,
        kind=data["kind"],
        target_link=data.get("target_link") or "",
        target_chat_id=data.get("target_chat_id"),
        post_id=data.get("post_id"),
        category=data.get("category", "misc"),
        quantity=int(data["quantity"]),
        duration_h=duration_h,
        expires_at=expires_at,
        price_total=total,
    )
    if data.get("copy_chat_id"):
        order_kwargs["copy_chat_id"] = int(data["copy_chat_id"])
    if data.get("copy_msg_id"):
        order_kwargs["copy_msg_id"] = int(data["copy_msg_id"])
    order_id = await db.create_order(**order_kwargs)
    await db.add_balance(call.from_user.id, -total, "spend", f"order #{order_id}")
    await state.clear()
    if call.message:
        srok = f"Срок: {duration_h} ч." if duration_h > 0 else "Срок: до набора нужного количества."
        await call.message.edit_text(
            f"✅ Заказ #{order_id} создан и запущен!\n"
            f"Списано: {fmt_money(total)}.\n"
            f"{srok}",
            reply_markup=kb_back("menu:buy"),
        )
    await call.answer("Заказ создан")
    # Если заказ на бота — уведомляем админов
    if data["kind"] == "bot_start":
        admins = await _all_admin_ids()
        for adm in admins:
            try:
                await bot.send_message(
                    adm,
                    f"🤖 <b>Новый заказ на бота!</b>\n\n"
                    f"📋 Заказ #{order_id}\n"
                    f"👤 Заказчик: <code>{call.from_user.id}</code>\n"
                    f"🔗 Ссылка: {data.get('target_link', '-')}\n"
                    f"📊 Количество: {int(data['quantity'])}\n"
                    f"💰 Потрачено: {fmt_money(total)}\n"
                    f"🕐 Создан: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                )
            except Exception:
                pass


@router.callback_query(BuyOrder.confirm, F.data == "buy:cancel")
async def buy_cancel(call: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    if call.message:
        await call.message.edit_text("Отменено.", reply_markup=kb_buy_root())
    await call.answer()


_ORDER_ICON = {
    "channel_sub": "📢", "chat_join": "💬", "bot_start": "🤖",
    "post_view": "👁", "reaction": "👍", "post_in_channel": "📣",
}
ORD_PAGE_SIZE = 5


def _ord_button_label(o: dict) -> str:
    icon = _ORDER_ICON.get(o["kind"], "❔")
    status_mark = {"active": "", "done": "✅ ", "cancelled": "❌ "}.get(o["status"], "")
    # Добавляем стоимость и время
    price = fmt_money(o['price_total'])
    if o.get('duration_h') and o['duration_h'] > 0:
        time_info = f"{o['duration_h']}ч"
    else:
        time_info = "∞"
    return f"{status_mark}{icon} #{o['id']} • {o['completed']}/{o['quantity']} • {price} • {time_info}"


async def _render_orders_page(call: CallbackQuery, page: int) -> None:
    if not call.from_user or not call.message:
        return
    all_orders = await db.list_user_orders(call.from_user.id)
    orders = [o for o in all_orders if o["status"] == "active"]
    if not orders:
        await call.message.edit_text(
            "📋 У вас нет активных заказов.",
            reply_markup=ikb([[("◀️ Назад", "menu:buy")]]),
        )
        return
    pages = max(1, math.ceil(len(orders) / ORD_PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    chunk = orders[page * ORD_PAGE_SIZE:(page + 1) * ORD_PAGE_SIZE]
    btn_rows: list[list[tuple[str, str]]] = []
    for o in chunk:
        btn_rows.append([(_ord_button_label(o), f"ord:{o['id']}:{page}")])
    nav: list[tuple[str, str]] = []
    if page > 0:
        nav.append(("◀️", f"ordlist:{page - 1}"))
    nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"ordlist:{page + 1}"))
    btn_rows.append(nav)
    btn_rows.append([("◀️ Назад", "menu:buy")])
    await call.message.edit_text(
        "Список ваших активных заказов.",
        reply_markup=ikb(btn_rows),
    )


@router.callback_query(F.data == "buy:list")
async def buy_list(call: CallbackQuery) -> None:
    await _render_orders_page(call, 0)
    await call.answer()


@router.callback_query(F.data.startswith("ordlist:"))
async def ordlist_page(call: CallbackQuery) -> None:
    try:
        page = int(call.data.split(":", 1)[1])
    except (ValueError, IndexError):
        page = 0
    await _render_orders_page(call, page)
    await call.answer()


def _kb_order_card(o: dict, page: int) -> InlineKeyboardMarkup:
    rows: list[list[tuple[str, str]]] = []
    link = o.get("target_link")
    if link:
        if not link.startswith("http"):
            uname = link.lstrip("@").strip()
            if re.match(r"^[A-Za-z0-9_]+$", uname):
                link = f"https://t.me/{uname}"
            else:
                link = None
    if link:
        url_btn = InlineKeyboardButton(text="🔗 Ссылка", url=link)
    else:
        url_btn = None
    if o["status"] == "active" and o["kind"] != "post_in_channel":
        rows.append([("❌ Отменить", f"ord:{o['id']}:{page}:cancel")])
    rows.append([("◀️ К списку", f"ordlist:{page}")])
    inline_kb: list[list[InlineKeyboardButton]] = []
    if url_btn:
        inline_kb.append([url_btn])
    for row in rows:
        inline_kb.append([InlineKeyboardButton(text=t, callback_data=d) for t, d in row])
    return InlineKeyboardMarkup(inline_keyboard=inline_kb)


@router.callback_query(F.data.regexp(r"^ord:\d+:\d+$"))
async def ord_open(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    parts = call.data.split(":")
    oid = int(parts[1]); page = int(parts[2])
    o = await db.get_order(oid)
    if not o or o["user_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True); return
    icon = _ORDER_ICON.get(o["kind"], "❔")
    
    # Дополнительная информация
    kind_label = KIND_LABEL.get(o["kind"], o["kind"])
    category = CATEGORY_BY_KEY.get(o.get("category", "misc"), o.get("category", "-"))
    created = datetime.fromtimestamp(o["created_at"], tz=timezone.utc).strftime("%d.%m.%Y %H:%M")
    
    # Срок
    if o.get('duration_h') and o['duration_h'] > 0:
        time_info = f"<b>⏱ Срок:</b> {o['duration_h']} ч."
        if o.get('expires_at'):
            expires = datetime.fromtimestamp(o['expires_at'], tz=timezone.utc).strftime("%d.%m.%Y %H:%M")
            time_info += f"\n<b>⏰ Истекает:</b> {expires}"
    else:
        time_info = "<b>⏱ Срок:</b> до выполнения"
    
    # Стоимость за единицу
    price_per_unit = float(o['price_total']) / max(1, int(o['quantity']))
    
    # Статус выполнения
    if o["kind"] == "bot_start" and o["status"] == "active":
        progress_text = "<b>✅ Статус:</b> выполняется"
    else:
        progress_text = f"<b>✅ Выполнено:</b> {o['completed']}/{o['quantity']}"
    
    text = (
        f"{icon} <b>Заказ #{o['id']}</b>\n\n"
        f"<b>📋 Тип:</b> {kind_label}\n"
        f"<b>📁 Категория:</b> {category}\n"
        f"<b>🎯 Цель:</b> {o.get('target_link') or '-'}\n"
        f"{progress_text}\n"
        f"<b>💵 Цена за ед.:</b> {fmt_money(price_per_unit)}\n"
        f"<b>💰 Общая стоимость:</b> {fmt_money(o['price_total'])}\n"
        f"{time_info}\n"
        f"<b>📅 Создан:</b> {created}\n"
        f"<b>📊 Статус:</b> {o['status']}"
    )
    await call.message.edit_text(text, reply_markup=_kb_order_card(o, page))
    await call.answer()


@router.callback_query(F.data.regexp(r"^ord:\d+:\d+:cancel$"))
async def ord_cancel(call: CallbackQuery) -> None:
    if not call.from_user or not call.message:
        return
    parts = call.data.split(":")
    oid = int(parts[1]); page = int(parts[2])
    o = await db.get_order(oid)
    if not o or o["user_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True); return
    if o["status"] != "active":
        await call.answer("Заказ уже не активен.", show_alert=True); return
    
    # Запрещаем отмену post_in_channel
    if o["kind"] == "post_in_channel":
        await call.answer("❌ Нельзя отменить размещение поста в канале.", show_alert=True)
        return
    
    # Для ботов возвращаем 75%
    if o["kind"] == "bot_start":
        refund = round(float(o["price_total"]) * 0.75, 2)
    else:
        refund = float(o["price_total"]) * (1 - (o["completed"] / max(1, o["quantity"])))
        refund = round(refund, 2)
    
    if await db.cancel_order(oid, call.from_user.id):
        if refund > 0:
            await db.add_balance(call.from_user.id, refund, "refund", f"cancel order #{oid}")
    
    await call.answer(
        f"Отменён. Возврат: {fmt_money(refund)}", show_alert=True
    )
    await _render_orders_page(call, page)


@router.message(F.text.regexp(r"^/cancel_\d+$"))
async def cancel_order(message: Message) -> None:
    if not message.from_user or not message.text:
        return
    oid = int(message.text.removeprefix("/cancel_"))
    o = await db.get_order(oid)
    if not o or o["user_id"] != message.from_user.id:
        await message.answer("Заказ не найден.")
        return
    if o["status"] != "active":
        await message.answer("Заказ уже не активен.")
        return
    refund = float(o["price_total"]) * (1 - (o["completed"] / max(1, o["quantity"])))
    refund = round(refund, 2)
    if await db.cancel_order(oid, message.from_user.id):
        if refund > 0:
            await db.add_balance(message.from_user.id, refund, "refund", f"cancel order #{oid}")
        await message.answer(
            f"❌ Заказ #{oid} отменён. Возврат: {fmt_money(refund)}."
        )


PIC_DURATIONS = [(1, "1 час"), (12, "12 часов"), (24, "1 день"), (24*7, "7 дней")]


async def _refresh_member_count(resource_id: int, tg_chat_id: int) -> int:
    try:
        n = await bot.get_chat_member_count(tg_chat_id)
    except Exception:
        return 0
    async with db.conn() as c:
        await c.execute(
            "UPDATE resources SET members_count=?, members_updated_at=? WHERE id=?",
            (int(n), int(time.time()), resource_id),
        )
        await c.commit()
    return int(n)


@router.callback_query(F.data == "buy:post_in_channel")
async def buy_pic_root(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not call.message:
        return
    await state.clear()
    await state.set_state(BuyOrder.pic_choose_channel)
    await _render_pic_channels(call, 0)
    await call.answer()
    
async def _render_pic_channels(call: CallbackQuery, page: int) -> None:
    """Отображает список каналов с пагинацией."""
    if not call.from_user or not call.message:
        return
    
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT * FROM resources WHERE type='channel' AND is_active=1"
        )
        chans = [dict(r) for r in await cur.fetchall()]
    
    now = int(time.time())
    for ch in chans:
        if not ch.get("members_updated_at") or now - int(ch["members_updated_at"]) > 3600:
            if ch.get("tg_chat_id"):
                ch["members_count"] = await _refresh_member_count(ch["id"], int(ch["tg_chat_id"]))
    
    chans.sort(key=lambda r: int(r.get("members_count") or 0), reverse=True)
    
    if not chans:
        await call.message.edit_text(
            "❌ В системе ещё нет подключённых каналов.",
            reply_markup=kb_back("menu:buy")
        )
        return
    
    # Получаем количество активных размещений для каждого канала
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT resource_id, COUNT(*) as cnt FROM placements WHERE status='live' GROUP BY resource_id"
        )
        placements_count = {r["resource_id"]: r["cnt"] for r in await cur.fetchall()}
    
    total_pages = max(1, math.ceil(len(chans) / PIC_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    
    start = page * PIC_PAGE_SIZE
    end = start + PIC_PAGE_SIZE
    chunk = chans[start:end]
    
    btn_rows: list[list[tuple[str, str]]] = []
    temp_row: list[tuple[str, str]] = []
    
    for i, ch in enumerate(chunk):
        title = ch.get("title") or ch.get("username") or f"#{ch['id']}"
        m = int(ch.get("members_count") or 0)
        max_pl = int(ch.get("max_placements") or 3)
        live_count = placements_count.get(ch["id"], 0)
        
        if live_count >= max_pl:
            label = f"🔴 {title} ({m}) — занят"
        else:
            free = max_pl - live_count
            label = f"🟢 {title} ({m}) — {free} мест"
        
        temp_row.append((label, f"pic_pick:{ch['id']}"))
        
        if len(temp_row) == 2 or i == len(chunk) - 1:
            btn_rows.append(temp_row)
            temp_row = []
    
    nav: list[tuple[str, str]] = []
    if page > 0:
        nav.append(("◀️", f"pic_page:{page - 1}"))
    nav.append((f"📄 {page + 1}/{total_pages}", "noop"))
    if page < total_pages - 1:
        nav.append(("▶️", f"pic_page:{page + 1}"))
    btn_rows.append(nav)
    btn_rows.append([("◀️ Отмена", "menu:buy")])
    
    await call.message.edit_text(
        f"📝 <b>Пост в канале</b>\n\n"
        f"🟢 — есть места | 🔴 — занят\n"
        f"Выберите канал:\n"
        f"Страница {page + 1} из {total_pages}",
        reply_markup=ikb(btn_rows)
    )
    
@router.callback_query(BuyOrder.pic_choose_channel, F.data.startswith("pic_page:"))
async def pic_page_turn(call: CallbackQuery) -> None:
    """Перелистывание страниц каналов."""
    if not call.from_user:
        return
    try:
        page = int(call.data.split(":", 1)[1])
    except (ValueError, IndexError):
        page = 0
    await _render_pic_channels(call, page)
    await call.answer()    


@router.callback_query(BuyOrder.pic_choose_channel, F.data.startswith("pic_pick:"))
async def buy_pic_pick(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not call.message:
        return
    rid = int(call.data.split(":", 1)[1])
    async with db.conn() as c:
        cur = await c.execute("SELECT * FROM resources WHERE id=?", (rid,))
        row = await cur.fetchone()
    if not row:
        await call.answer("Канал не найден", show_alert=True); return
    ch = dict(row)
    
    # Проверяем лимит
    max_pl = int(ch.get("max_placements") or 3)
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT COUNT(*) FROM placements WHERE resource_id=? AND status='live'",
            (rid,)
        )
        live_count = (await cur.fetchone())[0]
    
    if live_count >= max_pl:
        await call.answer(
            f"🔴 Канал занят  ({max_pl}/{max_pl}). Выберите другой канал.",
            show_alert=True
        )
        return
    
    title = ch.get("title") or ch.get("username") or f"#{ch['id']}"
    members = int(ch.get("members_count") or 0)
    price_h = float(ch.get("price_per_hour") or 50)
    link = f"https://t.me/{ch['username']}" if ch.get("username") else "(приватный канал)"
    
    # Информация о лимите
    free_slots = max_pl - live_count
    limit_info = f"\n📊 Свободных мест: <b>{free_slots}</b> из {max_pl}"
    
    await state.update_data(pic_resource_id=rid, pic_price_per_hour=price_h, pic_title=title)
    text = (
        f"📢 <b>{title}</b>\n"
        f"🔗 {link}\n"
        f"👥 Подписчиков: <b>{members}</b>\n"
        f"💵 Цена за 1 час рекламы: <b>{fmt_money(price_h)}</b>"
        f"{limit_info}\n\n"
        f"Если хотите купить - нажмите «💳 Купить»."
    )
    rows = [
        [("💳 Купить", "pic_buy")],
        [("◀️ Назад", "buy:post_in_channel")],
    ]
    await call.message.edit_text(text, reply_markup=ikb(rows), disable_web_page_preview=True)
    await call.answer()


@router.callback_query(BuyOrder.pic_choose_channel, F.data == "pic_buy")
async def buy_pic_ask_post(call: CallbackQuery, state: FSMContext) -> None:
    if not call.message:
        return
    await state.set_state(BuyOrder.pic_post)
    await call.message.edit_text(
        "📝 <b>Отправьте пост для размещения</b>\n\n"
        "Вы можете:\n"
        "• Переслать готовый пост из канала\n"
        "• Отправить фото/видео с текстом\n"
        "• Отправить просто текст\n\n"
        "🔗 Ссылку можно вставить прямо в текст — она будет кликабельной.",
        reply_markup=kb_back("menu:buy"),
    )
    await call.answer()

@router.message(BuyOrder.pic_post)
async def buy_pic_post_recv(message: Message, state: FSMContext) -> None:
    if not message.chat:
        return
    
    # Сохраняем как есть — без парсинга кнопок
    await state.update_data(
        pic_copy_chat_id=message.chat.id,
        pic_copy_msg_id=message.message_id
    )
    
    await state.set_state(BuyOrder.pic_duration)
    
    data = await state.get_data()
    price_h = float(data.get("pic_price_per_hour", 50))
    
    # Формируем кнопки с ценами
    rows = []
    for h, label in PIC_DURATIONS:
        total_price = round(price_h * h, 2)
        rows.append([(f"{label} — {fmt_money(total_price)}", f"pic_dur:{h}")])
    rows.append([("◀️ Отмена", "menu:buy")])
    
    await message.answer(
        f"✅ Пост сохранён.\n\n"
        f"💵 Цена за 1 час: <b>{fmt_money(price_h)}</b>\n\n"
        f"💡 <b>Совет:</b> Вставьте ссылку в текст или используйте "
        f"<a href='https://t.me/BotFather'>@BotFather</a> для создания постов с кнопками.\n\n"
        f"⏱ Выберите время размещения:",
        reply_markup=ikb(rows),
        disable_web_page_preview=True
    )


@router.callback_query(BuyOrder.pic_duration, F.data.startswith("pic_dur:"))
async def buy_pic_duration(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not call.message:
        return
    h = int(call.data.split(":", 1)[1])
    data = await state.get_data()
    price_h = float(data.get("pic_price_per_hour") or 50)
    total = round(price_h * h, 2)
    title = data.get("pic_title", "канал")
    await state.update_data(pic_duration_h=h, pic_total=total)
    await state.set_state(BuyOrder.pic_confirm)
    user = await db.get_user(call.from_user.id)
    bal = user.balance if user else 0
    enough = bal >= total
    text = (
        f"📝 <b>Подтверждение</b>\n\n"
        f"📢 Канал: <b>{title}</b>\n"
        f"⏱ Время: <b>{h} ч.</b>\n"
        f"💰 Стоимость: <b>{fmt_money(total)}</b>\n"
        f"💵 Ваш баланс: {fmt_money(bal)}"
    )
    rows = []
    if enough:
        rows.append([("✅ Подтвердить и оплатить", "pic_confirm")])
    else:
        rows.append([("💳 Пополнить", "bal:deposit")])
    rows.append([("◀️ Отмена", "menu:buy")])
    await call.message.edit_text(text, reply_markup=ikb(rows))
    await call.answer()


@router.callback_query(BuyOrder.pic_confirm, F.data == "pic_confirm")
async def buy_pic_confirm(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not call.message:
        return
    data = await state.get_data()
    rid = int(data["pic_resource_id"])
    h = int(data["pic_duration_h"])
    total = float(data["pic_total"])
    copy_chat = int(data["pic_copy_chat_id"])
    copy_msg = int(data["pic_copy_msg_id"])
    user = await db.get_user(call.from_user.id)
    if not user or user.balance < total:
        await call.answer("Недостаточно средств", show_alert=True); return
    async with db.conn() as c:
        cur = await c.execute("SELECT * FROM resources WHERE id=?", (rid,))
        row = await cur.fetchone()
    if not row:
        await call.answer("Канал недоступен", show_alert=True); return
    ch = dict(row)
    if not ch.get("tg_chat_id"):
        await call.answer("Канал недоступен", show_alert=True); return
    
    # Проверяем лимит размещений в канале
    max_pl = int(ch.get("max_placements") or 3)
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT COUNT(*) FROM placements WHERE resource_id=? AND status='live'",
            (rid,)
        )
        live_count = (await cur.fetchone())[0]
    
    if live_count >= max_pl:
        await call.answer(
            f"❌ Канал достиг лимита рекламных постов ({max_pl}). Попробуйте позже.",
            show_alert=True
        )
        return
    
    # Пересылаем с сохранением кнопок
    try:
        cp = await bot.copy_message(
            chat_id=int(ch["tg_chat_id"]),
            from_chat_id=copy_chat,
            message_id=copy_msg,
        )
    except Exception as e:
        log.warning("pic publish copy fail: %s, trying forward", e)
        try:
            cp = await bot.forward_message(
                chat_id=int(ch["tg_chat_id"]),
                from_chat_id=copy_chat,
                message_id=copy_msg,
            )
        except Exception as e2:
            log.warning("pic publish fail: %s", e2)
            await call.answer(f"Не удалось разместить пост: {e2}", show_alert=True)
            return
    
    await db.add_balance(call.from_user.id, -total, "spend", "post_in_channel")
    now = int(time.time())
    expires = now + h * 3600
    async with db.conn() as c:
        cur = await c.execute(
            "INSERT INTO orders(user_id, kind, target_link, target_chat_id, "
            "category, quantity, completed, duration_h, expires_at, price_total, "
            "status, created_at, target_resource_id, copy_chat_id, copy_msg_id, frozen_payout) "
            "VALUES (?, 'post_in_channel', ?, ?, 'misc', 1, 0, ?, ?, ?, 'active', ?, ?, ?, ?, ?)",
            (call.from_user.id,
             ch.get("username") and f"https://t.me/{ch['username']}" or f"chat:{ch['tg_chat_id']}",
             ch["tg_chat_id"], h, expires, total, now, rid, copy_chat, copy_msg, total),
        )
        order_id = cur.lastrowid
        commission = await get_ads_commission()
        payout = round(total * max(0.0, 100.0 - commission) / 100.0, 2)
        await c.execute(
            "INSERT INTO placements(order_id, resource_id, placed_msg_id, placed_at, "
            "expires_at, payout, status, paid) VALUES (?, ?, ?, ?, ?, ?, 'live', 0)",
            (order_id, rid, cp.message_id, now, expires, payout),
        )
        await c.commit()
    await state.clear()
    title = ch.get("title") or ch.get("username") or f"#{ch['id']}"
    await call.message.edit_text(
        f"✅ Пост размещён в канале <b>{title}</b> на {h} ч.\n"
        f"💰 Списано: {fmt_money(total)}.\n\n"
        f"Если владелец канала удалит пост раньше срока - получите частичный возврат.",
        reply_markup=kb_back("menu:buy"),
    )
    try:
        await bot.send_message(
            int(ch["user_id"]),
            f"📥 В вашем канале <b>{title}</b> размещён рекламный пост на {h} ч.\n"
            f"Через {h} ч. вы получите выплату <b>{fmt_money(payout)}</b>.\n\n"
            f"<i>Если удалите пост до окончания - средства не будут начислены.</i>",
        )
    except Exception:
        pass
    await call.answer("Размещено")


async def _check_pic_deletions() -> None:
    """Проверяет, не удалён ли рекламный пост post_in_channel.

    Старый вариант вызывал edit_message_reply_markup(..., reply_markup=None) и
    тем самым мог сам удалить кнопки у рекламного поста. Безопасная проверка
    выполняется только если админ задал config.deletion_check_chat_id: бот
    пробует переслать пост в служебный приватный чат/канал и сразу удаляет
    тестовое сообщение. Если служебный чат не задан, проверка удалений
    пропускается, чтобы не портить рекламные публикации.
    """
    check_chat_raw = await db.cfg_get("deletion_check_chat_id", "")
    if not check_chat_raw:
        return

    try:
        check_chat_id = int(check_chat_raw)
    except ValueError:
        log.warning("pic deletion check disabled: invalid deletion_check_chat_id=%r", check_chat_raw)
        return

    async with db.conn() as c:
        cur = await c.execute(
            "SELECT p.*, o.kind, o.user_id AS buyer_id, o.duration_h, o.price_total, o.expires_at AS o_exp, "
            "       r.tg_chat_id, r.user_id AS owner_id, r.title, r.username "
            "FROM placements p JOIN orders o ON o.id=p.order_id "
            "JOIN resources r ON r.id=p.resource_id "
            "WHERE p.status='live' AND o.kind='post_in_channel'"
        )
        rows = [dict(r) for r in await cur.fetchall()]

    for p in rows:
        if not p.get("placed_msg_id") or not p.get("tg_chat_id"):
            continue

        try:
            copied = await bot.forward_message(
                chat_id=check_chat_id,
                from_chat_id=int(p["tg_chat_id"]),
                message_id=int(p["placed_msg_id"]),
                disable_notification=True,
            )
            try:
                await bot.delete_message(check_chat_id, copied.message_id)
            except Exception:
                pass
            continue

        except TelegramBadRequest as e:
            msg = str(e).lower()
            deleted = (
                "message to forward not found" in msg
                or "message_id_invalid" in msg
                or "message to be forwarded not found" in msg
                or "message not found" in msg
                or "not found" in msg
            )
            if not deleted:
                log.warning("pic deletion check err: %s", e)
                continue
        except Exception as e:
            log.warning("pic deletion check err: %s", e)
            continue

        async with db.conn() as c:
            cur = await c.execute("SELECT status FROM orders WHERE id=?", (p["order_id"],))
            order_row = await cur.fetchone()

        if order_row and order_row["status"] == "cancelled":
            async with db.conn() as c:
                await c.execute(
                    "UPDATE placements SET status='deleted_by_owner' WHERE id=?",
                    (p["id"],),
                )
                await c.commit()
            continue

        refund = round(float(p["price_total"]), 2)
        async with db.conn() as c:
            await c.execute(
                "UPDATE placements SET status='deleted_by_owner', paid=0 WHERE id=?",
                (p["id"],),
            )
            await c.execute(
                "UPDATE orders SET status='cancelled' WHERE id=?",
                (p["order_id"],),
            )
            await c.commit()

        if refund > 0:
            await db.add_balance(
                int(p["buyer_id"]),
                refund,
                "refund",
                f"post_in_channel deleted #{p['order_id']}",
            )

        title = p.get("title") or p.get("username") or "канал"
        try:
            await bot.send_message(
                int(p["owner_id"]),
                f"⚠️ Вы удалили рекламный пост в канале <b>{title}</b> до окончания срока.\n"
                f"Средства не начислены, покупателю выполнен полный возврат.",
            )
        except Exception:
            pass
        try:
            await bot.send_message(
                int(p["buyer_id"]),
                f"⚠️ Ваш пост в канале <b>{title}</b> был удалён владельцем раньше срока.\n"
                f"Выполнен <b>полный возврат</b>: {fmt_money(refund)}.",
            )
        except Exception:
            pass



@router.callback_query(F.data == "bal:history")
async def bal_history(call: CallbackQuery) -> None:
    if not call.from_user:
        return
    txs = await db.list_transactions(call.from_user.id, 15)
    if not txs:
        text = "📑 История пуста."
    else:
        lines = []
        for t in txs:
            ts = datetime.fromtimestamp(t["created_at"], tz=timezone.utc).strftime("%d.%m %H:%M")
            sign = "+" if t["amount"] >= 0 else ""
            lines.append(f"{ts} • {sign}{t['amount']:.2f} ₽ • {t['kind']} {t['comment'] or ''}")
        text = "📑 <b>История транзакций</b>\n\n" + "\n".join(lines)
    if call.message:
        await call.message.edit_text(text, reply_markup=kb_back("bal:back"))
    await call.answer()


class DepositFlow(StatesGroup):
    waiting_amount = State()


@router.callback_query(F.data == "bal:deposit")
async def bal_deposit(call: CallbackQuery, state: FSMContext) -> None:
    token = await db.cfg_get("cryptobot_token")
    if not token:
        await call.answer(
            "Пополнения временно недоступны: админ не подключил CryptoBot.",
            show_alert=True,
        )
        return
    if not call.message:
        return
    minv = await get_min_deposit()
    rate = await get_usdt_rub_rate()
    await state.set_state(DepositFlow.waiting_amount)
    await call.message.edit_text(
        f"💳 <b>Пополнение через CryptoBot</b>\n\n"
        f"Минимум: <b>{fmt_money(minv)}</b>\n"
        f"Текущий курс: <b>1 USDT = {rate:.2f} ₽</b>\n\n"
        f"Введите сумму в ₽ (число):",
        reply_markup=kb_back("bal:back"),
    )
    await call.answer()


@router.message(DepositFlow.waiting_amount)
async def bal_deposit_amount(message: Message, state: FSMContext) -> None:
    if not message.from_user or not message.text:
        return
    try:
        amount = float(message.text.replace(",", ".").strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число.")
        return
    minv = await get_min_deposit()
    if amount < minv:
        await message.answer(f"❌ Минимум {fmt_money(minv)}.")
        return
    invoice = await create_cryptobot_invoice(message.from_user.id, amount)
    if not invoice:
        await message.answer(
            "❌ Не удалось создать инвойс. Возможно, токен CryptoBot некорректен.",
            reply_markup=kb_back("bal:back"),
        )
        await state.clear()
        return
    pay_url = invoice.get("pay_url") or invoice.get("bot_invoice_url") or invoice.get("mini_app_invoice_url") or ""
    inv_id = str(invoice.get("invoice_id") or invoice.get("id") or "")
    if not pay_url or not inv_id:
        log.warning("CryptoBot invoice malformed: %s", invoice)
        await message.answer("❌ Ошибка инвойса.", reply_markup=kb_back("bal:back"))
        await state.clear()
        return
    await state.clear()
    rate = await get_usdt_rub_rate()
    usdt = round(amount / rate, 2)
    await message.answer(
        f"💳 Инвойс на <b>{fmt_money(amount)}</b> ({usdt} USDT) создан.\n\n"
        f"1. Нажмите «💰 Оплатить»\n"
        f"2. Оплатите счёт в @CryptoBot\n"
        f"3. Вернитесь и нажмите «Проверить»",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💰 Оплатить", url=pay_url)],
            [InlineKeyboardButton(text="✅ Я оплатил - проверить",
                                   callback_data=f"depcheck:{inv_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
        ]),
    )


@router.callback_query(F.data.startswith("depcheck:"))
async def bal_dep_check(call: CallbackQuery) -> None:
    if not call.from_user:
        return
    inv_id = call.data.split(":", 1)[1]
    res = await check_cryptobot_invoice(inv_id)
    
    log.info("depcheck: user=%s inv=%s status=%s", call.from_user.id, inv_id, res.get("status") if res else "None")
    
    if res and res.get("status") == "paid":
        amount = float(res["amount"])
        rate = await get_usdt_rub_rate()
        rub = round(amount * rate, 2)
        await db.add_balance(call.from_user.id, rub, "deposit", f"cryptobot {inv_id}")
        
        u = await db.get_user(call.from_user.id)
        log.info("depcheck: user=%s ref_id=%s", call.from_user.id, u.ref_id if u else None)
        
        if u and u.ref_id:
            ref_bonus = round(rub * await get_ref_percent() / 100, 2)
            log.info("depcheck: ref_bonus=%s for ref_id=%s", ref_bonus, u.ref_id)
            await db.add_balance(u.ref_id, ref_bonus, "ref", f"deposit by {call.from_user.id}")
            async with db.conn() as c:
                await c.execute("UPDATE users SET ref_earnings = ref_earnings + ? WHERE tg_id=?",
                                 (ref_bonus, u.ref_id))
                await c.commit()
        
        await call.answer(f"Зачислено {fmt_money(rub)}", show_alert=True)
        if call.message:
            await call.message.edit_text(
                f"✅ Пополнение успешно. Зачислено {fmt_money(rub)}.",
                reply_markup=kb_back("bal:back"),
            )
    else:
        await call.answer("Оплата ещё не пришла.", show_alert=True)


class WithdrawFlow(StatesGroup):
    waiting_amount = State()


@router.callback_query(F.data == "bal:withdraw")
async def bal_withdraw(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not call.message:
        return
    minw = await get_min_withdraw()
    user = await db.get_user(call.from_user.id)
    bal = user.balance if user else 0
    await state.set_state(WithdrawFlow.waiting_amount)
    await call.message.edit_text(
        f"💸 <b>Вывод средств</b>\n\n"
        f"Минимум: <b>{fmt_money(minw)}</b>\n"
        f"Ваш баланс: <b>{fmt_money(bal)}</b>\n\n"
        f"Введите сумму в ₽ (просто число).\n"
        f"После одобрения админом - получите чек CryptoBot в @CryptoBot.",
        reply_markup=kb_back("bal:back"),
    )
    await call.answer()


@router.message(WithdrawFlow.waiting_amount)
async def withdraw_amount(message: Message, state: FSMContext) -> None:
    if not message.from_user or not message.text:
        return
    try:
        amt = float(message.text.replace(",", ".").strip())
        if amt <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число.")
        return
    minw = await get_min_withdraw()
    if amt < minw:
        await message.answer(f"❌ Минимум {fmt_money(minw)}.")
        return
    user = await db.get_user(message.from_user.id)
    if not user or user.balance < amt:
        await message.answer("❌ Недостаточно средств.")
        return
    await db.add_balance(message.from_user.id, -amt, "withdraw_hold", "withdraw req")
    async with db.conn() as c:
        cur = await c.execute(
            "INSERT INTO transactions(user_id, kind, amount, comment, created_at) "
            "VALUES (?, 'withdraw_request', ?, 'pending', ?)",
            (message.from_user.id, amt, int(time.time())),
        )
        req_id = cur.lastrowid
        await c.commit()
    await state.clear()
    await message.answer(
        f"📨 Заявка на вывод <b>{fmt_money(amt)}</b> отправлена админу.\n"
        f"Ожидайте одобрения - чек CryptoBot придёт автоматически.",
        reply_markup=kb_back("bal:back"),
    )
    admins = await _all_admin_ids()
    uname = f"@{message.from_user.username}" if message.from_user.username else "(нет username)"
    for adm in admins:
        try:
            await bot.send_message(
                adm,
                f"💸 <b>Запрос на вывод</b>\n"
                f"👤 User: <code>{message.from_user.id}</code> {uname}\n"
                f"💰 Сумма: <b>{fmt_money(amt)}</b>\n"
                f"📝 Заявка: #{req_id}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"wd:ok:{req_id}"),
                     InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wd:no:{req_id}")],
                ]),
            )
        except Exception:
            pass


async def _all_admin_ids() -> set[int]:
    s = set(ADMIN_IDS)
    extra = await db.cfg_get("admin_ids", "")
    s.update(int(x) for x in re.findall(r"\d+", extra))
    return s


@admin_router.callback_query(F.data.startswith("wd:"))
async def withdraw_admin_decide(call: CallbackQuery) -> None:
    if not call.from_user or not await is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    parts = call.data.split(":")
    if len(parts) < 3:
        await call.answer(); return
    action = parts[1]
    req_id = int(parts[2])
    async with db.conn() as c:
        cur = await c.execute("SELECT * FROM transactions WHERE id=?", (req_id,))
        row = await cur.fetchone()
    if not row:
        await call.answer("Заявка не найдена", show_alert=True); return
    tx = dict(row)
    if tx.get("kind") != "withdraw_request" or tx.get("comment") != "pending":
        await call.answer("Заявка уже обработана", show_alert=True); return
    user_id = int(tx["user_id"])
    amt = float(tx["amount"])
    if action == "no":
        await db.add_balance(user_id, amt, "refund", f"withdraw #{req_id} declined")
        async with db.conn() as c:
            await c.execute("UPDATE transactions SET comment='declined' WHERE id=?", (req_id,))
            await c.commit()
        try:
            await bot.send_message(user_id,
                f"❌ Ваша заявка на вывод #{req_id} ({fmt_money(amt)}) отклонена.\n"
                f"Средства возвращены на баланс.")
        except Exception: pass
        if call.message:
            await call.message.edit_text(call.message.text + f"\n\n❌ <b>Отклонено</b>")
        await call.answer("Отклонено")
        return
    rate = await get_usdt_rub_rate()
    usdt = round(amt / rate, 2)
    if usdt <= 0:
        await call.answer("Сумма слишком мала для чека", show_alert=True); return
    res = await cryptobot_request("createCheck", {
        "asset": "USDT",
        "amount": str(usdt),
        "description": f"TopLeads withdraw #{req_id}",
    })
    if not res:
        await call.answer("CryptoBot не отвечает или нет средств в Crypto Pay", show_alert=True); return
    check_url = res.get("bot_check_url") or res.get("url") or ""
    check_id = str(res.get("check_id") or res.get("id") or "")
    async with db.conn() as c:
        await c.execute("UPDATE transactions SET comment=? WHERE id=?",
                        (f"approved:{check_id}", req_id))
        await c.commit()
    try:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💰 Получить", url=check_url)]
        ]) if check_url else None
        await bot.send_message(
            user_id,
            f"✅ Заявка на вывод #{req_id} одобрена.\n"
            f"💰 Сумма: <b>{fmt_money(amt)}</b> ({usdt} USDT)\n\n"
            f"Нажмите «Получить» чтобы активировать чек в @CryptoBot.",
            reply_markup=kb,
        )
    except Exception as e:
        log.warning("notify user fail: %s", e)
    if call.message:
        await call.message.edit_text(call.message.text + f"\n\n✅ <b>Одобрено</b>, чек создан.")
    await call.answer("Одобрено")


@router.message(Command("withdraw"))
async def cmd_withdraw(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    await state.set_state(WithdrawFlow.waiting_amount)
    minw = await get_min_withdraw()
    await message.answer(
        f"💸 Введите сумму вывода в ₽ (минимум {fmt_money(minw)}):"
    )


@admin_router.callback_query(F.data.startswith("adm:"))
async def admin_root(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not await is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    action = call.data.split(":", 1)[1]
    
    if action == "stats":
        s = await db.stats()
        
        # Активные рекламные кампании
        async with db.conn() as c:
            cur = await c.execute("SELECT COUNT(*) FROM placements WHERE status='live'")
            active_placements = (await cur.fetchone())[0]
            
            cur = await c.execute("SELECT IFNULL(SUM(payout), 0) FROM placements WHERE status='live'")
            placements_sum = (await cur.fetchone())[0]
        
        text = (
            "📊 <b>Статистика</b>\n\n"
            f"👤 Пользователей: {s['users']} (за сутки +{s['users_today']})\n"
            f"📂 Активных ресурсов: {s['resources']}\n"
            f"📋 Заказов активных: {s['orders_active']} / всего: {s['orders_total']}\n"
            f"📣 Рекламных кампаний: <b>{active_placements}</b> на <b>{fmt_money(placements_sum)}</b>\n"
            f"💰 Баланс на руках: {fmt_money(s['balance_total'])}\n"
            f"💸 Всего потрачено: {fmt_money(s['spent_total'])}"
        )
        if call.message:
            await call.message.edit_text(text, reply_markup=kb_back("menu:admin"))
        await call.answer()
        return
    
    elif action == "analytics":
        # Меню аналитики
        rows = [
            [("🌐 Языки пользователей", "adm:lang_stats")],
            [("📊 Активность за 7 дней", "adm:week_stats")],
            [("📂 Аудит ресурса", "adm:audit_resource")],
            [("◀️ Назад", "menu:admin")],
        ]
        if call.message:
            await call.message.edit_text(
                "📈 <b>Аналитика</b>\n\n"
                "Выберите отчёт:",
                reply_markup=ikb(rows)
            )
        await call.answer()
        return
    
    elif action == "lang_stats":
        # Языки пользователей
        async with db.conn() as c:
            cur = await c.execute("SELECT language_code, COUNT(*) as cnt FROM users GROUP BY language_code ORDER BY cnt DESC")
            langs = {r["language_code"] or "unknown": r["cnt"] for r in await cur.fetchall()}
            total = sum(langs.values())
        
        lang_names = {
            "ru": "🇷🇺 Русский",
            "en": "🇬🇧 English",
            "uk": "🇺🇦 Українська",
            "fa": "FA",
            "ar": "AR",
            "uz": "🇺🇿 Oʻzbekcha",
            "es": "🇪🇸 Español",
            "id": "🇮🇩 Bahasa Indonesia",
            "be": "🇧🇾 Беларуская",
            "bn": "BN",
            "de": "🇩🇪 Deutsch",
            "fi": "FI",
            "unknown": "🌍 Неизвестно",
        }
        now = datetime.now().strftime("%H:%M:%S") 
        
        text = f"🌐 <b>Языки пользователей</b>\n\n"
        text += f"👥 Всего пользователей: <b>{total}</b>\n\n"
        
        sorted_langs = sorted(langs.items(), key=lambda x: x[1], reverse=True)
        shown = 0
        others = 0
        for lang, cnt in sorted_langs:
            if shown < 12:
                name = lang_names.get(lang, f"🌍 {lang}")
                percent = round(cnt / max(1, total) * 100, 1)
                text += f"{name} — <b>{cnt}</b> ({percent}%)\n"
                shown += 1
            else:
                others += cnt
        
        if others > 0:
            text += f"Прочие — <b>{others}</b> ({round(others / max(1, total) * 100, 1)}%)\n"
        text += f"\n🕐 Обновлено: {now}"
        
        if call.message:
            await call.message.edit_text(
                text,
                reply_markup=ikb([[("🔄 Обновить", "adm:lang_stats")], [("◀️ Назад", "adm:analytics")]])
            )
        await call.answer()
        return
    
    elif action == "week_stats":
        # Активность за 7 дней
        now = int(time.time())
        week_ago = now - 7 * 86400
        
        async with db.conn() as c:
            cur = await c.execute("SELECT COUNT(*) FROM users WHERE created_at >= ?", (week_ago,))
            new_users = (await cur.fetchone())[0]
            
            cur = await c.execute("SELECT COUNT(*) FROM completions WHERE created_at >= ?", (week_ago,))
            completions = (await cur.fetchone())[0]
            
            cur = await c.execute("SELECT COUNT(*) FROM orders WHERE created_at >= ?", (week_ago,))
            orders = (await cur.fetchone())[0]
            
            cur = await c.execute("SELECT IFNULL(SUM(payout), 0) FROM completions WHERE created_at >= ?", (week_ago,))
            total_earned = (await cur.fetchone())[0]
            
            cur = await c.execute("SELECT IFNULL(SUM(amount), 0) FROM transactions WHERE kind='deposit' AND created_at >= ?", (week_ago,))
            deposits = (await cur.fetchone())[0]
            
        update_time = datetime.now().strftime("%H:%M:%S")
        
        text = (
            "📊 <b>Активность за 7 дней</b>\n\n"
            f"👤 Новых пользователей: <b>{new_users}</b>\n"
            f"✅ Выполнено заданий: <b>{completions}</b>\n"
            f"📋 Создано заказов: <b>{orders}</b>\n"
            f"💎 Заработано пользователями: <b>{fmt_money(total_earned)}</b>\n"
            f"💰 Пополнений: <b>{fmt_money(deposits)}</b>\n\n"
            f"📅 С {datetime.fromtimestamp(week_ago).strftime('%d.%m.%Y')} по {datetime.fromtimestamp(now).strftime('%d.%m.%Y')}"
            f"🕐 Обновлено: {update_time}" 
        )
        
        if call.message:
            await call.message.edit_text(
                text,
                reply_markup=ikb([[("🔄 Обновить", "adm:week_stats")], [("◀️ Назад", "adm:analytics")]])
            )
        await call.answer()
        return
    
    elif action == "audit_resource":
        await state.set_state(AdminAction.give_user)
        await state.update_data(audit_mode=True)
        
        if call.message:
            await call.message.edit_text(
                "📂 <b>Аудит ресурса</b>\n\n"
                "Отправьте ID ресурса или @username для проверки.\n\n"
                "Будет показана детальная статистика.",
                reply_markup=kb_back("adm:analytics")
            )
        await call.answer()
        return
    
    elif action == "audit_user":
        await state.set_state(AdminAction.give_user)
        await state.update_data(audit_user=True)
        if call.message:
            await call.message.edit_text(
                "👤 <b>Аудит пользователя</b>\n\n"
                "Отправьте Telegram ID пользователя для проверки.",
                reply_markup=kb_back("menu:admin")
            )
        await call.answer()
        return
    
    elif action == "give":
        await state.set_state(AdminAction.give_user)
        if call.message:
            await call.message.edit_text("Кому начислить? Пришлите tg_id пользователя:",
                                          reply_markup=kb_back("menu:admin"))
        await call.answer()
        return
    
    elif action == "settoken":
        await state.set_state(AdminAction.set_token)
        if call.message:
            await call.message.edit_text(
                "Пришлите токен CryptoBot (@CryptoBot → Crypto Pay → Create App).",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "set_botohub_token":
        await state.set_state(AdminAction.set_token)
        await state.update_data(setting="botohub_token")
        if call.message:
            await call.message.edit_text(
                "Пришлите API токен Botohub.",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "set_subgram_secret":
        await state.set_state(AdminAction.set_token)
        await state.update_data(setting="subgram_secret")
        if call.message:
            await call.message.edit_text(
                "Пришлите Secret Key SubGram (из личного кабинета).",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "set_tgrass_token":
        await state.set_state(AdminAction.set_token)
        await state.update_data(setting="tgrass_token")
        if call.message:
            await call.message.edit_text(
                "Пришлите API токен Tgrass (@tgrassbot → API).",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "broadcast":
        await state.set_state(AdminAction.broadcast_text)
        if call.message:
            await call.message.edit_text(
                "Пришлите текст рассылки (HTML разрешён).",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "allres":
        await _admin_resources_page(call, 0)
        await call.answer()
        return
    
    elif action == "res_limit":
        cur = await db.cfg_get("max_resources", "3")
        await state.set_state(AdminAction.set_price)
        await state.update_data(price_key="__res_limit__")
        if call.message:
            await call.message.edit_text(
                f"🔢 <b>Лимит ресурсов на пользователя</b>\n\n"
                f"Сейчас: <b>{cur}</b>\n\n"
                f"Пришлите новое число (1-100).",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "bot_orders":
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT * FROM orders WHERE kind='bot_start' AND status='active' ORDER BY id DESC"
            )
            orders = [dict(r) for r in await cur.fetchall()]
        
        if not orders:
            if call.message:
                await call.message.edit_text("🤖 Нет активных заказов на ботов.", reply_markup=kb_back("menu:admin"))
            await call.answer()
            return
        
        text = "🤖 <b>Заказы на ботов</b>\n\n"
        btn_rows = []
        for o in orders:
            text += (
                f"📋 <b>Заказ #{o['id']}</b>\n"
                f"├ Ссылка: {o.get('target_link', '-')}\n"
                f"├ Количество: {o['completed']}/{o['quantity']}\n"
                f"├ Потрачено: {fmt_money(o['price_total'])}\n"
                f"└ Создан: {datetime.fromtimestamp(o['created_at'], tz=timezone.utc).strftime('%d.%m.%Y %H:%M')}\n\n"
            )
            btn_rows.append([(f"✅ Выполнить #{o['id']}", f"adm:complete_bot:{o['id']}")])
        
        btn_rows.append([("◀️ Назад", "menu:admin")])
        if call.message:
            await call.message.edit_text(text, reply_markup=ikb(btn_rows))
        await call.answer()
        return
    
    elif action.startswith("complete_bot:"):
        oid = int(action.split(":")[-1])
        async with db.conn() as c:
            # Сразу выполняем весь заказ
            cur = await c.execute("SELECT * FROM orders WHERE id=?", (oid,))
            row = await cur.fetchone()
            
            if row:
                o = dict(row)
                await c.execute("UPDATE orders SET status='done', completed=quantity WHERE id=?", (oid,))
                await c.commit()
                
                # Уведомляем заказчика
                try:
                    await bot.send_message(
                        o["user_id"],
                        f"🎉 <b>Ваш заказ на бота выполнен!</b>\n\n"
                        f"📋 Заказ #{oid}\n"
                        f"🔗 Ссылка: {o.get('target_link', '-')}\n"
                        f"📊 Выполнено: {o['quantity']}/{o['quantity']}\n"
                        f"💰 Потрачено: {fmt_money(o['price_total'])}"
                    )
                except Exception:
                    pass
        
        await call.answer("✅ Заказ выполнен! Заказчик уведомлён.", show_alert=True)
        
        # Обновляем список заказов
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT * FROM orders WHERE kind='bot_start' AND status='active' ORDER BY id DESC"
            )
            orders = [dict(r) for r in await cur.fetchall()]
        
        if not orders:
            await call.message.edit_text("🤖 Нет активных заказов на ботов.", reply_markup=kb_back("menu:admin"))
            return
        
        text = "🤖 <b>Заказы на ботов</b>\n\n"
        btn_rows = []
        for o in orders:
            text += (
                f"📋 <b>Заказ #{o['id']}</b>\n"
                f"├ Ссылка: {o.get('target_link', '-')}\n"
                f"├ Количество: {o['completed']}/{o['quantity']}\n"
                f"├ Потрачено: {fmt_money(o['price_total'])}\n"
                f"└ Создан: {datetime.fromtimestamp(o['created_at'], tz=timezone.utc).strftime('%d.%m.%Y %H:%M')}\n\n"
            )
            btn_rows.append([(f"✅ Выполнить #{o['id']}", f"adm:complete_bot:{o['id']}")])
        
        btn_rows.append([("◀️ Назад", "menu:admin")])
        await call.message.edit_text(text, reply_markup=ikb(btn_rows))
        return
    
    elif action == "refp":
        cur = await get_ref_percent()
        await state.set_state(AdminAction.set_price)
        await state.update_data(price_key="__refp__")
        if call.message:
            await call.message.edit_text(
                f"👥 <b>Процент реф. системы</b>\n\n"
                f"Сейчас: <b>{cur}%</b>\n\n"
                f"Пришлите новое целое число (0-100).",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "mindep":
        cur = await get_min_deposit()
        await state.set_state(AdminAction.set_price)
        await state.update_data(price_key="__mindep__")
        if call.message:
            await call.message.edit_text(
                f"⬇ <b>Мин. сумма пополнения</b>\n\nСейчас: <b>{fmt_money(cur)}</b>\n\nПришлите новое число (₽).",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "minwd":
        cur = await get_min_withdraw()
        await state.set_state(AdminAction.set_price)
        await state.update_data(price_key="__minwd__")
        if call.message:
            await call.message.edit_text(
                f"⬆ <b>Мин. сумма вывода</b>\n\nСейчас: <b>{fmt_money(cur)}</b>\n\nПришлите новое число (₽).",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action in ("minord_ch", "minord_chat", "minord_view", "minord_bot"):
        kind = {"minord_ch": "channel_sub", "minord_chat": "chat_join", "minord_view": "post_view", "minord_bot": "bot_start"}[action]
        labels = {
            "channel_sub": ("📢 Мин. заказ канал", "подписчиков"),
            "chat_join":   ("💬 Мин. заказ чат", "вступивших"),
            "post_view":   ("👁 Мин. кол-во просмотров", "просмотров"),
            "bot_start":   ("🤖 Мин. заказ бот", "запусков"),
        }
        title, what = labels[kind]
        cur = await get_min_order_qty(kind)
        await state.set_state(AdminAction.set_price)
        await state.update_data(price_key=f"__minord_{kind}__")
        if call.message:
            await call.message.edit_text(
                f"<b>{title}</b>\n\nСейчас минимум: <b>{cur}</b> {what}.\n\n"
                f"Пришлите новое целое число (1+).",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "vchan":
        vid = await db.cfg_get("view_channel_id", "")
        vtitle = await db.cfg_get("view_channel_title", "")
        vlink = await db.cfg_get("view_channel_link", "")
        if vid:
            cur_info = f"Сейчас: <b>{vtitle or vid}</b>\n{vlink or ''}"
        else:
            cur_info = "Канал ещё не установлен."
        await state.set_state(AdminAction.view_channel_link)
        if call.message:
            await call.message.edit_text(
                f"🎯 <b>Канал для просмотров</b>\n\n{cur_info}\n\n"
                f"Пришлите ссылку на канал (@username или https://t.me/username).\n"
                f"⚠️ Канал должен быть <b>публичным</b>, а бот — <b>администратором</b> в нём.",
                reply_markup=kb_back("menu:admin"),
            )
        await call.answer()
        return
    
    elif action == "startop":
        ops = await db.list_start_op()
        text = "➕ <b>ОП на /start</b>\n\n"
        if ops:
            for op in ops:
                text += f"• #{op['id']} {op['title'] or op['username']}  /delop_{op['id']}\n"
        else:
            text += "Список пуст."
        text += "\n\nДобавить новый - /addop &lt;ссылка&gt;\nИли пришлите ссылку прямо сейчас."
        await state.set_state(AdminAction.startop_link)
        if call.message:
            await call.message.edit_text(text, reply_markup=kb_back("menu:admin"))
        await call.answer()
        return
    
    elif action == "reserve":
        balance = await get_cryptobot_balance()
        now = datetime.now().strftime("%H:%M:%S")
        
        if balance is None:
            text = (
                "💰 <b>Резерв системы</b>\n\n"
                "❌ Не удалось получить баланс CryptoBot.\n"
                "Проверьте токен в настройках.\n\n"
                f"🕐 Обновлено: {now}"
            )
        elif not balance:
            text = (
                "💰 <b>Резерв системы</b>\n\n"
                "💎 <b>CryptoBot:</b> баланс пуст\n"
                "Возможно, средства ещё не зачислены.\n\n"
                f"🕐 Обновлено: {now}"
            )
        else:
            lines = []
            for b in balance:
                asset = b.get("asset", "?")
                available = float(b.get("available", 0))
                lines.append(f"• {asset}: <b>{available:.2f}</b>")
            
            stats = await db.stats()
            text = (
                "💰 <b>Резерв системы</b>\n\n"
                "<b>Баланс CryptoBot:</b>\n" +
                "\n".join(lines) + "\n\n"
                f"👥 <b>Баланс пользователей:</b> {fmt_money(stats['balance_total'])}\n"
                f"💸 <b>Всего потрачено:</b> {fmt_money(stats['spent_total'])}\n\n"
                f"🕐 Обновлено: {now}"
            )
        
        rows = [
            [("💳 Пополнить резерв", "adm:reserve_add")],
            [("🔄 Обновить", "adm:reserve")],
            [("◀️ Назад", "menu:admin")],
        ]
        if call.message:
            await call.message.edit_text(text, reply_markup=ikb(rows))
        await call.answer()
        return
    
    elif action == "reserve_add":
        await state.set_state(AdminAction.set_price)
        await state.update_data(price_key="__reserve_add__")
        if call.message:
            await call.message.edit_text(
                "💳 <b>Пополнение резерва</b>\n\n"
                "Введите сумму в USDT для пополнения резерва.\n\n"
                "Будет создан счёт в CryptoBot для оплаты.",
                reply_markup=kb_back("adm:reserve")
            )
        await call.answer()
        return
    
    elif action == "scan":
        if call.message:
            await call.message.edit_text(
                "🔍 <b>Сканирование ресурсов</b>\n\n"
                "Бот проверит все активные ресурсы и выключит невалидные.\n"
                "Нажмите «▶️ Запустить» для начала.",
                reply_markup=ikb([
                    [("▶️ Запустить сканирование", "adm:scan_start")],
                    [("◀️ Назад", "menu:admin")],
                ])
            )
        await call.answer()
        return
    
    elif action == "scan_start":
        await call.answer("🔍 Сканирование запущено...", show_alert=False)
        
        # Сканируем ресурсы
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT * FROM resources WHERE is_active=1 ORDER BY id"
            )
            resources = [dict(r) for r in await cur.fetchall()]
        
        invalid_resources = 0
        valid_resources = 0
        invalid_orders = 0
        valid_orders = 0
        report_lines = []
        
        # Проверяем ресурсы
        for r in resources:
            chat_id = r.get("tg_chat_id")
            username = r.get("username")
            name = r.get("title") or f"@{username}" or f"id={chat_id}"
            type_icon = {"channel": "📢", "chat": "💬", "bot": "🤖"}.get(r["type"], "❔")
            
            is_valid = False
            
            if chat_id:
                try:
                    await bot.get_chat(chat_id)
                    is_valid = True
                except (TelegramBadRequest, TelegramForbiddenError):
                    pass
            elif username:
                try:
                    chat = await bot.get_chat(f"@{username}")
                    is_valid = True
                    if not chat_id:
                        async with db.conn() as c:
                            await c.execute(
                                "UPDATE resources SET tg_chat_id=? WHERE id=?",
                                (chat.id, r["id"])
                            )
                            await c.commit()
                except (TelegramBadRequest, TelegramForbiddenError):
                    pass
            
            if is_valid:
                valid_resources += 1
            else:
                invalid_resources += 1
                report_lines.append(f"🔴 Ресурс: {type_icon} {name} — недоступен")
                async with db.conn() as c:
                    await c.execute("UPDATE resources SET is_active=0 WHERE id=?", (r["id"],))
                    await c.commit()
                
                try:
                    await bot.send_message(
                        r["user_id"],
                        f"⚠️ Ваш ресурс <b>{name}</b> отключён — недоступен."
                    )
                except Exception:
                    pass
            
            await asyncio.sleep(0.2)
        
        # Проверяем заказы (только channel_sub, chat_join, post_view, bot_start)
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT * FROM orders WHERE status='active' "
                "AND kind IN ('channel_sub', 'chat_join', 'bot_start', 'post_view')"
            )
            orders = [dict(r) for r in await cur.fetchall()]
        
        for o in orders:
            target_link = o.get("target_link", "")
            target_chat_id = o.get("target_chat_id")
            kind = o["kind"]
            
            is_valid = False
            
            if kind in ("channel_sub", "chat_join", "post_view"):
                if target_chat_id:
                    try:
                        await bot.get_chat(target_chat_id)
                        is_valid = True
                    except (TelegramBadRequest, TelegramForbiddenError):
                        pass
                elif target_link:
                    # Пробуем получить username из ссылки
                    uname = target_link.replace("https://t.me/", "").replace("@", "").split("/")[0]
                    if uname and not uname.startswith("+"):
                        try:
                            chat = await bot.get_chat(f"@{uname}")
                            is_valid = True
                            async with db.conn() as c:
                                await c.execute(
                                    "UPDATE orders SET target_chat_id=? WHERE id=?",
                                    (chat.id, o["id"])
                                )
                                await c.commit()
                        except (TelegramBadRequest, TelegramForbiddenError):
                            pass
                    elif "/+" in target_link:
                        # Инвайт-ссылки пропускаем
                        is_valid = True
            
            elif kind == "bot_start":
                # Ботов проверяем по username
                uname = target_link.replace("https://t.me/", "").replace("@", "").split("?")[0]
                if uname and not uname.startswith("+"):
                    try:
                        await bot.get_chat(f"@{uname}")
                        is_valid = True
                    except (TelegramBadRequest, TelegramForbiddenError):
                        pass
                else:
                    is_valid = True  # Инвайты пропускаем
            
            if is_valid:
                valid_orders += 1
            else:
                invalid_orders += 1
                report_lines.append(f"🟡 Заказ #{o['id']} ({kind}): {target_link[:30]}... — цель недоступна")
                async with db.conn() as c:
                    await c.execute("UPDATE orders SET status='cancelled' WHERE id=?", (o["id"],))
                    await c.commit()
                
                    try:
                        await bot.send_message(
                            o["user_id"],
                            f"⚠️ Ваш заказ #{o['id']} на {kind} отменён.\n"
                            f"Причина: целевой ресурс больше не существует.\n"
                            f"Средства сгорели."
                        )
                    except Exception:
                        pass

            await asyncio.sleep(0.2)
        
        # Формируем отчёт
        total_invalid = invalid_resources + invalid_orders
        report_text = (
            f"🔍 <b>Сканирование завершено</b>\n\n"
            f"📂 <b>Ресурсы:</b>\n"
            f"├ Доступно: <b>{valid_resources}</b>\n"
            f"└ Отключено: <b>{invalid_resources}</b>\n\n"
            f"📋 <b>Заказы:</b>\n"
            f"├ Активны: <b>{valid_orders}</b>\n"
            f"└ Отменены: <b>{invalid_orders}</b>\n\n"
            f"📊 <b>Всего проблем:</b> {total_invalid}"
        )
        
        if total_invalid > 0:
            report_text += "\n\n<b>Детали:</b>\n"
            for line in report_lines[:20]:  # Максимум 20 строк
                report_text += f"{line}\n"
        
        if call.message:
            await call.message.edit_text(
                report_text,
                reply_markup=ikb([
                    [("🔄 Повторить", "adm:scan_start")],
                    [("◀️ Назад", "menu:admin")],
                ])
            )
        await call.answer()
        return
    
    elif action == "prices":
        traffic_kinds = [
            ("channel_sub", "Подписка на канал (1 шт.)"),
            ("chat_join",   "Вступление в чат (1 шт.)"),
            ("bot_start",   "Запуск бота (1 старт)"),
            ("view_post",   "Просмотр поста (1 клик)"),
            ("auto_views",  "Автопросмотры (1 шт.)"),
        ]
        text = "💵 <b>Цены</b> (₽)\n\n"
        rows = []
        text += "🛒 <b>Покупателю трафика</b> (с него списывается):\n"
        for k, label in traffic_kinds:
            v = await get_buy_price(k)
            text += f"• {label}: <b>{fmt_money(v)}</b>\n"
            rows.append([(f"✏️ Покуп. {label}", f"price:buy_{k}")])
        text += "\n💸 <b>Продавцу трафика</b> (выплата исполнителю):\n"
        for k, label in traffic_kinds:
            v = await get_sell_price(k)
            text += f"• {label}: <b>{fmt_money(v)}</b>\n"
            rows.append([(f"✏️ Прод. {label}", f"price:sell_{k}")])
        commission = await get_ads_commission()
        text += "\n📣 <b>Реклама в канале</b>:\n"
        text += f"• Комиссия владельца бота: <b>{commission:g}%</b>\n"
        rows.append([("✏️ Комиссия с рекламы (%)", "price:ads_commission_percent")])
        for k, label in [
            ("owner_post_per_hour", "Выплата владельцу канала за 1ч поста (мин.)"),
            ("owner_chat_per_hour", "Выплата владельцу чата за 1ч ОП"),
        ]:
            v = await db.get_price(k)
            text += f"• {label}: <b>{fmt_money(v)}</b>\n"
            rows.append([(f"✏️ {label}", f"price:{k}")])
        rows.append([("◀️ Назад", "menu:admin")])
        if call.message:
            await call.message.edit_text(text, reply_markup=ikb(rows))
    
    await call.answer()


@admin_router.callback_query(F.data.startswith("price:"))
async def admin_price_set_start(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not await is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    key = call.data.split(":", 1)[1]
    if key == "ads_commission_percent":
        cur_val = await get_ads_commission()
        prompt = (
            f"Текущая комиссия с рекламы: <b>{cur_val:g}%</b>\n\n"
            f"Пришлите новое число (0–100)."
        )
    elif key.startswith("buy_"):
        cur_val = await get_buy_price(key[4:])
        prompt = f"Цена покупателю — текущая: <b>{fmt_money(cur_val)}</b>\n\nПришлите новое число (₽)."
    elif key.startswith("sell_"):
        cur_val = await get_sell_price(key[5:])
        prompt = f"Выплата продавцу — текущая: <b>{fmt_money(cur_val)}</b>\n\nПришлите новое число (₽)."
    else:
        cur_val = await db.get_price(key)
        prompt = f"Текущее значение: <b>{fmt_money(cur_val)}</b>\n\nПришлите новое число (₽)."
    await state.set_state(AdminAction.set_price)
    await state.update_data(price_key=key)
    if call.message:
        await call.message.edit_text(prompt, reply_markup=kb_back("menu:admin"))
    await call.answer()


@admin_router.message(AdminAction.set_price)
async def admin_price_set(message: Message, state: FSMContext) -> None:
    if not message.text:
        return
    data = await state.get_data()
    key = data.get("price_key")
    
    if key == "__res_limit__":
        try:
            n = int(float(message.text.replace(",", ".").strip()))
            if n < 1 or n > 100:
                raise ValueError
        except ValueError:
            await message.answer("❌ Введите целое число от 1 до 100.")
            return
        await db.cfg_set("max_resources", str(n))
        await state.clear()
        await message.answer(f"✅ Лимит ресурсов обновлён: <b>{n}</b>")
        return
    
    if key == "__reserve_add__":
        try:
            amount = float(message.text.replace(",", ".").strip())
            if amount <= 0:
                raise ValueError
        except ValueError:
            await message.answer("❌ Введите положительное число.")
            return
        
        # Создаём инвойс для пополнения резерва
        invoice = await cryptobot_request("createInvoice", {
            "asset": "USDT",
            "amount": str(amount),
            "description": "Top-up reserve AutoOP Bot",
            "payload": "reserve_topup",
            "expires_in": 3600,
        })
        
        if not invoice:
            await message.answer(
                "❌ Не удалось создать счёт. Проверьте токен CryptoBot.",
                reply_markup=kb_back("menu:admin")
            )
            await state.clear()
            return
        
        pay_url = invoice.get("pay_url") or invoice.get("bot_invoice_url") or ""
        inv_id = str(invoice.get("invoice_id") or invoice.get("id") or "")
        
        if not pay_url:
            await message.answer("❌ Ошибка создания счёта.", reply_markup=kb_back("menu:admin"))
            await state.clear()
            return
        
        await state.clear()
        await message.answer(
            f"💳 <b>Счёт на пополнение резерва</b>\n\n"
            f"💰 Сумма: <b>{amount} USDT</b>\n"
            f"🆔 ID: <code>{inv_id}</code>\n\n"
            f"Нажмите кнопку ниже для оплаты:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить", url=pay_url)],
                [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"reserve_check:{inv_id}")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:admin")],
            ])
        )
        return
    
    if key == "__refp__":
        try:
            n = int(float(message.text.replace(",", ".").strip()))
            if not (0 <= n <= 100):
                raise ValueError
        except ValueError:
            await message.answer("❌ Введите целое число от 0 до 100.")
            return
        await db.cfg_set("ref_percent", str(n))
        await state.clear()
        await message.answer(f"✅ Реф. процент обновлён: <b>{n}%</b>")
        return
    
    if key in ("__mindep__", "__minwd__"):
        try:
            v = float(message.text.replace(",", ".").strip())
            if v < 0:
                raise ValueError
        except ValueError:
            await message.answer("❌ Введите положительное число.")
            return
        cfg_key = "min_deposit" if key == "__mindep__" else "min_withdraw"
        await db.cfg_set(cfg_key, str(v))
        await state.clear()
        label = "пополнения" if cfg_key == "min_deposit" else "вывода"
        await message.answer(f"✅ Мин. сумма {label} обновлена: <b>{fmt_money(v)}</b>")
        return
    
    if key.startswith("__minord_") and key.endswith("__"):
        kind = key[len("__minord_"):-2]
        try:
            n = int(float(message.text.replace(",", ".").strip()))
            if n < 1:
                raise ValueError
        except ValueError:
            await message.answer("❌ Введите целое число (1+).")
            return
        cfg_key = {
            "channel_sub": "min_order_channel",
            "chat_join":   "min_order_chat",
            "post_view":   "min_order_view",
            "bot_start":   "min_order_bot",
        }.get(kind)
        if not cfg_key:
            await state.clear(); return
        await db.cfg_set(cfg_key, str(n))
        await state.clear()
        await message.answer(f"✅ Минимум для заказа обновлён: <b>{n}</b>")
        return
    
    if key == "ads_commission_percent":
        try:
            v = float(message.text.replace(",", ".").strip())
            if not (0 <= v <= 100):
                raise ValueError
        except ValueError:
            await message.answer("❌ Введите число от 0 до 100.")
            return
        await db.cfg_set("ads_commission_percent", str(v))
        await state.clear()
        await message.answer(f"✅ Комиссия с рекламы обновлена: <b>{v:g}%</b>")
        return
    
    try:
        v = float(message.text.replace(",", ".").strip())
        if v < 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число.")
        return
    
    if key.startswith("buy_"):
        await db.cfg_set(f"buy_price_{key[4:]}", str(v))
        await state.clear()
        await message.answer(f"✅ Цена покупателю «{key[4:]}» обновлена: {fmt_money(v)}")
        return
    
    if key.startswith("sell_"):
        await db.cfg_set(f"sell_price_{key[5:]}", str(v))
        await state.clear()
        await message.answer(f"✅ Выплата продавцу «{key[5:]}» обновлена: {fmt_money(v)}")
        return
    
    await db.cfg_set(f"price_{key}", str(v))
    await state.clear()
    await message.answer(f"✅ Цена «{key}» обновлена: {fmt_money(v)}")


@admin_router.message(AdminAction.give_user)
async def admin_give_user(message: Message, state: FSMContext) -> None:
    if not message.text:
        return
    
    data = await state.get_data()
    
    # Если это запрос аудита пользователя ← ВОТ СЮДА ПЕРВЫМ
    if data.get("audit_user"):
        uid = int(message.text.strip()) if message.text.strip().isdigit() else None
        if not uid:
            await message.answer("❌ Отправьте Telegram ID пользователя.")
            await state.clear()
            return
        
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT COUNT(*) FROM tgrass_logs WHERE user_id=? AND action='subscribed'", (uid,)
            )
            total_subs = (await cur.fetchone())[0]
            
            three_days_ago = int(time.time()) - 259200
            cur = await c.execute(
                "SELECT COUNT(*) FROM tgrass_logs WHERE user_id=? AND action='unsubscribed' AND created_at >= ?",
                (uid, three_days_ago)
            )
            early_unsubs = (await cur.fetchone())[0]
            
            cur = await c.execute(
                "SELECT * FROM tgrass_logs WHERE user_id=? ORDER BY created_at DESC LIMIT 10",
                (uid,)
            )
            logs = [dict(r) for r in await cur.fetchall()]
        
        text = f"👤 <b>Аудит пользователя #{uid}</b>\n\n"
        text += f"📊 <b>Tgrass статистика:</b>\n"
        text += f"├ Всего подписок: <b>{total_subs}</b>\n"
        text += f"├ Отписок раньше 3 дней: <b>{early_unsubs}</b>\n"
        text += f"└ Риск: {'🔴 Высокий' if early_unsubs > 2 else '🟢 Низкий'}\n\n"
        
        if logs:
            text += "<b>Последние действия:</b>\n"
            for log in logs:
                icon = "✅" if log["action"] == "subscribed" else "❌"
                ts = datetime.fromtimestamp(log["created_at"], tz=timezone.utc).strftime("%d.%m %H:%M")
                ch_name = log.get("channel_name") or "—"
                text += f"{icon} {ch_name[:20]} — {ts}\n"
        
        await state.clear()
        await message.answer(text, reply_markup=kb_back("menu:admin"))
        return
    
    # Если это запрос аудита
    if data.get("audit_mode"):
        text = message.text.strip()
        
        # Пробуем распознать ID или username
        if text.startswith("@"):
            async with db.conn() as c:
                cur = await c.execute("SELECT * FROM resources WHERE username=? AND is_active=1", (text[1:],))
                row = await cur.fetchone()
        elif text.isdigit():
            async with db.conn() as c:
                cur = await c.execute("SELECT * FROM resources WHERE id=?", (int(text),))
                row = await cur.fetchone()
        else:
            await message.answer("❌ Отправьте ID ресурса (число) или @username.")
            return
        
        if not row:
            await message.answer("❌ Ресурс не найден.")
            return
        
        r = dict(row)
        rid = r["id"]
        
        # Собираем статистику
        async with db.conn() as c:
            # Всего выполнений в системе
            cur = await c.execute("SELECT COUNT(*) FROM completions")
            completions = (await cur.fetchone())[0]
            
            # Сколько из них через этот чат
            cur = await c.execute(
                "SELECT COUNT(*) FROM completions WHERE comment IN (?, ?)",
                (f"chat:{r['tg_chat_id']}", f"tgrass:{r['tg_chat_id']}")
            )
            chat_completions = (await cur.fetchone())[0]
            
            # Размещений рекламы
            cur = await c.execute(
                "SELECT COUNT(*) FROM placements WHERE resource_id=?", (rid,)
            )
            placements = (await cur.fetchone())[0]
            
            # Доход владельца
            cur = await c.execute(
                "SELECT IFNULL(SUM(payout), 0) FROM placements WHERE resource_id=? AND paid=1", (rid,)
            )
            owner_income = (await cur.fetchone())[0]
            
            # Последняя активность
            cur = await c.execute("SELECT MAX(created_at) FROM completions")
            last_activity = (await cur.fetchone())[0]
            
            # Показов ОП (приблизительно)
            op_shows = chat_completions
            
            # Дней в системе
            created_at = r.get("created_at", 0)
            days_in_system = "неизвестно"
            if created_at:
                days = (int(time.time()) - int(created_at)) // 86400
                if days == 0:
                    days_in_system = "сегодня"
                elif days == 1:
                    days_in_system = "1 день"
                elif days < 31:
                    days_in_system = f"{days} дн."
                else:
                    months = days // 30
                    days_in_system = f"{months} мес."
            
            # Языки пользователей, выполнявших задания через этот чат
            cur = await c.execute(
                "SELECT u.language_code, COUNT(*) as cnt "
                "FROM completions c "
                "JOIN orders o ON o.id = c.order_id "
                "JOIN users u ON u.tg_id = c.user_id "
                "WHERE o.target_chat_id = ? "
                "GROUP BY u.language_code "
                "ORDER BY cnt DESC",
                (r["tg_chat_id"],)
            )
            user_langs = {row["language_code"] or "unknown": row["cnt"] for row in await cur.fetchall()}
            total_lang_users = sum(user_langs.values())
        
        type_icon = {"channel": "📢 Канал", "chat": "💬 Чат", "bot": "🤖 Бот"}.get(r["type"], r["type"])
        
        # Формируем название и ссылку
        title = r.get("title")
        username = r.get("username")
        tg_chat_id = r.get("tg_chat_id")
        
        if title and str(title).strip():
            name = str(title).strip()
        elif username and str(username).strip():
            name = f"@{str(username).strip()}"
        elif tg_chat_id:
            name = f"Чат #{tg_chat_id}"
        else:
            name = "Без названия"
        
        # Ссылка на чат
        if username and str(username).strip():
            chat_link = f"https://t.me/{str(username).strip()}"
        elif tg_chat_id:
            chat_link = f"tg://chat?id={tg_chat_id}"
        else:
            chat_link = None
        
        # Отображаем имя со ссылкой
        if chat_link:
            name_display = f"<a href='{chat_link}'>{name}</a>"
        else:
            name_display = name
        
        cat = CATEGORY_BY_KEY.get(r.get("category", "misc"), "?")
        status = "🟢 Активен" if r["is_active"] else "🔴 Выключен"
        
        last_act_str = "Никогда"
        if last_activity:
            last_act_str = datetime.fromtimestamp(last_activity, tz=timezone.utc).strftime("%d.%m.%Y %H:%M")
        
        # Формируем статистику по языкам
        lang_names = {
            "ru": "🇷🇺 Русский",
            "en": "🇬🇧 English",
            "uk": "🇺🇦 Українська",
            "fa": "FA",
            "ar": "AR",
            "uz": "🇺🇿 Oʻzbekcha",
            "es": "🇪🇸 Español",
            "id": "🇮🇩 Bahasa Indonesia",
            "be": "🇧🇾 Беларуская",
            "bn": "BN",
            "de": "🇩🇪 Deutsch",
            "fi": "FI",
            "unknown": "🌍 Неизвестно",
        }
        
        lang_text = ""
        if total_lang_users > 0:
            lang_text = f"\n🌐 <b>Языки пользователей ({total_lang_users}):</b>\n"
            sorted_langs = sorted(user_langs.items(), key=lambda x: x[1], reverse=True)
            shown = 0
            others = 0
            for lang, cnt in sorted_langs:
                if shown < 8:
                    lang_name = lang_names.get(lang, f"🌍 {lang}")
                    percent = round(cnt / max(1, total_lang_users) * 100, 1)
                    lang_text += f"├ {lang_name} — <b>{cnt}</b> ({percent}%)\n"
                    shown += 1
                else:
                    others += cnt
            if others > 0:
                lang_text += f"└ Прочие — <b>{others}</b> ({round(others / max(1, total_lang_users) * 100, 1)}%)\n"
        
        audit_text = (
            f"📂 <b>Аудит ресурса #{rid}</b>\n\n"
            f"{type_icon} {name_display}\n"
            f"Статус: {status}\n"
            f"Категория: {cat}\n"
            f"Владелец: <code>{r['user_id']}</code>\n"
            f"В системе: <b>{days_in_system}</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"├ Всего выполнений: <b>{completions}</b>\n"
            f"├ Через этот чат: <b>{chat_completions}</b>\n"
            f"├ Размещений рекламы: <b>{placements}</b>\n"
            f"├ Доход владельца: <b>{fmt_money(owner_income)}</b>\n"
            f"└ Последняя активность: {last_act_str}"
            f"{lang_text}\n"
            f"🕐 Аудит проведён: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )
        
        await state.clear()
        await message.answer(audit_text, reply_markup=kb_back("adm:analytics"))
        return
    
    # Обычная обработка выдачи баланса
    try:
        uid = int(message.text.strip())
    except ValueError:
        await message.answer("Это не tg_id.")
        return
    await state.update_data(target_user=uid)
    await state.set_state(AdminAction.give_amount)
    await message.answer("Сумма (можно отрицательную):")


@admin_router.message(AdminAction.give_amount)
async def admin_give_amt(message: Message, state: FSMContext) -> None:
    if not message.text:
        return
    try:
        amt = float(message.text.strip())
    except ValueError:
        await message.answer("Сумма не число.")
        return
    data = await state.get_data()
    target = int(data["target_user"])
    await db.add_balance(target, amt, "admin", f"by {message.from_user.id if message.from_user else '?'}")
    await state.clear()
    await message.answer(f"✅ Начислено {amt} пользователю {target}.")


@admin_router.message(AdminAction.set_token)
async def admin_set_token(message: Message, state: FSMContext) -> None:
    if not message.text:
        return
    data = await state.get_data()
    setting = data.get("setting", "cryptobot")
    
    token = message.text.strip()
    
    if setting == "tgrass_token":
        await db.cfg_set("tgrass_api_token", token)
        await state.clear()
        await message.answer("✅ Токен Tgrass сохранён.")
        return
    
    if setting == "flyer_token":
        await db.cfg_set("flyer_api_token", token)
        await state.clear()
        await message.answer("✅ Токен Flyer сохранён.")
        return
    
    if setting == "botohub_token":
        await db.cfg_set("botohub_api_token", token)
        await state.clear()
        await message.answer("✅ Токен Botohub сохранён.")
        return
    
    if setting == "subgram_secret":
        await db.cfg_set("subgram_api_token", token)
        await state.clear()
        await message.answer("✅ Secret Key SubGram сохранён.")
        return

    # По умолчанию — CryptoBot
    await db.cfg_set("cryptobot_token", token)
    await state.clear()
    await message.answer("✅ Токен сохранён.")


@admin_router.message(AdminAction.broadcast_text)
async def admin_broadcast(message: Message, state: FSMContext) -> None:
    if not message.text and not message.photo and not message.video:
        return
    
    await state.clear()
    
    # Парсим кнопки из текста
    buttons = []
    caption = message.caption or message.text or ""
    
    if caption:
        lines = caption.split("\n")
        clean_lines = []
        for line in lines:
            if line.startswith("#") and "#" in line[1:] and ("http://" in line or "https://" in line or "@" in line):
                # Формат: #Текст кнопки#ссылка
                parts = line.split("#")
                if len(parts) >= 3:
                    btn_text = parts[1].strip()
                    btn_url = parts[2].strip()
                    if btn_url.startswith("@"):
                        btn_url = f"https://t.me/{btn_url[1:]}"
                    buttons.append((btn_text, btn_url))
            else:
                clean_lines.append(line)
        caption = "\n".join(clean_lines)
    
    kb = None
    if buttons:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=text, url=url)] for text, url in buttons
        ])
    
    sent = failed = 0
    async with db.conn() as c:
        cur = await c.execute("SELECT tg_id FROM users WHERE is_banned=0")
        ids = [r["tg_id"] for r in await cur.fetchall()]
    
    await message.answer(f"Запускаю рассылку на {len(ids)} получателей...")
    
    for tg_id in ids:
        try:
            if message.photo:
                await bot.send_photo(
                    chat_id=tg_id,
                    photo=message.photo[-1].file_id,
                    caption=caption or ".",
                    reply_markup=kb
                )
            elif message.video:
                await bot.send_video(
                    chat_id=tg_id,
                    video=message.video.file_id,
                    caption=caption or ".",
                    reply_markup=kb
                )
            else:
                await bot.send_message(
                    chat_id=tg_id,
                    text=caption,
                    reply_markup=kb
                )
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    
    await message.answer(f"✅ Готово: {sent}, ошибок: {failed}.")


@admin_router.message(AdminAction.startop_link)
async def admin_startop_add(message: Message, state: FSMContext) -> None:
    if not message.text:
        return
    parsed = parse_link(message.text)
    if not parsed.get("username"):
        await message.answer("Не похоже на ссылку. Пример: https://t.me/topleadss")
        return
    try:
        chat = await bot.get_chat(f"@{parsed['username']}")
    except (TelegramBadRequest, TelegramForbiddenError):
        chat = None
    chat_id = chat.id if chat else None
    title = chat.title if chat else parsed["username"]
    await db.add_start_op(chat_id, parsed["username"], title, f"https://t.me/{parsed['username']}")
    await state.clear()
    await message.answer(f"✅ {title} добавлен в ОП на /start.")


@admin_router.message(AdminAction.view_channel_link)
async def admin_view_channel_set(message: Message, state: FSMContext) -> None:
    if not message.from_user or not await is_admin(message.from_user.id) or not message.text:
        return
    parsed = parse_link(message.text)
    uname = parsed.get("username")
    if not uname:
        await message.answer(
            "❌ Не распознал ссылку. Пример: @my_channel или https://t.me/my_channel"
        )
        return
    try:
        chat = await bot.get_chat(f"@{uname}")
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        await message.answer(
            f"❌ Не могу открыть канал @{uname}: {e}\n"
            f"Убедитесь, что канал публичный и бот туда добавлен."
        )
        return
    try:
        me = await bot.get_me()
        mem = await bot.get_chat_member(chat.id, me.id)
        if mem.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
            await message.answer(
                "❌ Бот должен быть <b>администратором</b> в этом канале с правом публикации.\n"
                "Добавьте бота админом и пришлите ссылку ещё раз."
            )
            return
    except Exception as e:
        await message.answer(f"❌ Не удалось проверить права бота: {e}")
        return
    await db.cfg_set("view_channel_id", str(chat.id))
    await db.cfg_set("view_channel_username", uname)
    await db.cfg_set("view_channel_title", chat.title or uname)
    await db.cfg_set("view_channel_link", f"https://t.me/{uname}")
    await state.clear()
    await message.answer(
        f"✅ Канал для просмотров установлен:\n<b>{chat.title or uname}</b>\n"
        f"https://t.me/{uname}"
    )


@router.message(Command("claim_admin"))
async def claim_admin(message: Message) -> None:
    """Первый пользователь, вызвавший /claim_admin при пустом списке админов, становится админом."""
    if not message.from_user:
        return
    cur_admins = await db.cfg_get("admin_ids", "")
    has_env = bool(ADMIN_IDS)
    has_cfg = bool(re.findall(r"\d+", cur_admins))
    if has_env or has_cfg:
        await message.answer("❌ Админ уже назначен.")
        return
    await db.cfg_set("admin_ids", str(message.from_user.id))
    await message.answer(
        f"✅ Вы назначены администратором (tg_id={message.from_user.id}).\n"
        f"Теперь у вас в главном меню доступна кнопка <b>⚙️ Админ-панель</b>.",
        reply_markup=kb_main(True),
    )


@admin_router.message(F.text.regexp(r"^/delop_\d+$"))
async def admin_del_op(message: Message) -> None:
    if not message.from_user or not await is_admin(message.from_user.id) or not message.text:
        return
    oid = int(message.text.removeprefix("/delop_"))
    await db.del_start_op(oid)
    await message.answer(f"🗑 ОП #{oid} удалён.")


@admin_router.message(Command("addop"))
async def admin_addop_cmd(message: Message, state: FSMContext) -> None:
    if not message.from_user or not await is_admin(message.from_user.id) or not message.text:
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /addop &lt;ссылка&gt;")
        return
    await state.set_state(AdminAction.startop_link)
    message.text = parts[1]
    await admin_startop_add(message, state)


CRYPTOBOT_API = "https://pay.crypt.bot/api"


async def cryptobot_request(method: str, params: dict | None = None) -> dict | None:
    token = await db.cfg_get("cryptobot_token")
    if not token:
        return None
    url = f"{CRYPTOBOT_API}/{method}"
    headers = {"Crypto-Pay-API-Token": token}
    async with aiohttp.ClientSession() as s:
        try:
            async with s.post(url, headers=headers, json=params or {}, timeout=20) as r:
                data = await r.json()
                if not data.get("ok"):
                    log.warning("CryptoBot %s -> %s", method, data)
                    return None
                return data.get("result")
        except Exception as e:
            log.error("CryptoBot %s failed: %s", method, e)
            return None


_RATE_CACHE: dict[str, float | int] = {"value": 0.0, "ts": 0}


async def get_usdt_rub_rate() -> float:
    """Получает курс USDT/RUB из CryptoBot getExchangeRates с кэшем на 5 мин.
    Фолбэк - cfg "rate_usdt_rub" (default 100).
    """
    now = int(time.time())
    if _RATE_CACHE["value"] and now - int(_RATE_CACHE["ts"]) < 300:
        return float(_RATE_CACHE["value"])
    rates = await cryptobot_request("getExchangeRates", {})
    if rates:
        for r in rates:
            try:
                if r.get("source") == "USDT" and r.get("target") == "RUB":
                    val = float(r.get("rate") or 0)
                    if val > 0:
                        _RATE_CACHE["value"] = val
                        _RATE_CACHE["ts"] = now
                        return val
            except Exception:
                continue
    try:
        v = float(await db.cfg_get("rate_usdt_rub", "100"))
        return v if v > 0 else 100.0
    except Exception:
        return 100.0


async def create_cryptobot_invoice(user_id: int, rub_amount: float) -> dict | None:
    rate = await get_usdt_rub_rate()
    usdt = round(rub_amount / rate, 2)
    res = await cryptobot_request("createInvoice", {
        "asset": "USDT",
        "amount": str(usdt),
        "description": f"Top-up TopLeads bot, user {user_id}",
        "payload": str(user_id),
        "expires_in": 1800,
    })
    return res


async def check_cryptobot_invoice(invoice_id: str) -> dict | None:
    res = await cryptobot_request("getInvoices", {"invoice_ids": str(invoice_id)})
    if res and res.get("items"):
        return res["items"][0]
    return None


async def worker_publish_loop() -> None:
    await asyncio.sleep(5)
    tick = 0
    auto_tick = 0
    while True:
        try:
            await _check_pic_deletions()
            if tick % 3 == 0:
                await _publish_active_post_orders()
                await _expire_placements()
                await _expire_orders()
            if tick % 30 == 0:
                await _cleanup_legacy_op_cards()
            
            auto_tick += 1
            if auto_tick % 15 == 0:  # каждые 5 минут (15 × 20 сек = 300 сек)
                await _process_auto_views()
                
        except Exception as e:
            log.exception("worker error: %s", e)
        tick += 1
        await asyncio.sleep(20)


async def _publish_active_post_orders() -> None:
    """Публикует post_view-заказы в общем канале для просмотров (один раз на заказ)."""
    vcid_raw = await db.cfg_get("view_channel_id", "")
    if not vcid_raw:
        return
    try:
        view_chat_id = int(vcid_raw)
    except ValueError:
        return
    view_username = await db.cfg_get("view_channel_username", "")
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT * FROM orders WHERE status='active' AND kind='post_view' "
            "AND (expires_at IS NULL OR expires_at > strftime('%s','now')) "
            "AND (post_id IS NULL OR post_id=0)"
        )
        orders = [dict(r) for r in await cur.fetchall()]
    for o in orders:
        if not (o.get("copy_chat_id") and o.get("copy_msg_id")):
            continue
        async with db.conn() as c:
            cur = await c.execute(
                "UPDATE orders SET post_id=-1 WHERE id=? "
                "AND (post_id IS NULL OR post_id=0)",
                (o["id"],),
            )
            await c.commit()
            if cur.rowcount != 1:
                continue
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👁 Просмотрел", callback_data=f"view:{o['id']}")]
        ])
        try:
            cp = await bot.copy_message(
                chat_id=view_chat_id,
                from_chat_id=int(o["copy_chat_id"]),
                message_id=int(o["copy_msg_id"]),
                reply_markup=kb,
            )
        except Exception as e:
            log.warning("post_view publish fail order#%s: %s", o["id"], e)
            async with db.conn() as c:
                await c.execute(
                    "UPDATE orders SET post_id=NULL WHERE id=? AND post_id=-1",
                    (o["id"],),
                )
                await c.commit()
            continue
        post_link = (
            f"https://t.me/{view_username}/{cp.message_id}" if view_username else None
        )
        async with db.conn() as c:
            await c.execute(
                "UPDATE orders SET post_id=?, target_chat_id=?, "
                "target_link=CASE WHEN ? IS NOT NULL THEN ? ELSE target_link END "
                "WHERE id=?",
                (cp.message_id, view_chat_id, post_link, post_link, o["id"]),
            )
            await c.commit()
        log.info("post_view order#%s published to view-channel as msg %s", o["id"], cp.message_id)


async def _cleanup_legacy_op_cards() -> None:
    """Зачищает старые ОП-карточки (отдельные сообщения «Задание: просмотр поста»)
    из подключённых чатов. Сейчас все задания показываются в единой карточке
    при попытке писать в чат — отдельные карточки не нужны."""
    async with db.conn() as c:
        cur = await c.execute("SELECT * FROM op_cards")
        cards = [dict(r) for r in await cur.fetchall()]
    for card in cards:
        try:
            await bot.delete_message(int(card["tg_chat_id"]), int(card["message_id"]))
        except Exception:
            pass
    if cards:
        async with db.conn() as c:
            await c.execute("DELETE FROM op_cards")
            await c.commit()


async def _remove_op_cards_for_order(order_id: int) -> None:
    """Удаляет все ОП-карточки указанного заказа из всех чатов."""
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT * FROM op_cards WHERE order_id=?", (order_id,)
        )
        cards = [dict(r) for r in await cur.fetchall()]
    for card in cards:
        try:
            await bot.delete_message(card["tg_chat_id"], card["message_id"])
        except Exception:
            pass
    async with db.conn() as c:
        await c.execute("DELETE FROM op_cards WHERE order_id=?", (order_id,))
        await c.commit()


async def _expire_placements() -> None:
    """Снимает посты из каналов партнёров и зачисляет владельцам деньги."""
    now = int(time.time())
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT p.*, r.tg_chat_id, r.user_id FROM placements p "
            "JOIN resources r ON r.id = p.resource_id "
            "WHERE p.status='live' AND p.expires_at <= ?",
            (now,),
        )
        rows = [dict(r) for r in await cur.fetchall()]
    for p in rows:
        try:
            await bot.delete_message(p["tg_chat_id"], p["placed_msg_id"])
        except Exception:
            pass
        if not p["paid"]:
            await db.add_balance(p["user_id"], float(p["payout"]), "earn",
                                  f"placement #{p['id']}")
        async with db.conn() as c:
            await c.execute(
                "UPDATE placements SET status='done', paid=1 WHERE id=?", (p["id"],)
            )
            await c.commit()


async def _expire_orders() -> None:
    """Помечает заказы 'done' если завершились по сроку или количеству."""
    now = int(time.time())
    async with db.conn() as c:
        await c.execute(
            "UPDATE orders SET status='done' "
            "WHERE status='active' AND ("
            "  (expires_at IS NOT NULL AND expires_at <= ?)"
            "  OR completed >= quantity)",
            (now,),
        )
        await c.commit()


@router.callback_query(F.data.startswith("view:"))
async def on_view(call: CallbackQuery) -> None:
    if not call.from_user:
        return
    order_id = int(call.data.split(":")[1])
    o = await db.get_order(order_id)
    if not o or o["status"] != "active":
        await call.answer("Заказ уже не активен.", show_alert=True)
        return
    if o["completed"] >= o["quantity"]:
        await call.answer("Лимит заказа выполнен.", show_alert=True)
        return
    payout = await get_sell_price("view_post")
    async with db.conn() as c:
        cur = await c.execute(
            "INSERT OR IGNORE INTO completions(order_id, user_id, kind, payout, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (order_id, call.from_user.id, "post_view", payout, int(time.time())),
        )
        await c.commit()
        if (cur.rowcount or 0) == 0:
            await call.answer("Вы уже отмечались.", show_alert=True)
            return
        await c.execute(
            "UPDATE orders SET completed = completed + 1 WHERE id=?", (order_id,)
        )
        await c.commit()
    await db.add_balance(call.from_user.id, payout, "earn", f"view order #{order_id}")
    await call.answer(f"✅ Просмотр зачтён, +{fmt_money(payout)}", show_alert=True)
    o2 = await db.get_order(order_id)
    if o2 and o2["completed"] >= o2["quantity"]:
        async with db.conn() as c:
            await c.execute(
                "UPDATE orders SET status='done' WHERE id=? AND status='active'",
                (order_id,),
            )
            await c.commit()
        if o2.get("target_chat_id") and o2.get("post_id"):
            try:
                await bot.edit_message_reply_markup(
                    chat_id=int(o2["target_chat_id"]),
                    message_id=int(o2["post_id"]),
                    reply_markup=None,
                )
            except Exception:
                pass
        await _remove_op_cards_for_order(order_id)
        try:
            await bot.send_message(
                int(o2["user_id"]),
                f"🎉 Ваш заказ #{order_id} на просмотры выполнен: "
                f"{o2['completed']}/{o2['quantity']}.",
            )
        except Exception:
            pass  

_last_gate: dict[tuple[int, int], tuple[int, int]] = {}
_GATE_COOLDOWN = 20


async def _user_has_recent_completion(user_id: int, cutoff: int) -> bool:
    """Проверяет, выполнял ли пользователь задания в чатах (не посты)."""
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT 1 FROM completions WHERE user_id=? AND created_at>=? "
            "AND kind NOT IN ('post_view', 'auto_view', 'auto_view_click') LIMIT 1",
            (user_id, cutoff),
        )
        return (await cur.fetchone()) is not None


async def _active_op_orders(enabled_cats: set[str] | None = None) -> list[dict]:
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT * FROM orders WHERE status='active' "
            "AND kind IN ('post_view','channel_sub','chat_join','bot_start') "
            "AND completed < quantity "
            "AND (expires_at IS NULL OR expires_at > strftime('%s','now')) "
            "ORDER BY id DESC"
        )
        orders = [dict(r) for r in await cur.fetchall()]
    if enabled_cats:
        orders = [
            o for o in orders
            if o["kind"] == "post_view"
            or not o.get("category")
            or o["category"] in enabled_cats
        ]
    return orders


def _task_button_for_order(o: dict) -> list[InlineKeyboardButton] | None:
    link = o.get("target_link")
    if not link:
        return None
    
    if not link.startswith("http") and not link.startswith("tg://"):
        uname = link.lstrip("@").strip()
        if not re.match(r"^[A-Za-z0-9_]+$", uname):
            return None
        link = f"https://t.me/{uname}"
    
    label = {
        "post_view":   f"👁 Пост #{o['id']}",
        "channel_sub": "📢 Канал",
        "chat_join":   "💬 Чат",
        "bot_start":   f"🤖 Бот #{o['id']}",
    }.get(o["kind"], "🔗")
    return [InlineKeyboardButton(text=label, url=link)]


async def _build_gate_card(
    user_id: int,
    enabled_cats: set[str] | None,
    max_tasks: int = 5,
    chat_id: int = None,
) -> tuple[str, InlineKeyboardMarkup] | None:
    orders = await _active_op_orders(enabled_cats)
    
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT order_id FROM completions WHERE user_id=?", (user_id,)
        )
        done = {int(r[0]) for r in await cur.fetchall()}
    
    filtered_orders = []
    has_post_view = False
    for o in orders:
        if o["kind"] == "post_view":
            has_post_view = True
        elif o["kind"] == "bot_start":
            continue
        elif o["kind"] in ("channel_sub", "chat_join"):
            target = o.get("target_chat_id")
            if target:
                try:
                    mem = await bot.get_chat_member(int(target), user_id)
                    if mem.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                        log.info("gate_card: SKIP order=%s — user already subscribed", o["id"])
                        continue
                except Exception:
                    pass
            if o["id"] in done:
                log.info("gate_card: SKIP order=%s kind=%s already done", o["id"], o["kind"])
                continue
            filtered_orders.append(o)
        elif o["id"] not in done:
            filtered_orders.append(o)
        else:
            log.info("gate_card: SKIP order=%s kind=%s already done", o["id"], o["kind"])
    
    user = await db.get_user(user_id)
    
    # Если нет своих заданий — пробуем Tgrass
    if not filtered_orders and not has_post_view:
        if user:
            tgrass_data = await get_tgrass_offers(
                user_id, user.username, "ru", False, max_tasks
            )
            if tgrass_data.get("status") == "not_ok" and tgrass_data.get("offers"):
                rows = []
                for offer in tgrass_data["offers"]:
                    if offer.get("subscribed"):
                        continue
                    btn_text = "📢 Подписаться" if offer.get("type") == "channel" else "🔗 Перейти"
                    rows.append([InlineKeyboardButton(
                        text=f"{btn_text}: {offer.get('name', 'Оффер')}",
                        url=offer.get("link", "")
                    )])
                if rows:
                    rows.append([InlineKeyboardButton(text="✅ Проверить", callback_data="verify_tgrass")])
                    text = "📋 <b>Спонсорские задания</b>\n\nВыполните и нажмите «Проверить»:"
                    return text, InlineKeyboardMarkup(inline_keyboard=rows)
                
        # Пробуем Botohub
        if user and not filtered_orders and not has_post_view:
            boh_data = await get_botohub_tasks(user_id)
            if boh_data and boh_data.get("tasks") and not boh_data.get("completed") and not boh_data.get("skip"):
                # Берём только max_tasks заданий
                shown_tasks = boh_data["tasks"][:max_tasks]
                rows = []
                for i, link in enumerate(shown_tasks):
                    rows.append([InlineKeyboardButton(
                        text=f"📢 Задание {i+1}",
                        url=link
                    )])
                if rows:
                    # Сохраняем показанные задания в кеш
                    async with db.conn() as c:
                        await c.execute(
                            "INSERT OR REPLACE INTO subgram_cache(user_id, chat_id, sponsors, created_at) VALUES (?, ?, ?, ?)",
                            (user_id, chat_id or 0, json.dumps({"type": "botohub", "tasks": shown_tasks}), int(time.time()))
                        )
                        await c.commit()
                    
                    rows.append([InlineKeyboardButton(text="✅ Проверить", callback_data="verify_botohub")])
                    text = "📋 <b>Задания от спонсоров</b>\n\nВыполните и нажмите «Проверить»:"
                    return text, InlineKeyboardMarkup(inline_keyboard=rows)
        return None
    
    if not filtered_orders:
        return None
    
    rows: list[list[InlineKeyboardButton]] = []
    shown = 0
    
    for o in filtered_orders:
        if shown >= max_tasks:
            break
        btn = _task_button_for_order(o)
        if btn:
            rows.append(btn)
            shown += 1
    
    if rows:
        rows.append([InlineKeyboardButton(text="✅ Проверить", callback_data="verifyall")])
    
    text = "Чтобы писать в чат — выполните задания:"
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(F.chat.type.in_({"group", "supergroup"}))
async def chat_gateway(message: Message) -> None:
    if not message.from_user or not message.chat:
        return
    if message.from_user.is_bot:
        return
    
    uid = message.from_user.id
    cid = message.chat.id
    
    if message.text and message.text.startswith("/"):
        try:
            await bot.delete_message(cid, message.message_id)
            log.info("gateway: deleted command '%s' from uid=%s in chat=%s", message.text, uid, cid)
        except Exception as e:
            log.warning("gateway: cannot delete command: %s", e)
        return
    
    log.info("gateway: msg from uid=%s in chat=%s", uid, cid)
    
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT * FROM resources WHERE type='chat' AND tg_chat_id=? AND is_active=1",
            (cid,),
        )
        row = await cur.fetchone()
    if not row:
        log.info("gateway: chat=%s not in resources", cid)
        return
    
    res = dict(row)
    try:
        mem = await bot.get_chat_member(cid, uid)
        if mem.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
            log.info("gateway: uid=%s is admin/creator, skip", uid)
            return
    except Exception as e:
        log.warning("gateway: get_chat_member fail uid=%s: %s", uid, e)
    
    bind_hours = max(1, int(res.get("bind_hours") or 1))
    max_tasks = max(1, int(res.get("max_tasks") or 5))
    enabled_cats = set(
        filter(None, (res.get("show_categories") or "").split(","))
    )
    cutoff = int(time.time()) - bind_hours * 3600
    
    if await _user_has_recent_completion(uid, cutoff):
        log.info("gateway: uid=%s in bind window (%sh), allow", uid, bind_hours)
        return
    
    key = (cid, uid)
    last = _last_gate.get(key)
    if last and last[1] == -1:
        log.info("gateway: uid=%s has skip, allow", uid)
        return
    
    built = await _build_gate_card(uid, enabled_cats, max_tasks, cid)
    if not built:
        log.info("gateway: uid=%s has no pending tasks, allow", uid)
        return
    
    log.info("gateway: uid=%s — gating, will delete & show card", uid)
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except Exception as e:
        log.warning("gateway: cannot delete msg in %s: %s", message.chat.id, e)
        return
    
    key = (message.chat.id, message.from_user.id)
    now = int(time.time())
    last = _last_gate.get(key)
    if last and now - last[1] < _GATE_COOLDOWN:
        return
    if last:
        try:
            await bot.delete_message(message.chat.id, last[0])
        except Exception:
            pass
    
    text, kb = built
    name = (message.from_user.first_name or "друг").replace("<", "").replace(">", "")
    mention = f'<a href="tg://user?id={message.from_user.id}">{name}</a>'
    final_text = f"{mention}\n\n{text}"
    try:
        sent = await bot.send_message(
            message.chat.id, final_text, reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.warning("gateway: cannot send card in %s: %s", message.chat.id, e)
        return
    
    _last_gate[key] = (sent.message_id, now)
    async def _auto_del(chat_id: int, msg_id: int) -> None:
        await asyncio.sleep(120)
        try:
            await bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
        if _last_gate.get(key, (0, 0))[0] == msg_id:
            _last_gate.pop(key, None)
    asyncio.create_task(_auto_del(message.chat.id, sent.message_id))


@router.callback_query(F.data == "verifyall")
async def on_verify_all(call: CallbackQuery) -> None:
    if not call.from_user or not call.message or not call.message.chat:
        return
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT * FROM resources WHERE type='chat' AND tg_chat_id=? AND is_active=1",
            (chat_id,),
        )
        row = await cur.fetchone()
    res = dict(row) if row else None
    enabled_cats = set(
        filter(None, ((res.get("show_categories") if res else "") or "").split(","))
    )
    max_tasks = max(1, int((res.get("max_tasks") if res else 5) or 5))
    orders = await _active_op_orders(enabled_cats)
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT order_id FROM completions WHERE user_id=?", (user_id,)
        )
        done = {int(r[0]) for r in await cur.fetchall()}
    pending = [o for o in orders if o["id"] not in done]

    earned_total = 0.0
    counted = 0
    for o in pending:
        if o["completed"] >= o["quantity"]:
            continue
        
        payout = 0
        
        if o["kind"] in ("channel_sub", "chat_join"):
            target = o.get("target_chat_id")
            invite = o.get("target_link", "")
            
            log.info("verifyall: checking order=%s target=%s user=%s", o["id"], target, user_id)
            
            if not target and invite and "/+" in invite:
                continue
            
            if not target:
                log.info("verifyall: no target for order=%s", o["id"])
                continue
            try:
                mem = await bot.get_chat_member(int(target), user_id)
                log.info("verifyall: order=%s status=%s", o["id"], mem.status)
            except Exception as e:
                log.info("verifyall: FAIL order=%s: %s", o["id"], e)
                continue
            if mem.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                continue
            price_key = {"channel_sub": "channel_sub", "chat_join": "chat_join"}[o["kind"]]
            payout = await get_sell_price(price_key)
        else:
            continue
        
        async with db.conn() as c:
            cur2 = await c.execute(
                "INSERT OR IGNORE INTO completions(order_id, user_id, kind, payout, created_at, comment) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (o["id"], user_id, o["kind"], payout, int(time.time()), f"chat:{chat_id}"),
            )
            await c.commit()
            if (cur2.rowcount or 0) == 0:
                continue
            await c.execute(
                "UPDATE orders SET completed = completed + 1 WHERE id=?", (o["id"],)
            )
            await c.commit()
        
        # Начисляем владельцу чата
        chat_owner = res["user_id"] if res else None
        if chat_owner:
            await db.add_balance(chat_owner, payout, "earn", f"verify order #{o['id']} by user {user_id}")
            log.info("verifyall: paid %s to chat owner %s for order %s", fmt_money(payout), chat_owner, o['id'])
        
        earned_total += payout
        counted += 1
        
        o2 = await db.get_order(o["id"])
        if o2 and o2["completed"] >= o2["quantity"]:
            async with db.conn() as c:
                await c.execute(
                    "UPDATE orders SET status='done' WHERE id=? AND status='active'",
                    (o["id"],),
                )
                await c.commit()
            await _remove_op_cards_for_order(o["id"])

    user = await db.get_user(user_id)
    
    built = await _build_gate_card(user_id, enabled_cats, max_tasks, chat_id)
    if not built:
        try:
            await bot.delete_message(chat_id, call.message.message_id)
        except Exception:
            pass
        _last_gate.pop((chat_id, user_id), None)
        if counted > 0:
            await call.answer("✅ Задания выполнены! Доступ открыт.", show_alert=True)
        else:
            await call.answer("Можете писать в чат.", show_alert=True)
        return
    
    text, kb = built
    name = (call.from_user.first_name or "друг").replace("<", "").replace(">", "")
    mention = f'<a href="tg://user?id={user_id}">{name}</a>'
    final_text = f"{mention}\n\n{text}"
    try:
        await call.message.edit_text(
            final_text, reply_markup=kb, disable_web_page_preview=True
        )
    except Exception:
        pass
    
    if counted > 0:
        await call.answer("✅ Задания выполнены! Доступ открыт.", show_alert=True)
    else:
        await call.answer("Пока ничего не засчитано — выполните задания и нажмите снова.", show_alert=True)
        
@router.callback_query(F.data == "verify_botohub")
async def on_verify_botohub(call: CallbackQuery) -> None:
    if not call.from_user:
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id if call.message else None
    
    # Получаем владельца чата (ВНЕ проверки completed)
    chat_owner = None
    if chat_id:
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT user_id FROM resources WHERE type='chat' AND tg_chat_id=? AND is_active=1",
                (chat_id,)
            )
            row = await cur.fetchone()
            if row:
                chat_owner = row["user_id"]
    
    boh_data = await get_botohub_tasks(user_id)
    
    if boh_data and boh_data.get("completed"):
        task_count = len(boh_data.get("tasks", []))
        payout = await get_sell_price("channel_sub")
        total = round(payout * task_count, 2)
        
        if chat_owner:
            await db.add_balance(chat_owner, total, "earn", f"botohub by user {user_id}")
        
        # Удаляем кеш
        async with db.conn() as c:
            await c.execute(
                "DELETE FROM subgram_cache WHERE user_id=? AND chat_id=?",
                (user_id, chat_id or 0)
            )
            await c.commit()
        
        if call.message:
            try:
                await bot.delete_message(call.message.chat.id, call.message.message_id)
            except Exception:
                pass
        
        await call.answer("✅ Задания выполнены! Доступ открыт.", show_alert=True)
    else:
        await call.answer("❌ Не все задания выполнены.", show_alert=True)      
        
@router.callback_query(F.data == "verify_tgrass")
async def on_verify_tgrass(call: CallbackQuery) -> None:
    """Проверка выполнения заданий Tgrass."""
    if not call.from_user:
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id if call.message else None
    
    # Получаем владельца чата
    chat_owner = None
    if chat_id:
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT user_id FROM resources WHERE type='chat' AND tg_chat_id=? AND is_active=1",
                (chat_id,)
            )
            row = await cur.fetchone()
            if row:
                chat_owner = row["user_id"]
    
    tgrass_data = await get_tgrass_offers(
        user_id,
        call.from_user.username,
        call.from_user.language_code or "ru",
        call.from_user.is_premium or False
    )
    
    if tgrass_data.get("status") == "ok":
        # Проверяем отписки
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT offer_id FROM tgrass_logs WHERE user_id=? AND action='subscribed'",
                (user_id,)
            )
            prev_subs = {r["offer_id"] for r in await cur.fetchall()}
        
        current_offer_ids = {o.get("offer_id") for o in tgrass_data.get("offers", [])}
        unsubbed = prev_subs - current_offer_ids
        
        for offer_id in unsubbed:
            ch_name = "—"
            ch_link = "—"
            async with db.conn() as c:
                cur = await c.execute(
                    "SELECT channel_name, channel_link FROM tgrass_logs WHERE user_id=? AND offer_id=? AND action='subscribed' LIMIT 1",
                    (user_id, offer_id)
                )
                row = await cur.fetchone()
                if row:
                    ch_name = row["channel_name"] or "—"
                    ch_link = row["channel_link"] or "—"
            
            async with db.conn() as c:
                await c.execute(
                    "INSERT INTO tgrass_logs(user_id, offer_id, channel_name, channel_link, action, created_at) "
                    "VALUES (?, ?, ?, ?, 'unsubscribed', ?)",
                    (user_id, offer_id, ch_name, ch_link, int(time.time()))
                )
                await c.commit()
        
        # Начисляем за новые подписки
        payout = await get_sell_price("channel_sub")
        earned = 0
        
        for offer in tgrass_data.get("offers", []):
            kind = "channel_sub" if offer.get("type") == "channel" else "chat_join"
            offer_id = offer.get("offer_id", 0)
            
            async with db.conn() as c:
                cur2 = await c.execute(
                    "INSERT OR IGNORE INTO completions(order_id, user_id, kind, payout, created_at, comment) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (offer_id, user_id, kind, payout, int(time.time()), f"tgrass:{chat_id}"),
                )
                await c.commit()
                if (cur2.rowcount or 0) > 0:
                    # Начисляем владельцу чата
                    if chat_owner:
                        await db.add_balance(chat_owner, payout, "earn", f"tgrass {kind} #{offer_id} by user {user_id}")
                    else:
                        await db.add_balance(user_id, payout, "earn", f"tgrass {kind} #{offer_id}")
                    earned += payout
                    
                    # Извлекаем название
                    ch_name = offer.get("name") or "—"
                    ch_link = offer.get("link", "")
                    if ch_name == "—" and ch_link and "t.me/" in ch_link and "/+" not in ch_link:
                        parts = ch_link.split("t.me/")[-1].split("/")[0]
                        if parts:
                            ch_name = f"@{parts}"
                    
                    # Логируем подписку
                    await c.execute(
                        "INSERT INTO tgrass_logs(user_id, offer_id, channel_name, channel_link, action, created_at) "
                        "VALUES (?, ?, ?, ?, 'subscribed', ?)",
                        (user_id, offer_id, ch_name, ch_link, int(time.time()))
                    )
                    await c.commit()
        
        await reset_tgrass_offers(user_id)
        
        if call.message:
            try:
                await bot.delete_message(call.message.chat.id, call.message.message_id)
            except Exception:
                pass
        
        await call.answer(
            "✅ Задания выполнены! Доступ открыт.",
            show_alert=True
        )
    elif tgrass_data.get("status") == "not_ok":
        # Проверяем отписки и при not_ok
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT offer_id FROM tgrass_logs WHERE user_id=? AND action='subscribed'",
                (user_id,)
            )
            prev_subs = {r["offer_id"] for r in await cur.fetchall()}
        
        current_offer_ids = {o.get("offer_id") for o in tgrass_data.get("offers", [])}
        unsubbed = prev_subs - current_offer_ids
        
        for offer_id in unsubbed:
            ch_name = "—"
            ch_link = "—"
            async with db.conn() as c:
                cur = await c.execute(
                    "SELECT channel_name, channel_link FROM tgrass_logs WHERE user_id=? AND offer_id=? AND action='subscribed' LIMIT 1",
                    (user_id, offer_id)
                )
                row = await cur.fetchone()
                if row:
                    ch_name = row["channel_name"] or "—"
                    ch_link = row["channel_link"] or "—"
            
            async with db.conn() as c:
                await c.execute(
                    "INSERT INTO tgrass_logs(user_id, offer_id, channel_name, channel_link, action, created_at) "
                    "VALUES (?, ?, ?, ?, 'unsubscribed', ?)",
                    (user_id, offer_id, ch_name, ch_link, int(time.time()))
                )
                await c.commit()
        
        await call.answer("❌ Не все задания выполнены. Проверьте подписки.", show_alert=True)
    else:
        await call.answer("Нет активных заданий.", show_alert=True) 
        
@router.callback_query(F.data == "skip_tasks")
async def on_skip_tasks(call: CallbackQuery) -> None:
    """Пропустить задания — просто разрешаем писать без наград."""
    if not call.from_user or not call.message or not call.message.chat:
        return
    
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    
    # Удаляем карточку заданий
    try:
        await bot.delete_message(chat_id, call.message.message_id)
    except Exception:
        pass
    
    # Сохраняем скип в _last_gate с timestamp = -1 (флаг скипа)
    key = (chat_id, user_id)
    _last_gate[key] = (0, -1)  # msg_id=0, timestamp=-1 = скип
    
    # Если пользователь не зарегистрирован — предлагаем зарегистрироваться позже
    user = await db.get_user(user_id)
    if not user:
        me = await bot.get_me()
        await call.answer(
            "⏭ Задания пропущены. Можете писать в чат.\n\n"
            f"💡 Чтобы зарабатывать — зарегистрируйтесь в @{me.username}",
            show_alert=True
        )
    else:
        await call.answer(
            "⏭ Вы пропустили задания. Можете писать в чат без наград.",
            show_alert=True
        )  
        
async def get_cryptobot_balance() -> list[dict] | None:
    """Получает баланс кошелька CryptoBot."""
    token = await db.cfg_get("cryptobot_token")
    if not token:
        log.warning("get_cryptobot_balance: no token")
        return None
    
    async with aiohttp.ClientSession() as s:
        try:
            async with s.get(
                f"{CRYPTOBOT_API}/getBalance",
                headers={"Crypto-Pay-API-Token": token},
                timeout=10
            ) as r:
                data = await r.json()
                log.info("CryptoBot getBalance response: %s", data)
                if data.get("ok"):
                    result = data.get("result", [])
                    for b in result:
                        log.info("Balance: %s %s (available: %s)", b.get("available"), b.get("asset"), b.get("available"))
                    return result
                else:
                    log.warning("CryptoBot getBalance error: %s", data)
                    return None
        except Exception as e:
            log.error("CryptoBot getBalance failed: %s", e)
            return None 
        
@admin_router.callback_query(F.data.startswith("reserve_check:"))
async def reserve_check_payment(call: CallbackQuery) -> None:
    """Проверяет оплату счёта пополнения резерва."""
    if not call.from_user or not await is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    
    inv_id = call.data.split(":", 1)[1]
    res = await check_cryptobot_invoice(inv_id)
    
    if res and res.get("status") == "paid":
        amount = float(res.get("amount", 0))
        await call.answer(f"✅ Резерв пополнен на {amount:.2f} USDT!", show_alert=True)
        if call.message:
            await call.message.edit_text(
                f"✅ <b>Резерв успешно пополнен!</b>\n\n"
                f"💰 Сумма: <b>{amount:.2f} USDT</b>\n"
                f"🆔 Транзакция: <code>{inv_id}</code>",
                reply_markup=ikb([[("◀️ В админ-панель", "menu:admin")]])
            )
    else:
        await call.answer("Оплата ещё не поступила. Попробуйте позже.", show_alert=True)   
        
async def _show_auto_views_menu(call: CallbackQuery) -> None:
    """Показывает главное меню автопросмотров."""
    if not call.from_user or not call.message:
        return
    
    user = await db.get_user(call.from_user.id)
    price_per_view = await get_buy_price("auto_views")
    bal = user.balance if user else 0
    can_buy = int(bal / price_per_view) if price_per_view > 0 else 0
    
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT COUNT(*) FROM auto_views_channels WHERE user_id=? AND is_active=1",
            (call.from_user.id,)
        )
        active_channels = (await cur.fetchone())[0]
    
    text = (
        "👁‍🗨 <b>Автопросмотры</b>\n\n"
        "👀 Наш бот предлагает Вам уникальную возможность "
        "увеличения автоматических просмотров на Ваш канал!\n\n"
        f"👁 1 автопросмотр — <b>{fmt_money(price_per_view)}</b>\n"
        f"💳 Баланс — <b>{fmt_money(bal)}</b>\n"
        f"📊 Его хватит на <b>{can_buy}</b> автопросмотров\n"
        f"⏰ Активных каналов: <b>{active_channels}</b>"
    )
    
    rows = [
        [("➕ Добавить канал", "auto:add")],
        [("⏳ Активные каналы", "auto:list")],
        [("◀️ Назад", "menu:buy")],
    ]
    
    await call.message.edit_text(text, reply_markup=ikb(rows))
    await call.answer()


@router.callback_query(F.data == "auto:add")
async def auto_add_channel(call: CallbackQuery, state: FSMContext) -> None:
    """Начинает процесс добавления канала для автопросмотров."""
    if not call.from_user or not call.message:
        return
    
    await state.set_state(AutoViewsFlow.waiting_forward)
    
    await call.message.edit_text(
        "📢 <b>Добавление канала</b>\n\n"
        "💬 Для запуска автопросмотров добавьте нашего бота "
        "<b>@AutoOP_Bot</b> в администраторы Вашего канала, "
        "а затем <b>перешлите любое сообщение из этого канала</b>:",
        reply_markup=kb_back("buy:auto_views")
    )
    await call.answer()


@router.message(AutoViewsFlow.waiting_forward)
async def auto_add_forward(message: Message, state: FSMContext) -> None:
    """Принимает пересланное сообщение и добавляет канал."""
    if not message.from_user or not message.forward_from_chat:
        await message.answer(
            "❌ Перешлите сообщение из канала!",
            reply_markup=kb_back("buy:auto_views")
        )
        return
    
    chat = message.forward_from_chat
    chat_id = chat.id
    username = chat.username
    title = chat.title or f"Канал #{chat_id}"
    
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT 1 FROM auto_views_channels WHERE user_id=? AND chat_id=?",
            (message.from_user.id, chat_id)
        )
        if await cur.fetchone():
            await message.answer(
                f"❌ Канал <b>{title}</b> уже добавлен!",
                reply_markup=kb_back("buy:auto_views")
            )
            await state.clear()
            return
        
        now = int(time.time())
        await c.execute(
            "INSERT INTO auto_views_channels(user_id, chat_id, username, title, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (message.from_user.id, chat_id, username, title, now)
        )
        await c.commit()
    
    await state.clear()
    
    await message.answer(
        f"✅ Канал <b>{title}</b> добавлен!\n\n"
        "Перейдите в раздел «⏳ Активные каналы» для настройки.",
        reply_markup=kb_back("buy:auto_views")
    )


@router.callback_query(F.data == "auto:list")
async def auto_list_channels(call: CallbackQuery) -> None:
    """Показывает список активных каналов."""
    if not call.from_user or not call.message:
        return
    
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT * FROM auto_views_channels WHERE user_id=?",
            (call.from_user.id,)
        )
        channels = [dict(r) for r in await cur.fetchall()]
    
    if not channels:
        await call.message.edit_text(
            "⏳ У вас нет добавленных каналов.\n\n"
            "Нажмите «➕ Добавить канал» чтобы начать.",
            reply_markup=ikb([
                [("➕ Добавить канал", "auto:add")],
                [("◀️ Назад", "buy:auto_views")],
            ])
        )
        await call.answer()
        return
    
    btn_rows = []
    for ch in channels:
        name = ch.get("title") or f"@{ch['username']}" or f"id={ch['chat_id']}"
        status = "🟢" if ch["is_active"] else "🔴"
        btn_rows.append([
            (f"{status} {name}", f"auto:settings:{ch['id']}"),
            ("🗑", f"auto:delete:{ch['id']}")
        ])
    
    btn_rows.append([("➕ Добавить канал", "auto:add")])
    btn_rows.append([("◀️ Назад", "buy:auto_views")])
    
    await call.message.edit_text(
        "⏳ <b>Ваши каналы</b>\n\n"
        "Выберите канал для настройки:",
        reply_markup=ikb(btn_rows)
    )
    await call.answer()


@router.callback_query(F.data.startswith("auto:settings:"))
async def auto_channel_settings(call: CallbackQuery) -> None:
    """Настройки автопросмотров для канала."""
    if not call.from_user or not call.message:
        return
    ch_id = int(call.data.split(":")[2])
    await auto_channel_settings_show(call.message, ch_id, call.from_user.id)
    await call.answer()
    
async def auto_channel_settings_show(message: Message, ch_id: int, user_id: int) -> None:
    """Показывает настройки канала (без callback)."""
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT * FROM auto_views_channels WHERE id=? AND user_id=?",
            (ch_id, user_id)
        )
        ch = await cur.fetchone()
    
    if not ch:
        return
    
    ch = dict(ch)
    name = ch.get("title") or f"@{ch['username']}" or f"id={ch['chat_id']}"
    link = f"https://t.me/{ch['username']}" if ch.get("username") else ""
    
    if link:
        name_display = f'<a href="{link}">{name}</a>'
    else:
        name_display = name
    
    price_per_view = await get_buy_price("auto_views")
    views_per_post = ch.get("views_per_post", 10)
    cost_per_post = round(price_per_view * views_per_post, 2)
    daily_limit = ch.get("daily_limit", 0)
    status = "🟢 Активен" if ch["is_active"] else "🔴 Приостановлен"
    
    user = await db.get_user(user_id)
    balance = user.balance if user else 0
    
    today_start = int(datetime.now().replace(hour=0, minute=0, second=0).timestamp())
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT COUNT(*) FROM auto_views_posts WHERE channel_id=? AND created_at>=?",
            (ch_id, today_start)
        )
        done_today = (await cur.fetchone())[0]
    
    text = (
        f"🛠 <b>Настройки автопросмотров</b>\n\n"
        f"📢 Канал: {name_display}\n\n"
        f"👁 Стоимость автопросмотра: <b>{fmt_money(price_per_view)}</b>\n"
        f"👀 Просмотров на каждый пост: <b>{views_per_post}</b>\n"
        f"💵 Стоимость поста: <b>{fmt_money(cost_per_post)}</b>\n"
        f"💳 Баланс площадки: <b>{fmt_money(balance)}</b>\n\n"
        f"Следующие посты получат по <b>{views_per_post}</b> просмотров\n\n"
        f"🔔 Статус: {status}\n"
        f"📆 Лимит просмотров в день: <b>{daily_limit if daily_limit > 0 else 'не установлен'}</b>\n"
        f"📊 Выполнено сегодня: <b>{done_today}</b>"
    )
    
    rows = [
        [("👀 Изменить просмотры", f"auto:set_views:{ch_id}")],
        [("📆 Установить лимит", f"auto:set_limit:{ch_id}")],
    ]
    if ch["is_active"]:
        rows.append([("⏸ Приостановить", f"auto:pause:{ch_id}")])
    else:
        rows.append([("▶️ Продолжить", f"auto:resume:{ch_id}")])
    rows.append([("🗑 Удалить канал", f"auto:delete:{ch_id}")])
    rows.append([("◀️ Назад", "auto:list")])
    
    try:
        await message.edit_text(text, reply_markup=ikb(rows), disable_web_page_preview=True)
    except Exception:
        pass


# Обработчики действий
@router.callback_query(F.data.startswith("auto:pause:"))
async def auto_pause(call: CallbackQuery) -> None:
    ch_id = int(call.data.split(":")[2])
    async with db.conn() as c:
        await c.execute("UPDATE auto_views_channels SET is_active=0 WHERE id=?", (ch_id,))
        await c.commit()
    await auto_channel_settings_show(call.message, ch_id, call.from_user.id)
    await call.answer("⏸ Приостановлено")


@router.callback_query(F.data.startswith("auto:resume:"))
async def auto_resume(call: CallbackQuery) -> None:
    ch_id = int(call.data.split(":")[2])
    async with db.conn() as c:
        await c.execute("UPDATE auto_views_channels SET is_active=1 WHERE id=?", (ch_id,))
        await c.commit()
    await auto_channel_settings_show(call.message, ch_id, call.from_user.id)
    await call.answer("▶️ Продолжено")


@router.callback_query(F.data.startswith("auto:delete:"))
async def auto_delete_channel(call: CallbackQuery) -> None:
    ch_id = int(call.data.split(":")[2])
    async with db.conn() as c:
        await c.execute("DELETE FROM auto_views_channels WHERE id=?", (ch_id,))
        await c.commit()
    await auto_list_channels(call)
    await call.answer("🗑 Канал удалён")


@router.callback_query(F.data.startswith("auto:set_views:"))
async def auto_set_views(call: CallbackQuery, state: FSMContext) -> None:
    ch_id = int(call.data.split(":")[2])
    await state.set_state(AutoViewsFlow.set_views)
    await state.update_data(auto_ch_id=ch_id)
    
    if call.message:
        await call.message.edit_text(
            "👀 <b>Количество просмотров на пост</b>\n\n"
            "Введите число от 1 до 1000:",
            reply_markup=kb_back(f"auto:settings:{ch_id}")
        )
    await call.answer()


@router.callback_query(F.data.startswith("auto:set_limit:"))
async def auto_set_limit(call: CallbackQuery, state: FSMContext) -> None:
    ch_id = int(call.data.split(":")[2])
    await state.set_state(AutoViewsFlow.set_limit)
    await state.update_data(auto_ch_id=ch_id)
    
    if call.message:
        await call.message.edit_text(
            "📆 <b>Дневной лимит просмотров</b>\n\n"
            "Введите число (0 = без лимита):",
            reply_markup=kb_back(f"auto:settings:{ch_id}")
        )
    await call.answer()


@router.message(AutoViewsFlow.set_views)
async def auto_set_views_value(message: Message, state: FSMContext) -> None:
    if not message.text:
        return
    try:
        n = int(message.text.strip())
        if n < 1 or n > 1000:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите число от 1 до 1000.")
        return
    
    data = await state.get_data()
    ch_id = data["auto_ch_id"]
    
    async with db.conn() as c:
        await c.execute("UPDATE auto_views_channels SET views_per_post=? WHERE id=?", (n, ch_id))
        await c.commit()
    
    await state.clear()
    await message.answer(f"✅ Просмотров на пост: <b>{n}</b>", reply_markup=kb_back(f"auto:settings:{ch_id}"))


@router.message(AutoViewsFlow.set_limit)
async def auto_set_limit_value(message: Message, state: FSMContext) -> None:
    if not message.text:
        return
    try:
        n = int(message.text.strip())
        if n < 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число (0 = без лимита).")
        return
    
    data = await state.get_data()
    ch_id = data["auto_ch_id"]
    
    async with db.conn() as c:
        await c.execute("UPDATE auto_views_channels SET daily_limit=? WHERE id=?", (n, ch_id))
        await c.commit()
    
    await state.clear()
    limit_text = f"<b>{n} просмотров</b>" if n > 0 else "<b>не установлен</b>"
    await message.answer(f"✅ Дневной лимит просмотров: {limit_text}", reply_markup=kb_back(f"auto:settings:{ch_id}")) 
    
async def _process_auto_views() -> None:
    """Проверяет новые посты в каналах и публикует их."""
    log.info("auto_views: checking...")
    view_channel_id_raw = await db.cfg_get("view_channel_id", "")
    if not view_channel_id_raw:
        log.info("auto_views: NO view_channel_id - EXIT")
        return
    
    try:
        view_chat_id = int(view_channel_id_raw)
        log.info("auto_views: view_chat_id=%s", view_chat_id)
    except ValueError:
        log.info("auto_views: BAD view_channel_id")
        return
    
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT * FROM auto_views_channels WHERE is_active=1"
        )
        channels = [dict(r) for r in await cur.fetchall()]
    
    log.info("auto_views: found %s active channels", len(channels))
    
    if not channels:
        return
    
    for ch in channels:
        chat_id = ch["chat_id"]
        username = ch.get("username")
        
        log.info("auto_views: processing channel #%s (chat_id=%s)", ch["id"], chat_id)
        
        views_per_post = int(ch.get("views_per_post", 10))
        daily_limit = int(ch.get("daily_limit") or 0)
        price_per_view = await get_buy_price("auto_views")
        cost = round(price_per_view * views_per_post, 2)
        
        # Проверяем дневной лимит
        if daily_limit > 0:
            today_start = int(datetime.now().replace(hour=0, minute=0, second=0).timestamp())
            async with db.conn() as c:
                cur = await c.execute(
                    "SELECT COUNT(*) FROM auto_views_posts WHERE channel_id=? AND created_at>=?",
                    (ch["id"], today_start)
                )
                posts_today = (await cur.fetchone())[0]
            
            views_today = posts_today * views_per_post
            log.info("auto_views: daily_limit=%s posts_today=%s views_today=%s", daily_limit, posts_today, views_today)
            if views_today >= daily_limit:
                log.info("auto_views: daily limit reached, skip")
                continue
        
        # Проверяем баланс
        user = await db.get_user(ch["user_id"])
        if not user or user.balance < cost:
            log.info("auto_views: insufficient balance (%s < %s), pausing", fmt_money(user.balance if user else 0), fmt_money(cost))
            async with db.conn() as c:
                await c.execute("UPDATE auto_views_channels SET is_active=0 WHERE id=?", (ch["id"],))
                await c.commit()
            try:
                await bot.send_message(
                    ch["user_id"],
                    f"⚠️ Автопросмотры для канала <b>{ch.get('title') or username or chat_id}</b> приостановлены.\n"
                    f"Недостаточно средств ({fmt_money(user.balance if user else 0)})."
                )
            except Exception:
                pass
            continue
        
        # Получаем последний известный message_id
        async with db.conn() as c:
            cur = await c.execute(
                "SELECT original_msg_id FROM auto_views_posts WHERE channel_id=? ORDER BY id DESC LIMIT 1",
                (ch["id"],)
            )
            row = await cur.fetchone()
        
        if row and row["original_msg_id"]:
            last_known_msg_id = int(row["original_msg_id"])
        else:
            # Первый запуск — ищем САМЫЙ ПОСЛЕДНИЙ пост
            log.info("auto_views: first run for channel #%s, finding latest post...", ch["id"])
            last_known_msg_id = 0
            
            for test_id in range(1000, 0, -1):
                try:
                    test_msg = await bot.forward_message(
                        chat_id=view_chat_id,
                        from_chat_id=chat_id,
                        message_id=test_id,
                        disable_notification=True
                    )
                    last_known_msg_id = test_id
                    try:
                        await bot.delete_message(view_chat_id, test_msg.message_id)
                    except:
                        pass
                    log.info("auto_views: latest existing post is #%s", last_known_msg_id)
                    break
                except TelegramBadRequest:
                    continue
            
            if last_known_msg_id == 0:
                log.info("auto_views: channel #%s appears empty", ch["id"])
                continue
            
            now = int(time.time())
            async with db.conn() as c:
                await c.execute(
                    "INSERT INTO auto_views_posts(channel_id, original_chat_id, original_msg_id, our_chat_id, our_msg_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (ch["id"], chat_id, last_known_msg_id, view_chat_id, 0, now)
                )
                await c.commit()
            log.info("auto_views: saved last_known_msg_id=%s to DB", last_known_msg_id)
        
        log.info("auto_views: last_known_msg_id=%s, waiting for new posts...", last_known_msg_id)
        
        # Ищем НОВЫЕ посты БЕЗ ограничения диапазона
        now = int(time.time())
        next_msg_id = last_known_msg_id + 1
        found_new = False
        
        while True:
            try:
                sent = await bot.forward_message(
                    chat_id=view_chat_id,
                    from_chat_id=chat_id,
                    message_id=next_msg_id,
                )
                
                # Нашли новый пост!
                log.info("auto_views: found NEW post #%s!", next_msg_id)
                
                await bot.send_message(
                    chat_id=view_chat_id,
                    text="👁 Нажмите «Просмотрел» после просмотра!",
                    reply_to_message_id=sent.message_id,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="👁 Просмотрел", callback_data=f"av:{ch['id']}:{next_msg_id}")],
                    ])
                )
                
                async with db.conn() as c:
                    await c.execute(
                        "INSERT INTO auto_views_posts(channel_id, original_chat_id, original_msg_id, our_chat_id, our_msg_id, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (ch["id"], chat_id, next_msg_id, view_chat_id, sent.message_id, now)
                    )
                    await c.commit()
                
                await db.add_balance(ch["user_id"], -cost, "spend", f"auto_views new #{next_msg_id}")
                
                log.info("auto_views: published NEW msg #%s from channel #%s, cost=%s", next_msg_id, ch["id"], fmt_money(cost))
                
                found_new = True
                next_msg_id += 1  # Проверяем следующий
                
            except TelegramBadRequest as e:
                err_msg = str(e).lower()
                if "not found" in err_msg or "invalid" in err_msg or "can't be forwarded" in err_msg:
                    break  # Постов больше нет
                else:
                    log.warning("auto_views: error for msg #%s: %s", next_msg_id, e)
                    break
            except Exception as e:
                log.warning("auto_views: error for channel #%s: %s", ch["id"], e)
                break
        
        if not found_new:
            log.info("auto_views: no new posts for channel #%s", ch["id"])
        
        await asyncio.sleep(0.5)
        
async def _publish_auto_post_stub(ch: dict, view_chat_id: int, cost: float, username: str | None) -> None:
    """Публикует пост-заглушку со ссылкой на канал."""
    chat_id = ch["chat_id"]
    
    channel_name = ch.get("title") or f"@{username}" or f"Канал #{chat_id}"
    channel_url = f"https://t.me/{username}" if username else ""
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 Просмотрел", callback_data=f"av:{ch['id']}:0")],
    ])
    
    if channel_url:
        kb.inline_keyboard.append([InlineKeyboardButton(text="📢 Перейти в канал", url=channel_url)])
    
    if channel_url:
        post_text = (
            f"📢 <b>Новый пост из</b> <a href='{channel_url}'>{channel_name}</a>\n\n"
            f"👁 Нажмите «Просмотрел» для накрутки просмотров!"
        )
    else:
        post_text = (
            f"📢 <b>Новый пост из канала</b> {channel_name}\n\n"
            f"👁 Нажмите «Просмотрел» для накрутки просмотров!"
        )
    
    try:
        sent = await bot.send_message(
            chat_id=view_chat_id,
            text=post_text,
            reply_markup=kb,
            disable_web_page_preview=True
        )
        
        now = int(time.time())
        async with db.conn() as c:
            await c.execute(
                "INSERT INTO auto_views_posts(channel_id, original_chat_id, our_chat_id, our_msg_id, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (ch["id"], chat_id, view_chat_id, sent.message_id, now)
            )
            await c.commit()
        
        await db.add_balance(ch["user_id"], -cost, "spend", f"auto_views stub #{ch['id']}")
        
        log.info("auto_views: published stub for channel #%s, cost=%s", ch["id"], fmt_money(cost))
        
    except Exception as e:
        log.warning("auto_views: cannot publish stub for channel #%s: %s", ch["id"], e)        

async def _publish_auto_post(ch: dict, msg: Message, view_chat_id: int, cost: float, username: str | None) -> None:
    """Публикует пост в канал просмотров."""
    chat_id = ch["chat_id"]
    
    if username:
        post_link = f"https://t.me/{username}/{msg.message_id}"
    else:
        chat_id_str = str(chat_id).replace("-100", "")
        post_link = f"https://t.me/c/{chat_id_str}/{msg.message_id}"
    
    channel_name = ch.get("title") or f"@{username}" or f"Канал #{chat_id}"
    channel_url = f"https://t.me/{username}" if username else ""
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 Просмотрел", callback_data=f"av:{ch['id']}:{msg.message_id}")],
        [InlineKeyboardButton(text="📢 Перейти в канал", url=post_link)]
    ])
    
    if channel_url:
        post_text = (
            f"📢 <b>Новый пост из</b> <a href='{channel_url}'>{channel_name}</a>\n\n"
            f"👁 Нажмите «Просмотрел» после просмотра!"
        )
    else:
        post_text = (
            f"📢 <b>Новый пост из канала</b> {channel_name}\n\n"
            f"👁 Нажмите «Просмотрел» после просмотра!"
        )
    
    try:
        sent = await bot.copy_message(
            chat_id=view_chat_id,
            from_chat_id=chat_id,
            message_id=msg.message_id,
            caption=post_text,
            reply_markup=kb
        )
        
        now = int(time.time())
        async with db.conn() as c:
            await c.execute(
                "INSERT INTO auto_views_posts(channel_id, original_chat_id, original_msg_id, our_chat_id, our_msg_id, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (ch["id"], chat_id, msg.message_id, view_chat_id, sent.message_id, now)
            )
            await c.commit()
        
        await db.add_balance(ch["user_id"], -cost, "spend", f"auto_views post #{msg.message_id}")
        
        log.info("auto_views: published post #%s from channel #%s, cost=%s", msg.message_id, ch["id"], fmt_money(cost))
        
    except Exception as e:
        log.warning("auto_views: cannot copy_message: %s, sending text", e)
        try:
            sent = await bot.send_message(
                chat_id=view_chat_id,
                text=post_text,
                reply_markup=kb,
                disable_web_page_preview=True
            )
            
            now = int(time.time())
            async with db.conn() as c:
                await c.execute(
                    "INSERT INTO auto_views_posts(channel_id, original_chat_id, original_msg_id, our_chat_id, our_msg_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (ch["id"], chat_id, msg.message_id, view_chat_id, sent.message_id, now)
                )
                await c.commit()
            
            await db.add_balance(ch["user_id"], -cost, "spend", f"auto_views post #{msg.message_id}")
            
            log.info("auto_views: published text post #%s from channel #%s", msg.message_id, ch["id"])
            
        except Exception as e2:
            log.warning("auto_views: total fail for channel #%s: %s", ch["id"], e2)


@router.callback_query(F.data.startswith("av:"))
async def on_auto_view_click(call: CallbackQuery) -> None:
    """Начисление награды за просмотр авто-поста. Один раз на пользователя на пост."""
    if not call.from_user:
        return
    
    parts = call.data.split(":")
    ch_id = int(parts[1])
    msg_id = int(parts[2])
    user_id = call.from_user.id
    
    # Используем order_id как хеш: ch_id * 1000000 + msg_id
    fake_order_id = ch_id * 1000000 + msg_id
    
    # Проверяем, не нажимал ли уже
    async with db.conn() as c:
        cur = await c.execute(
            "SELECT 1 FROM completions WHERE user_id=? AND order_id=? AND kind='auto_view'",
            (user_id, fake_order_id)
        )
        if await cur.fetchone():
            await call.answer("☑️ Вы уже получали награду за этот пост!", show_alert=True)
            return
    
    payout = await get_sell_price("view_post")
    
    # Сохраняем факт нажатия
    async with db.conn() as c:
        await c.execute(
            "INSERT INTO completions(order_id, user_id, kind, payout, created_at) "
            "VALUES (?, ?, 'auto_view', ?, ?)",
            (fake_order_id, user_id, payout, int(time.time()))
        )
        await c.commit()
    
    await db.add_balance(user_id, payout, "earn", f"auto_view ch#{ch_id} post#{msg_id}")
    
    await call.answer(f"✅ Просмотр зачтён, +{fmt_money(payout)}", show_alert=True)  
    
@router.message(Command("reset_auto"))
async def cmd_reset_auto(message: Message) -> None:
    """Сброс статистики автопросмотров для канала (только админ)."""
    if not message.from_user or not await is_admin(message.from_user.id):
        return
    
    async with db.conn() as c:
        await c.execute("DELETE FROM auto_views_posts")
        await c.commit()
    
    await message.answer("✅ Статистика автопросмотров сброшена. Перезапустите бота.")  
    
@router.callback_query(F.data.regexp(r"^res:\d+:set:max_placements$"))
async def res_set_max_placements(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not call.message:
        return
    rid = int(call.data.split(":")[1])
    r = await db.get_resource(rid)
    if not r or r["user_id"] != call.from_user.id:
        await call.answer("?", show_alert=True); return
    
    cur = r.get("max_placements") or 3
    
    await state.set_state(EditResourcePrice.waiting_price)
    await state.update_data(rid=rid, setting="max_placements")
    
    await call.message.edit_text(
        f"📊 <b>Максимум рекламных постов одновременно</b>\n\n"
        f"Сейчас: <b>{cur}</b>\n\n"
        f"Введите число от 1 до 10:\n"
        f"<i>Новые заказы не будут приниматься, если лимит превышен.</i>",
        reply_markup=ikb([[("◀️ Отмена", f"res:{rid}:0")]])
    )
    await call.answer() 
    
@router.message(Command("block"))
async def cmd_block(message: Message) -> None:
    """Блокировка ресурса по ID или username (только админ)."""
    if not message.from_user or not await is_admin(message.from_user.id):
        return
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /block @username или /block ID\nПример: /block #2 или /block @Testikotik")
        return
    
    text = parts[1].strip()
    
    async with db.conn() as c:
        if text.startswith("@"):
            cur = await c.execute("SELECT * FROM resources WHERE username=? AND is_active=1", (text[1:],))
            row = await cur.fetchone()
        elif text.startswith("#"):
            rid = text[1:]
            if rid.isdigit():
                cur = await c.execute("SELECT * FROM resources WHERE id=?", (int(rid),))
                row = await cur.fetchone()
            else:
                await message.answer("❌ Неверный формат ID. Пример: /block #2")
                return
        elif text.isdigit():
            cur = await c.execute("SELECT * FROM resources WHERE id=?", (int(text),))
            row = await cur.fetchone()
        else:
            await message.answer("❌ Отправьте @username, #ID или ID ресурса.")
            return
        
        if not row:
            await message.answer("❌ Ресурс не найден.")
            return
        
        r = dict(row)
        
        # Добавляем в ЧС
        await c.execute(
            "INSERT OR IGNORE INTO blacklist(tg_chat_id, username, reason, created_at) VALUES (?, ?, ?, ?)",
            (r["tg_chat_id"], r.get("username"), "Заблокирован командой", int(time.time()))
        )
        await c.execute("UPDATE resources SET is_active=0 WHERE id=?", (r["id"],))
        await c.commit()
    
    name = r.get("title") or f"@{r.get('username')}" or f"id={r.get('tg_chat_id')}"
    await message.answer(f"🚫 Ресурс <b>{name}</b> (#{r['id']}) заблокирован.")  
    
@router.message(Command("unblock"))
async def cmd_unblock(message: Message) -> None:
    """Разблокировка ресурса (только админ)."""
    if not message.from_user or not await is_admin(message.from_user.id):
        return
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /unblock @username\nПример: /unblock @Testikotik")
        return
    
    text = parts[1].strip()
    
    if text.startswith("@"):
        username = text[1:]
        async with db.conn() as c:
            await c.execute("DELETE FROM blacklist WHERE username=?", (username,))
            await c.commit()
        await message.answer(f"🔓 @{username} разблокирован.")
    else:
        await message.answer("❌ Используйте @username для разблокировки.") 
        
@router.message(Command("clean_op"))
async def cmd_clean_op(message: Message) -> None:
    """Удаляет дубликаты из ОП на /start (только админ)."""
    if not message.from_user or not await is_admin(message.from_user.id):
        return
    
    async with db.conn() as c:
        # Удаляем дубликаты по username
        await c.execute("""
            DELETE FROM start_op WHERE id NOT IN (
                SELECT MIN(id) FROM start_op GROUP BY username
            )
        """)
        await c.commit()
    
    await message.answer("✅ Дубликаты ОП удалены.")
    
@router.message(Command("subgram_check"))
async def cmd_subgram_check(message: Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    
    token = await db.cfg_get("subgram_api_token", "")
    me = await bot.get_me()
    
    headers = {"Auth": token}
    payload = {"action": "info", "bot_id": me.id}
    
    async with aiohttp.ClientSession() as session:
        async with session.post("https://api.subgram.org/bots", headers=headers, json=payload) as r:
            data = await r.json()
    
    await message.answer(f"📊 <b>SubGram Bot Info:</b>\n<pre>{json.dumps(data, indent=2, ensure_ascii=False)}</pre>")    
    
@router.message(Command("subgram_token"))
async def cmd_subgram_token(message: Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    
    token = await db.cfg_get("subgram_api_token", "")
    if not token:
        token = SUBGRAM_API_KEY
    
    await message.answer(f"Токен SubGram: <code>{token[:10]}...{token[-10:]}</code>\nДлина: {len(token)}")    

async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env var is not set")
    await db.init()
    log.info("DB initialized at %s", DB_PATH)
    me = await bot.get_me()
    log.info("Bot started as @%s (id=%s)", me.username, me.id)
    log.info("Admins: %s", ADMIN_IDS or "none (set ADMIN_IDS env)")
    asyncio.create_task(worker_publish_loop())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
