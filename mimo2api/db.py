"""
PostgreSQL 存储模块 — 替代所有本地文件 / SQLite 存储，适配 serverless 环境。
使用 psycopg2 同步连接 + asyncio.to_thread() 包装（与原代码模式一致）。
"""
import json
import os
import logging
import psycopg2
import psycopg2.extras
import psycopg2.pool

logger = logging.getLogger(__name__)

_DSN = os.getenv("DATABASE_URL", "")

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        if not _DSN:
            raise RuntimeError("DATABASE_URL 环境变量未配置，无法连接 PostgreSQL")
        # 确保 connect_timeout 存在（秒），防止连接无限挂起
        dsn = _DSN
        if "connect_timeout" not in dsn:
            sep = "&" if "?" in dsn else "?"
            dsn = f"{dsn}{sep}connect_timeout=10"
        _pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=5, dsn=dsn)
        logger.info("🐘 PostgreSQL 连接池已初始化")
    return _pool


class _ConnCtx:
    """同步连接上下文管理器，从连接池获取并归还。"""
    def __enter__(self):
        self.conn = _get_pool().getconn()
        self.conn.autocommit = True
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        _get_pool().putconn(self.conn)


def get_conn():
    return _ConnCtx()


# ──── 通用 KV 存储（替代 JSON 文件快照 / model_mapping） ────

def kv_init():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kv_store (
                    key   TEXT PRIMARY KEY,
                    value JSONB NOT NULL
                )
            """)


def kv_get(key: str) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM kv_store WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else None


def kv_put(key: str, value: dict) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO kv_store (key, value) VALUES (%s, %s::jsonb) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, json.dumps(value, ensure_ascii=False)),
            )


# ──── 用户存储 ────

def users_init():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id          TEXT PRIMARY KEY,
                    service_token    TEXT NOT NULL DEFAULT '',
                    xiaomichatbot_ph TEXT NOT NULL DEFAULT '',
                    name             TEXT NOT NULL DEFAULT '',
                    raw_data         JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)


def users_load_all() -> dict[str, dict]:
    """返回 {userId: raw_data_dict, ...}"""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM users")
            result = {}
            for row in cur.fetchall():
                uid = str(row["user_id"])
                result[uid] = dict(row["raw_data"])
                result[uid]["userId"] = uid
            return result


def users_add(uid: str, service_token: str, xiaomichatbot_ph: str, name: str, raw_data: dict) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (user_id, service_token, xiaomichatbot_ph, name, raw_data) "
                    "VALUES (%s, %s, %s, %s, %s::jsonb) "
                    "ON CONFLICT (user_id) DO UPDATE SET "
                    "service_token = EXCLUDED.service_token, "
                    "xiaomichatbot_ph = EXCLUDED.xiaomichatbot_ph, "
                    "name = EXCLUDED.name, "
                    "raw_data = EXCLUDED.raw_data",
                    (uid, service_token, xiaomichatbot_ph, name, json.dumps(raw_data, ensure_ascii=False)),
                )
    except Exception as e:
        if "does not exist" in str(e):
            users_init()
            users_add(uid, service_token, xiaomichatbot_ph, name, raw_data)
        else:
            raise


def users_delete(uid: str) -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE user_id = %s", (uid,))
                return cur.rowcount > 0
    except Exception as e:
        if "does not exist" in str(e):
            return False
        raise


def users_list_raw() -> list[dict]:
    """返回所有用户 raw_data 列表（供 UI 接口使用）"""
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT raw_data FROM users")
                return [dict(row["raw_data"]) for row in cur.fetchall()]
    except Exception as e:
        if "does not exist" in str(e):
            return []
        raise
