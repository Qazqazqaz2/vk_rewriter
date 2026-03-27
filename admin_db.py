"""
SQLite-хранилище для аудита действий VK-аккаунтов, списка заблокированных,
пароля админки и аудита посещений админ-панели по IP.
"""
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from werkzeug.security import check_password_hash, generate_password_hash

# Файл БД рядом с проектом (можно переопределить через ADMIN_DB_PATH)
_DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "admin_data.sqlite")


def _get_db_path() -> str:
    return os.getenv("ADMIN_DB_PATH", _DEFAULT_DB_PATH)


@contextmanager
def _connection():
    path = _get_db_path()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    """Создаёт таблицы при первом запуске."""
    with _connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vk_user_id INTEGER NOT NULL,
                vk_name TEXT NOT NULL,
                action TEXT NOT NULL,
                details TEXT,
                ip TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS blocked_vk_users (
                vk_user_id INTEGER PRIMARY KEY,
                reason TEXT,
                blocked_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_vk_user ON audit_log(vk_user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_visits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip TEXT NOT NULL,
                action TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_admin_visits_created ON admin_visits(created_at)")
        # Первый запуск: сохраняем хеш пароля из env, если задан
        row = conn.execute("SELECT value FROM admin_settings WHERE key = ?", ("admin_password_hash",)).fetchone()
        if row is None and os.getenv("ADMIN_PASSWORD"):
            initial = os.getenv("ADMIN_PASSWORD", "").strip()
            if initial:
                conn.execute(
                    "INSERT INTO admin_settings (key, value) VALUES (?, ?)",
                    ("admin_password_hash", generate_password_hash(initial)),
                )


def log_audit(vk_user_id: int, vk_name: str, action: str, details: Optional[str] = None, ip: Optional[str] = None) -> None:
    """Записать действие в аудит."""
    with _connection() as conn:
        conn.execute(
            "INSERT INTO audit_log (vk_user_id, vk_name, action, details, ip, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (vk_user_id, vk_name, action, details or "", ip or "", datetime.utcnow().isoformat() + "Z"),
        )


def is_blocked(vk_user_id: int) -> bool:
    """Проверить, заблокирован ли VK-аккаунт."""
    with _connection() as conn:
        row = conn.execute("SELECT 1 FROM blocked_vk_users WHERE vk_user_id = ?", (vk_user_id,)).fetchone()
        return row is not None


def block_user(vk_user_id: int, reason: Optional[str] = None) -> None:
    """Заблокировать VK-аккаунт."""
    with _connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO blocked_vk_users (vk_user_id, reason, blocked_at) VALUES (?, ?, ?)",
            (vk_user_id, reason or "", datetime.utcnow().isoformat() + "Z"),
        )


def unblock_user(vk_user_id: int) -> bool:
    """Разблокировать VK-аккаунт. Возвращает True, если запись была удалена."""
    with _connection() as conn:
        cur = conn.execute("DELETE FROM blocked_vk_users WHERE vk_user_id = ?", (vk_user_id,))
        return cur.rowcount > 0


def get_audit_log(limit: int = 200, offset: int = 0):
    """Список записей аудита (последние первые)."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, vk_user_id, vk_name, action, details, ip, created_at FROM audit_log ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return [dict(row) for row in cur.fetchall()]


def get_blocked_users():
    """Список заблокированных VK user_id с причиной и датой."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT vk_user_id, reason, blocked_at FROM blocked_vk_users ORDER BY blocked_at DESC"
        )
        return [dict(row) for row in cur.fetchall()]


# --- Пароль админки (хранится в БД, при первом запуске берётся из env) ---
def is_admin_configured() -> bool:
    """Есть ли сохранённый пароль админки (в БД или в env при первом запуске)."""
    with _connection() as conn:
        row = conn.execute("SELECT 1 FROM admin_settings WHERE key = ?", ("admin_password_hash",)).fetchone()
        if row is not None:
            return True
    return bool(os.getenv("ADMIN_PASSWORD", "").strip())


def check_admin_password(password: str) -> bool:
    """Проверить пароль. Использует хеш из БД или env при первом входе."""
    with _connection() as conn:
        row = conn.execute("SELECT value FROM admin_settings WHERE key = ?", ("admin_password_hash",)).fetchone()
        if row is not None:
            return check_password_hash(row["value"], password)
    # Первый запуск: пароль только в env
    return password == os.getenv("ADMIN_PASSWORD", "").strip()


def set_admin_password(new_password: str) -> None:
    """Установить новый пароль (сохраняется хеш в БД)."""
    if not new_password or len(new_password) < 4:
        raise ValueError("Пароль должен быть не короче 4 символов")
    with _connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO admin_settings (key, value) VALUES (?, ?)",
            ("admin_password_hash", generate_password_hash(new_password)),
        )


# --- Ключ сервиса решения капчи ---
def get_captcha_api_key() -> str:
    """Вернуть API-ключ сервиса решения капчи (или пустую строку, если не задан)."""
    with _connection() as conn:
        row = conn.execute(
            "SELECT value FROM admin_settings WHERE key = ?",
            ("captcha_api_key",),
        ).fetchone()
        return row["value"] if row is not None else ""


def set_captcha_api_key(api_key: Optional[str]) -> None:
    """Сохранить/очистить API-ключ сервиса решения капчи."""
    key = (api_key or "").strip()
    with _connection() as conn:
        if not key:
            conn.execute(
                "DELETE FROM admin_settings WHERE key = ?",
                ("captcha_api_key",),
            )
        else:
            conn.execute(
                "INSERT OR REPLACE INTO admin_settings (key, value) VALUES (?, ?)",
                ("captcha_api_key", key),
            )


def get_captcha_proxy() -> Optional[dict]:
    """Прокси для ruCaptcha VKCaptchaTask: {'type': 'http', 'address': '...', 'port': 8080, 'login': '', 'password': ''}."""
    with _connection() as conn:
        trow = conn.execute("SELECT value FROM admin_settings WHERE key = ?", ("captcha_proxy_type",)).fetchone()
        urow = conn.execute("SELECT value FROM admin_settings WHERE key = ?", ("captcha_proxy_uri",)).fetchone()
        ptype = (trow["value"] if trow else "").strip().lower() or "http"
        uri = (urow["value"] if urow else "").strip()
        if not uri:
            return None
        login, password = "", ""
        host_part = uri
        if "@" in uri:
            creds, host_part = uri.rsplit("@", 1)
            if ":" in creds:
                login, password = creds.split(":", 1)
            else:
                login = creds
        if ":" not in host_part:
            return None
        host, port_str = host_part.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            return None
        return {"type": ptype, "address": host, "port": port, "login": login, "password": password}


def set_captcha_proxy(proxy_type: Optional[str], proxy_uri: Optional[str]) -> None:
    proxy_type = (proxy_type or "").strip().lower() or None
    proxy_uri = (proxy_uri or "").strip() or None
    with _connection() as conn:
        if not proxy_type or not proxy_uri:
            conn.execute("DELETE FROM admin_settings WHERE key IN (?, ?)", ("captcha_proxy_type", "captcha_proxy_uri"))
            return
        conn.execute("INSERT OR REPLACE INTO admin_settings (key, value) VALUES (?, ?)", ("captcha_proxy_type", proxy_type))
        conn.execute("INSERT OR REPLACE INTO admin_settings (key, value) VALUES (?, ?)", ("captcha_proxy_uri", proxy_uri))


# --- Аудит посещений админ-панели по IP ---
def log_admin_visit(ip: str, action: str) -> None:
    """Записать посещение админки (action: page_login, page_dashboard, login_ok, login_fail, logout)."""
    with _connection() as conn:
        conn.execute(
            "INSERT INTO admin_visits (ip, action, created_at) VALUES (?, ?, ?)",
            (ip or "", action, datetime.utcnow().isoformat() + "Z"),
        )


def get_admin_visits(limit: int = 200, offset: int = 0):
    """Список посещений админки (последние первые)."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, ip, action, created_at FROM admin_visits ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return [dict(row) for row in cur.fetchall()]
