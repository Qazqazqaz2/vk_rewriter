"""
Flask API для VK Link Rewriter — запуск замены ссылок и потоковый лог.
"""

import os
import sys
import threading
from functools import wraps
from queue import Queue, Empty
from typing import Optional

from flask import Flask, request, Response, render_template, session, redirect, url_for

import vk_link_rewriter as core
import admin_db

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-me-in-production")
admin_db.init_db()

# Состояние текущей задачи
_log_queue: Queue = Queue()
_stop_event = threading.Event()
_worker_thread: Optional[threading.Thread] = None


class QueueWriter:
    """Перенаправляет print() в очередь для стриминга в браузер."""

    def __init__(self, queue: Queue):
        self.queue = queue

    def write(self, text: str) -> None:
        if text:
            self.queue.put(text)

    def flush(self) -> None:
        pass


def run_worker(token: str, old_link: str, new_link: str, communities: list[str], max_post_age_days: Optional[int] = None) -> None:
    global _stop_event
    logger = QueueWriter(_log_queue)
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout = sys.stderr = logger

    try:
        try:
            core.init_vk_api(token=token or None, ignore_env_token=True)
        except Exception as e:
            _log_queue.put(f"❌ Ошибка инициализации VK API: {e}\n")
            _log_queue.put("\x00")  # сигнал конца
            return

        if not communities:
            _log_queue.put("❌ Список сообществ пуст.\n")
            _log_queue.put("\x00")
            return

        _log_queue.put(f"\n🔍 Начинаем обработку {len(communities)} сообществ...\n")
        if max_post_age_days is not None:
            _log_queue.put(f"⏱ Макс. возраст поста: {max_post_age_days} дн.\n")
        for comm in communities:
            if _stop_event.is_set():
                _log_queue.put("\n⏹ Операция остановлена пользователем.\n")
                break
            try:
                core.process_community(comm, old_link, new_link, max_post_age_days=max_post_age_days)
            except Exception as e:
                _log_queue.put(f"❌ Ошибка при обработке {comm}: {e}\n")
        _log_queue.put("\n🎉 Работа завершена!\n")
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr
        _log_queue.put("\x00")  # сигнал конца потока


def _get_client_ip():
    return request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote_addr or ""


def _extract_token(raw: str) -> str:
    """Извлекает access_token из строки (полный URL или чистый токен)."""
    s = (raw or "").strip()
    if not s:
        return ""
    if s.startswith("vk1.") or (len(s) > 20 and "=" not in s):
        return s
    if "access_token=" in s:
        return s.split("access_token=")[-1].split("&")[0].split("#")[0].strip()
    return s


@app.route("/api/me", methods=["POST"])
def api_me():
    """По токену возвращает данные текущего VK-аккаунта (для отображения на сайте)."""
    data = request.get_json() or {}
    token = _extract_token(data.get("token") or "")
    if not token:
        return {"error": "Укажите VK токен"}, 400
    try:
        core.init_vk_api(token=token, ignore_env_token=True, use_rucaptcha_proxy=False)
        user = core.get_current_vk_user()
    except Exception as e:
        return {"error": str(e)}, 400
    if not user:
        return {"error": "Не удалось определить пользователя VK"}, 400
    return {
        "vk_user_id": user["id"],
        "first_name": user["first_name"],
        "last_name": user["last_name"],
        "screen_name": user.get("screen_name") or "",
        "display_name": f"{user['first_name']} {user['last_name']}".strip() or f"id{user['id']}",
    }


@app.route("/api/run", methods=["POST"])
def api_run():
    """Запуск замены ссылок. Тело: JSON { token, old_link, new_link, communities }."""
    global _worker_thread, _log_queue, _stop_event

    if _worker_thread and _worker_thread.is_alive():
        return {"error": "Задача уже выполняется"}, 409

    data = request.get_json() or {}
    token = _extract_token(data.get("token") or "")
    old_link = (data.get("old_link") or "").strip()
    new_link = (data.get("new_link") or "").strip()
    communities = [line.strip() for line in (data.get("communities") or []) if line.strip()]
    max_post_age_days = data.get("max_post_age_days")
    if max_post_age_days is not None:
        try:
            max_post_age_days = int(max_post_age_days)
            if max_post_age_days < 1:
                max_post_age_days = None
        except (TypeError, ValueError):
            max_post_age_days = None

    if not token:
        return {"error": "Укажите VK токен"}, 400
    if not old_link or not new_link:
        return {"error": "Старая и новая ссылки не должны быть пустыми"}, 400
    if not communities:
        return {"error": "Укажите хотя бы одно сообщество"}, 400

    # Определяем VK-аккаунт, проверяем блокировку, пишем в аудит
    try:
        core.init_vk_api(token=token, ignore_env_token=True, use_rucaptcha_proxy=False)
        vk_user = core.get_current_vk_user()
    except Exception as e:
        return {"error": f"Ошибка инициализации VK API: {e}"}, 400
    if not vk_user:
        try:
            details = core.get_last_vk_user_error()
        except Exception:
            details = None
        print(f"⚠️ /api/run: vk_user не определён. details={details!r}")
        payload = {"error": "Не удалось определить пользователя VK по токену"}
        if details:
            payload["details"] = details
        return payload, 400

    vk_user_id = vk_user["id"]
    vk_name = f"{vk_user['first_name']} {vk_user['last_name']}".strip() or f"id{vk_user_id}"

    if admin_db.is_blocked(vk_user_id):
        return {"error": "Этот VK-аккаунт заблокирован для использования сервиса"}, 403

    details = f"old_link={old_link[:80]}, new_link={new_link[:80]}, communities={len(communities)}"
    admin_db.log_audit(vk_user_id, vk_name, "run", details=details, ip=_get_client_ip())

    _stop_event.clear()
    _log_queue = Queue()
    _log_queue.put("🔄 Массовая замена ссылок в постах и комментариях ВК\n")
    _log_queue.put(f"Аккаунт VK: {vk_name} (id{vk_user_id})\n")
    _log_queue.put("Начало обработки...\n\n")

    _worker_thread = threading.Thread(
        target=run_worker,
        args=(token, old_link, new_link, communities, max_post_age_days),
        daemon=True,
    )
    _worker_thread.start()

    # Возвращаем ответ сразу, не держим открытое соединение с клиентом.
    # Фоновый поток продолжит работу даже если клиент отключится.
    return {
        "ok": True,
        "vk_user_id": vk_user_id,
        "vk_name": vk_name,
        "communities": len(communities),
    }


@app.route("/api/log-stream", methods=["GET"])
def api_log_stream():
    """Стриминг лога текущей задачи через SSE (отдельно от запуска)."""

    def generate():
        while True:
            try:
                chunk = _log_queue.get(timeout=30)
            except Empty:
                if _worker_thread and not _worker_thread.is_alive():
                    break
                continue
            if chunk == "\x00":
                break
            for line in chunk.replace("\r", "").split("\n"):
                yield f"data: {line}\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.route("/api/captcha", methods=["GET"])
def api_captcha_get():
    """
    Статус ручного решения VKCaptcha.
    Фронтенд может опрашивать этот эндпоинт и показывать VK ID Captcha SDK.
    """
    uri = (core.get_manual_captcha_redirect_uri() or "").strip()
    return {"has_captcha": bool(uri), "redirect_uri": uri or None}


@app.route("/api/stop", methods=["POST"])
def api_stop():
    """Запрос остановки текущей задачи (между сообществами)."""
    global _stop_event
    _stop_event.set()
    return {"ok": True}


@app.route("/api/status", methods=["GET"])
def api_status():
    """Статус выполнения фоновой задачи."""
    return {"running": bool(_worker_thread and _worker_thread.is_alive())}


@app.route("/api/captcha-success", methods=["POST"])
def api_captcha_success():
    """
    Принимает success_token от фронтенда (VK ID Captcha SDK) для ручного решения капчи.
    Используется как fallback, если ruCaptcha не справилась.
    """
    data = request.get_json() or {}
    token = (data.get("success_token") or "").strip()
    if not token:
        return {"error": "success_token is required"}, 400
    core.provide_manual_success_token(token)
    return {"ok": True}


# --- Админ-панель (кастомный путь, чтобы не светить /admin) ---
ADMIN_BASE = "/g4t54t45yytg5tyg5yt"


def _admin_required(f):
    """Проверка входа в админку (по session)."""
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not admin_db.is_admin_configured():
            return {"error": "Админ-панель отключена: задайте ADMIN_PASSWORD в .env при первом запуске"}, 503
        if session.get("admin_logged_in") is not True:
            return {"error": "Требуется вход в админ-панель"}, 401
        return f(*args, **kwargs)
    return wrapped


@app.route(ADMIN_BASE)
def admin_index():
    if not admin_db.is_admin_configured():
        return "Админ-панель отключена. Задайте переменную окружения ADMIN_PASSWORD при первом запуске.", 503
    admin_db.log_admin_visit(_get_client_ip(), "page_index")
    if session.get("admin_logged_in"):
        return redirect(url_for("admin_dashboard"))
    return redirect(url_for("admin_login"))


@app.route(ADMIN_BASE + "/login", methods=["GET", "POST"])
def admin_login():
    if not admin_db.is_admin_configured():
        return "Админ-панель отключена.", 503
    ip = _get_client_ip()
    if request.method == "GET":
        admin_db.log_admin_visit(ip, "page_login")
    if request.method == "POST":
        password = (request.get_json(silent=True) or {}).get("password", "") if request.is_json else (request.form.get("password") or "").strip()
        if admin_db.check_admin_password(password):
            admin_db.log_admin_visit(ip, "login_ok")
            session["admin_logged_in"] = True
            if request.is_json:
                return {"ok": True}
            return redirect(url_for("admin_dashboard"))
        admin_db.log_admin_visit(ip, "login_fail")
        if request.is_json:
            return {"error": "Неверный пароль"}, 401
    return render_template("admin_login.html")

@app.route(ADMIN_BASE + "/logout", methods=["GET", "POST"])
def admin_logout():
    admin_db.log_admin_visit(_get_client_ip(), "logout")
    session.pop("admin_logged_in", None)
    if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return {"ok": True}
    return redirect(url_for("admin_login"))


@app.route(ADMIN_BASE + "/dashboard")
def admin_dashboard():
    if not admin_db.is_admin_configured():
        return "Админ-панель отключена.", 503
    if not session.get("admin_logged_in"):
        return redirect(url_for("admin_login"))
    admin_db.log_admin_visit(_get_client_ip(), "page_dashboard")
    return render_template("admin_dashboard.html", admin_base=url_for("admin_index").rstrip("/"))


@app.route(ADMIN_BASE + "/api/audit")
@_admin_required
def admin_api_audit():
    limit = min(int(request.args.get("limit", 200)), 500)
    offset = int(request.args.get("offset", 0))
    return {"items": admin_db.get_audit_log(limit=limit, offset=offset)}


@app.route(ADMIN_BASE + "/api/blocked")
@_admin_required
def admin_api_blocked():
    return {"items": admin_db.get_blocked_users()}


@app.route(ADMIN_BASE + "/api/block", methods=["POST"])
@_admin_required
def admin_api_block():
    data = request.get_json() or {}
    try:
        vk_user_id = int(data.get("vk_user_id"))
    except (TypeError, ValueError):
        return {"error": "Укажите целочисленный vk_user_id"}, 400
    reason = (data.get("reason") or "").strip()
    admin_db.block_user(vk_user_id, reason=reason or None)
    return {"ok": True}


@app.route(ADMIN_BASE + "/api/unblock", methods=["POST"])
@_admin_required
def admin_api_unblock():
    data = request.get_json() or {}
    try:
        vk_user_id = int(data.get("vk_user_id"))
    except (TypeError, ValueError):
        return {"error": "Укажите целочисленный vk_user_id"}, 400
    if not admin_db.unblock_user(vk_user_id):
        return {"error": "Аккаунт не был в списке блокировки"}, 404
    return {"ok": True}


@app.route(ADMIN_BASE + "/api/visits")
@_admin_required
def admin_api_visits():
    """Аудит посещений админ-панели по IP."""
    limit = min(int(request.args.get("limit", 200)), 500)
    offset = int(request.args.get("offset", 0))
    return {"items": admin_db.get_admin_visits(limit=limit, offset=offset)}


@app.route(ADMIN_BASE + "/api/change-password", methods=["POST"])
@_admin_required
def admin_api_change_password():
    """Смена пароля админки (текущий пароль + новый)."""
    data = request.get_json() or {}
    current = (data.get("current_password") or "").strip()
    new_pass = (data.get("new_password") or "").strip()
    if not current:
        return {"error": "Укажите текущий пароль"}, 400
    if not admin_db.check_admin_password(current):
        return {"error": "Неверный текущий пароль"}, 401
    try:
        admin_db.set_admin_password(new_pass)
    except ValueError as e:
        return {"error": str(e)}, 400
    return {"ok": True}


@app.route(ADMIN_BASE + "/api/settings")
@_admin_required
def admin_api_settings():
    """Общие настройки админки (без секретов)."""
    proxy = admin_db.get_captcha_proxy() or {}
    return {
        "has_captcha_key": bool(admin_db.get_captcha_api_key()),
        "has_captcha_proxy": bool(proxy.get("address") and proxy.get("port")),
        "captcha_proxy_type": proxy.get("type") or "",
    }


@app.route(ADMIN_BASE + "/api/captcha-key", methods=["POST"])
@_admin_required
def admin_api_captcha_key():
    """Установить/очистить API-ключ сервиса решения капчи."""
    data = request.get_json() or {}
    api_key = (data.get("api_key") or "").strip()
    admin_db.set_captcha_api_key(api_key or None)
    return {"ok": True, "has_captcha_key": bool(api_key)}


@app.route(ADMIN_BASE + "/api/captcha-proxy", methods=["POST"])
@_admin_required
def admin_api_captcha_proxy():
    """Установить/очистить прокси для ruCaptcha VKCaptcha."""
    data = request.get_json() or {}
    proxy_type = (data.get("proxy_type") or "").strip()
    proxy_uri = (data.get("proxy_uri") or "").strip()
    admin_db.set_captcha_proxy(proxy_type or None, proxy_uri or None)
    proxy = admin_db.get_captcha_proxy() or {}
    return {
        "ok": True,
        "has_captcha_proxy": bool(proxy.get("address") and proxy.get("port")),
        "captcha_proxy_type": proxy.get("type") or "",
    }


@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True, threaded=True)
