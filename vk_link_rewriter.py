import json
import os
import time
import re
import sys
from urllib.parse import urlparse, urlunparse
from typing import Optional
from collections import deque
import threading

import vk_api
from vk_api.exceptions import ApiError, Captcha
from vk_api.vk_api import DEFAULT_USERAGENT
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib.parse import parse_qs, urlencode
try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None

import admin_db

load_dotenv()

VK_TOKEN: Optional[str] = os.getenv("VK_TOKEN")

vk_session: Optional[vk_api.VkApi] = None
vk = None

request_times = deque()

# Последняя ошибка при попытке определить текущего пользователя по токену.
_last_vk_user_error: Optional[str] = None

# Единый User-Agent для VK API и ruCaptcha VKCaptchaTask.
# Важно: mismatch UA может снижать шанс успешного прохождения not_robot_captcha.
VK_CAPTCHA_USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) "
        "Gecko/20100101 Firefox/132.0"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
]


def _get_vk_captcha_user_agent(attempt: int = 0) -> str:
    custom = (os.getenv("VK_CAPTCHA_USER_AGENT") or "").strip()
    if custom:
        return custom
    return VK_CAPTCHA_USER_AGENTS[attempt % len(VK_CAPTCHA_USER_AGENTS)]


from urllib.parse import urlparse  # уже должен быть импортирован, но на всякий случай

def _proxy_to_url(proxy: dict) -> str:
    """ФИНАЛЬНАЯ ВЕРСИЯ: поддержка https + 100% удаление дублей http://"""
    if not proxy or not proxy.get("address"):
        return None

    # Тип прокси — берём из настроек (теперь можно https)
    ptype = (proxy.get("type") or "http").strip().lower()
    if ptype not in ("http", "https", "socks5"):
        ptype = "https"   # по умолчанию теперь https, как ты хочешь

    # === АГРЕССИВНАЯ ОЧИСТКА ADDRESS ===
    address = str(proxy.get("address")).strip()

    # Если в address уже есть полный URL (http:// или https://)
    if "://" in address:
        # Берём только хост (отрезаем scheme)
        address = address.split("://")[-1]

    # Убираем порт, если он случайно прилип к адресу
    if ":" in address:
        address = address.split(":")[0]

    # Убираем лишние слэши и пробелы
    address = address.split("/")[0].strip()

    login = (proxy.get("login") or "").strip()
    password = (proxy.get("password") or "").strip()

    auth = ""
    if login:
        auth = f"{login}:{password}@" if password else f"{login}@"

    proxy_url = f"{ptype}://{auth}{address}:{proxy.get('port')}"

    print(f"🔧 Сформирован прокси для VK: {proxy_url}  (тип: {ptype})")
    return proxy_url

# ---------------------------------------------------------------------------
#  Перехват redirect_uri / remixstlid из JSON-ответа VK
# ---------------------------------------------------------------------------
_captcha_lock = threading.Lock()
_last_captcha_errors: dict[int, dict] = {}


def _vk_response_hook(response, *args, **kwargs):
    """requests response-hook: сохраняет error dict при captcha (code 14)."""
    try:
        if response.status_code == 200 and "api.vk.com" in (response.url or ""):
            data = response.json()
            error = data.get("error")
            if isinstance(error, dict) and error.get("error_code") == 14:
                with _captcha_lock:
                    _last_captcha_errors[threading.current_thread().ident] = dict(error)
    except Exception:
        pass


def _pop_last_captcha_error() -> dict:
    """Извлечь и удалить сохранённую ошибку captcha для текущего потока."""
    with _captcha_lock:
        return _last_captcha_errors.pop(threading.current_thread().ident, {})


# ---------------------------------------------------------------------------
#  Ручное решение VKCaptcha через VK ID Captcha SDK (в браузере)
# ---------------------------------------------------------------------------
_manual_captcha_event = threading.Event()
_manual_success_token: Optional[str] = None
_manual_redirect_uri: Optional[str] = None


def provide_manual_success_token(token: Optional[str]) -> None:
    """Вызывается из Flask, когда фронтенд получил success_token."""
    global _manual_success_token
    _manual_success_token = (token or "").strip() or None
    if _manual_success_token:
        _manual_captcha_event.set()


def get_manual_captcha_redirect_uri() -> Optional[str]:
    """redirect_uri, по которому сейчас ожидается ручное решение капчи."""
    return _manual_redirect_uri


def _wait_for_manual_success_token(timeout: float = 180.0) -> Optional[str]:
    """Блокирующее ожидание success_token от пользователя (fallback)."""
    global _manual_success_token
    _manual_success_token = None
    _manual_captcha_event.clear()
    if not _manual_captcha_event.wait(timeout):
        return None
    token = _manual_success_token
    _manual_success_token = None
    return token


# ---------------------------------------------------------------------------
#  HTTP-сессия с retry / timeout
# ---------------------------------------------------------------------------
def _build_http_session(
    timeout: tuple[float, float] = (10.0, 60.0),
    retries: int = 3,
    backoff_factor: float = 0.5,
) -> requests.Session:
    session = requests.Session()
    session.headers.setdefault("User-agent", _get_vk_captcha_user_agent(0))

    if Retry is not None:
        retry = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

    original_request = session.request

    def request_with_timeout(method, url, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = timeout
        return original_request(method, url, **kwargs)

    session.request = request_with_timeout
    return session


# ---------------------------------------------------------------------------
#  Инициализация VK API
# ---------------------------------------------------------------------------
def init_vk_api(
    token: Optional[str] = None,
    ignore_env_token: bool = False,
    use_rucaptcha_proxy: bool = True,
) -> None:
    global VK_TOKEN, vk_session, vk

    token_arg = (token or "").strip() or None
    http_session = _build_http_session()
    if (
        use_rucaptcha_proxy
        and (os.getenv("VK_USE_RUCAPTCHA_PROXY") or "1").strip().lower() not in ("0", "false", "no")
    ):
        proxy_for_vk = _get_rucaptcha_proxy()
        if proxy_for_vk:
            proxy_url = _proxy_to_url(proxy_for_vk)
            http_session.proxies.update({"http": proxy_url, "https": proxy_url})
            print(
                "🔌 VK API использует прокси ruCaptcha: "
                f"{proxy_for_vk.get('type')}://{proxy_for_vk.get('address')}:{proxy_for_vk.get('port')}"
            )

    if token_arg:
        VK_TOKEN = token_arg
    elif not ignore_env_token and VK_TOKEN:
        pass
    else:
        raise RuntimeError("Укажите VK токен (или задайте VK_TOKEN в .env).")

    vk_session = vk_api.VkApi(token=VK_TOKEN, session=http_session)

    _http = getattr(vk_session, "https", None)
    if _http is not None:
        hooks_list = _http.hooks.get("response")
        if hooks_list is None:
            _http.hooks["response"] = [_vk_response_hook]
        elif isinstance(hooks_list, list):
            if _vk_response_hook not in hooks_list:
                hooks_list.append(_vk_response_hook)
        else:
            _http.hooks["response"] = [hooks_list, _vk_response_hook]

    vk = vk_session.get_api()


def get_current_vk_user() -> Optional[dict]:
    """Возвращает данные текущего пользователя VK (владельца токена)."""
    if vk_session is None:
        return None

    global _last_vk_user_error
    _last_vk_user_error = None

    # На практике иногда `users.get` может вернуть пусто/ошибку
    # (например, из-за просроченного токена или отсутствия прав),
    # поэтому делаем fallback на другой метод и логируем исключения.
    try:
        users = vk_session.method(
            "users.get",
            {
                "v": "5.131",
                "fields": "id,first_name,last_name,screen_name",
            },
        )
        if users and len(users) > 0:
            u = users[0] or {}
            return {
                "id": u.get("id"),
                "first_name": u.get("first_name", "") or "",
                "last_name": u.get("last_name", "") or "",
                "screen_name": u.get("screen_name", "") or "",
            }
        _last_vk_user_error = "users.get вернул пустой ответ"
    except Exception as e:
        _last_vk_user_error = str(e)
        print(f"⚠️ get_current_vk_user: users.get ошибка: {e}")

    try:
        info = vk_session.method("account.getProfileInfo", {"v": "5.131"})
        if isinstance(info, dict):
            # На некоторых версиях полей может не быть — подставляем пустые строки.
            uid = info.get("id") or info.get("user_id") or info.get("uid")
            if uid:
                return {
                    "id": uid,
                    "first_name": info.get("first_name", "") or "",
                    "last_name": info.get("last_name", "") or "",
                    "screen_name": info.get("screen_name", "") or "",
                }
        _last_vk_user_error = "account.getProfileInfo вернул пустой ответ"
    except Exception as e:
        _last_vk_user_error = str(e)
        print(f"⚠️ get_current_vk_user: account.getProfileInfo ошибка: {e}")

    return None


def get_last_vk_user_error() -> Optional[str]:
    """Возвращает текст последней ошибки при определении текущего VK-пользователя."""
    return _last_vk_user_error


# ---------------------------------------------------------------------------
#  Нормализация redirect_uri VK (*.vk.ru → *.vk.com)
# ---------------------------------------------------------------------------
def _normalize_vk_redirect_uri(uri: str) -> str:
    """Полная очистка redirectUri: убирает origin=127.0.0.1 + нормализует vk.ru → vk.com"""
    try:
        uri = (uri or "").strip()
        if not uri:
            return uri

        parsed = urlparse(uri)
        query_params = parse_qs(parsed.query)

        # === ГЛАВНОЕ ИСПРАВЛЕНИЕ ===
        if "origin" in query_params:
            old_origin = query_params["origin"][0]
            print(f"🧹 УДАЛЯЕМ локальный origin из redirectUri: {old_origin}")
            query_params.pop("origin", None)

        # Добавляем нормальный origin (RuCaptcha это принимает)
        query_params["origin"] = ["https://vk.com"]

        # Нормализация домена
        host = parsed.netloc or ""
        new_host = host
        if host.endswith(".vk.ru"):
            new_host = host.replace(".vk.ru", ".vk.com")
        elif host in ("vk.ru", "id.vk.ru"):
            new_host = host.replace("vk.ru", "vk.com")

        new_query = urlencode(query_params, doseq=True)
        normalized = urlunparse(parsed._replace(netloc=new_host, query=new_query))

        if normalized != uri:
            print(f"🔁 redirectUri исправлен → {normalized[:160]}...")
        return normalized

    except Exception as e:
        print(f"⚠️ Ошибка нормализации redirectUri: {e}")
        return uri


# ---------------------------------------------------------------------------
#  Прокси для ruCaptcha
# ---------------------------------------------------------------------------
def _get_rucaptcha_proxy() -> Optional[dict]:
    """Прокси для ruCaptcha: сначала из админки, потом из env."""
    proxy = admin_db.get_captcha_proxy()
    if proxy and proxy.get("address") and proxy.get("port"):
        return proxy
    addr = (os.getenv("RUCAPTCHA_PROXY_ADDRESS") or "").strip()
    port_s = (os.getenv("RUCAPTCHA_PROXY_PORT") or "").strip()
    if not addr or not port_s:
        return None
    try:
        port = int(port_s)
    except ValueError:
        return None
    ptype = (os.getenv("RUCAPTCHA_PROXY_TYPE") or "https").strip().lower()
    if ptype not in ("http", "https", "socks5"):
        ptype = "https"
    return {
        "type": ptype,
        "address": addr,
        "port": port,
        "login": (os.getenv("RUCAPTCHA_PROXY_LOGIN") or "").strip(),
        "password": (os.getenv("RUCAPTCHA_PROXY_PASSWORD") or "").strip(),
    }


def _get_rucaptcha_proxy_pool() -> list[dict]:
    """
    Пул прокси для ruCaptcha из env RUCAPTCHA_PROXY_POOL.
    Формат: строки вида type://login:pass@host:port или host:port.
    """
    raw = (os.getenv("RUCAPTCHA_PROXY_POOL") or "").strip()
    if not raw:
        return []
    result: list[dict] = []
    for chunk in re.split(r"[,\n;]+", raw):
        item = (chunk or "").strip()
        if not item:
            continue
        if "://" not in item:
            item = "https://" + item
        try:
            parsed = urlparse(item)
            ptype = (parsed.scheme or "https").lower()
            if ptype not in ("http", "https", "socks5"):
                ptype = "https"
            if not parsed.hostname or not parsed.port:
                continue
            result.append(
                {
                    "type": ptype,
                    "address": parsed.hostname,
                    "port": int(parsed.port),
                    "login": parsed.username or "",
                    "password": parsed.password or "",
                }
            )
        except Exception:
            continue
    return result


# ---------------------------------------------------------------------------
#  Решение VKCaptcha через ruCaptcha (VKCaptchaTask)
# ---------------------------------------------------------------------------
def _solve_vkcaptcha_single_task(
    api_key: str,
    task: dict,
    max_wait: int = 180,
    poll_interval: int = 5,
) -> tuple[Optional[str], Optional[str]]:
    """
    Создаёт одну задачу ruCaptcha и ждёт результат.
    Возвращает (token, error_code).
    """
    try:
        resp = requests.post(
            "https://api.rucaptcha.com/createTask",
            json={"clientKey": api_key, "task": task},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        print(f"🔎 ruCaptcha createTask response: {data}")
    except Exception as e:
        print(f"⚠️ ruCaptcha createTask ошибка: {e}")
        return None, None

    if not isinstance(data, dict) or data.get("errorId"):
        print(f"⚠️ ruCaptcha createTask вернул ошибку: {data}")
        return None, (data.get("errorCode") if isinstance(data, dict) else None)

    task_id = data.get("taskId")
    if not task_id:
        print(f"⚠️ ruCaptcha createTask: нет taskId: {data}")
        return None, None

    result_payload = {"clientKey": api_key, "taskId": task_id}
    started = time.time()

    while time.time() - started < max_wait:
        time.sleep(poll_interval)
        try:
            r = requests.post(
                "https://api.rucaptcha.com/getTaskResult",
                json=result_payload,
                timeout=30,
            )
            r.raise_for_status()
            rdata = r.json()
        except Exception as e:
            print(f"⚠️ ruCaptcha getTaskResult ошибка: {e}, повтор через {poll_interval} сек…")
            continue

        if not isinstance(rdata, dict):
            continue

        err_id = rdata.get("errorId", 0)
        if err_id:
            err_code = rdata.get("errorCode") or f"errorId_{err_id}"
            print(f"⚠️ ruCaptcha getTaskResult ошибка: {rdata}")
            return None, str(err_code)

        status = rdata.get("status")
        if status == "processing":
            print("🔎 ruCaptcha: processing, ждём {} сек…".format(poll_interval))
            continue
        if status != "ready":
            print(f"⚠️ ruCaptcha: неожиданный статус '{status}': {rdata}")
            return None, None

        token = (rdata.get("solution", {}).get("token") or "").strip()
        if not token:
            print(f"⚠️ ruCaptcha: нет token в solution: {rdata}")
            return None, None
        print(f"🔎 ruCaptcha: получен success_token ({len(token)} символов)")
        return token, None

    print("⚠️ ruCaptcha: таймаут ожидания решения VKCaptcha.")
    return None, "TIMEOUT"


def _solve_vkcaptcha_via_rucaptcha(redirect_uri: str) -> Optional[str]:
    """Отправляет VKCaptcha в ruCaptcha, возвращает success_token или None."""
    api_key = (admin_db.get_captcha_api_key() or "").strip()
    if not api_key:
        print("⚠️ ruCaptcha: API ключ не задан в админке.")
        return None

    redirect_uri = _normalize_vk_redirect_uri(redirect_uri or "")
    if not redirect_uri:
        return None

    # Для VKCaptchaTask важно: работники должны воспроизвести запрос к капче
    # в том же окружении (минимум — тот же proxy и userAgent).
    # Поэтому фиксируем proxy и userAgent и НЕ ротируем их на ретраях.
    proxy = _get_rucaptcha_proxy()
    if not proxy:
        pool = _get_rucaptcha_proxy_pool()
        proxy = pool[0] if pool else None
    if not proxy:
        print(
            "⚠️ ruCaptcha VKCaptchaTask: не задан прокси. "
            "Укажите в админке или в env (RUCAPTCHA_PROXY_ADDRESS, RUCAPTCHA_PROXY_PORT)."
        )
        return None

    print(f"🔎 ruCaptcha VKCaptchaTask: redirect_uri: {redirect_uri[:80]}…")
    print(
        "🔎 ruCaptcha VKCaptchaTask: proxy fixed: "
        f"{proxy['type']}://{proxy['address']}:{proxy['port']}"
    )
    ua = _get_vk_captcha_user_agent(0)
    print(f"🔎 ruCaptcha VKCaptchaTask: userAgent fixed: {ua[:72]}…")

    max_retries = 10
    try:
        max_retries = max(1, int((os.getenv("RUCAPTCHA_MAX_RETRIES") or "").strip() or 10))
    except ValueError:
        max_retries = 10

    unsolvable_streak = 0
    for attempt in range(max_retries):
        task = {
            "type": "VKCaptchaTask",
            "redirectUri": redirect_uri,
            "userAgent": ua,
            "proxyType": proxy["type"],
            "proxyAddress": proxy["address"],
            "proxyPort": proxy["port"],
        }
        if proxy.get("login"):
            task["proxyLogin"] = proxy["login"]
        if proxy.get("password"):
            task["proxyPassword"] = proxy["password"]

        # Для VK капчи обычно лучше русскоязычный пул воркеров.
        task_payload = dict(task)
        task_payload["languagePool"] = "rn"

        if attempt > 0:
            print(f"🔐 ruCaptcha: повторная попытка {attempt + 1}/{max_retries} (ERROR_CAPTCHA_UNSOLVABLE)…")
        else:
            print("🔐 ruCaptcha: создаём VKCaptchaTask…")
        # UA/proxy фиксированы, поэтому логируем только факт ретрая.

        token, err_code = _solve_vkcaptcha_single_task(
            api_key, task_payload, max_wait=120, poll_interval=10
        )
        if token:
            return token
        if err_code == "ERROR_CAPTCHA_UNSOLVABLE" and attempt < max_retries - 1:
            unsolvable_streak += 1
            print(
                "⚠️ ruCaptcha: работники не смогли решить капчу. "
                "Проверьте качество прокси (режим РФ/СНГ повышает шансы)."
            )
            if unsolvable_streak >= 3:
                print(
                    "⏭ Прерываем авто-попытки ruCaptcha раньше: один и тот же прокси дал "
                    "несколько ERROR_CAPTCHA_UNSOLVABLE подряд. Переходим к ручному решению."
                )
                return None
            time.sleep(3)
            continue
        if err_code:
            print(
                "💡 Совет: при ERROR_CAPTCHA_UNSOLVABLE используйте резидентный/мобильный прокси "
                "с IP из РФ/СНГ — VK часто блокирует датацентровые IP."
            )
        return None

    return None


# ---------------------------------------------------------------------------
#  Повтор VK-запроса с success_token после решения капчи
# ---------------------------------------------------------------------------
def _retry_with_success_token(
    method: str,
    params: dict,
    token: str,
    raw_captcha: dict,
    label: str = "",
) -> Optional[dict]:
    """Повторяет VK API запрос, добавляя success_token и remixstlid."""
    params_with_token = {**params, "success_token": token}
    if raw_captcha.get("remixstlid"):
        params_with_token["remixstlid"] = raw_captcha["remixstlid"]
    if raw_captcha.get("captcha_sid"):
        params_with_token["captcha_sid"] = raw_captcha["captcha_sid"]

    debug = {
        "owner_id": params_with_token.get("owner_id"),
        "post_id": params_with_token.get("post_id"),
        "comment_id": params_with_token.get("comment_id"),
        "has_success_token": True,
        "has_remixstlid": "remixstlid" in params_with_token,
        "has_captcha_sid": "captcha_sid" in params_with_token,
    }
    print(f"🔐 Повторяем {method} с success_token{' (' + label + ')' if label else ''}: {json.dumps(debug, ensure_ascii=False)}")

    try:
        return vk_session.method(method, params_with_token)
    except ApiError as e2:
        print(f"⚠️ VK запрос с success_token{' (' + label + ')' if label else ''} вернул ошибку: {e2}")
        return None


# ---------------------------------------------------------------------------
#  Попытка решить капчу: сначала ruCaptcha, потом ручной ввод
# ---------------------------------------------------------------------------
def _try_solve_captcha(
    method: str,
    params: dict,
    redirect_uri: str,
    raw_captcha: dict,
) -> Optional[dict]:
    """
    Пытается решить VKCaptcha:
    1) Автоматически через ruCaptcha VKCaptchaTask
    2) Вручную через VK ID Captcha SDK (фронтенд)
    Возвращает результат VK API или None.
    """
    # --- 1) ruCaptcha ---
    if redirect_uri:
        token = _solve_vkcaptcha_via_rucaptcha(redirect_uri)
        if token:
            result = _retry_with_success_token(method, params, token, raw_captcha, "ruCaptcha")
            if result is not None:
                print(f"✅ Капча решена через ruCaptcha, метод {method} выполнен успешно.")
                return result
        print("⚠️ ruCaptcha не смогла решить капчу или повторный запрос вернул ошибку.")

    # --- 2) Ручной ввод через VK ID Captcha SDK ---
    if redirect_uri:
        global _manual_redirect_uri
        _manual_redirect_uri = redirect_uri
        print(f"VKCAPTCHA_MANUAL_REDIRECT_URI: {redirect_uri}")
        print("⏳ Ожидание решения капчи пользователем через VK ID Captcha (до 3 минут)...")
        manual_token = _wait_for_manual_success_token(timeout=180.0)
        _manual_redirect_uri = None
        if manual_token:
            result = _retry_with_success_token(method, params, manual_token, raw_captcha, "manual")
            if result is not None:
                print(f"✅ Капча решена пользователем, метод {method} выполнен успешно.")
                return result

    print(
        "  ❌ Автоматическое и ручное решение VKCaptcha не удалось.\n"
        "     Откройте redirect_uri в браузере, решите капчу,\n"
        "     затем перезапустите обработку."
    )
    return None


# ---------------------------------------------------------------------------
#  safe_request — основной метод вызова VK API с обработкой капчи
# ---------------------------------------------------------------------------
def safe_request(method, **kwargs):
    """Выполняет запрос к VK API с retry, rate limiting и обработкой капчи."""
    if vk_session is None:
        raise RuntimeError("VK API не инициализирован. Вызовите init_vk_api().")

    global request_times
    delay = 0.34
    net_delay = 1.0

    while True:
        current_time = time.time()
        while request_times and request_times[0] < current_time - 60:
            request_times.popleft()

        if len(request_times) >= 180:
            sleep_time = (request_times[0] - (current_time - 60)) + 0.01
            print(f"⚠️  Достигнут лимит 180 запросов/мин, пауза {sleep_time:.2f} сек...")
            time.sleep(sleep_time)
            current_time = time.time()
            while request_times and request_times[0] < current_time - 60:
                request_times.popleft()

        request_times.append(current_time)

        try:
            return vk_session.method(method, kwargs)

        except (Captcha, ApiError) as e:
            raw_captcha = _pop_last_captcha_error()

            if isinstance(e, ApiError):
                code = getattr(e, "code", None)

                if code == 14:
                    print(
                        f"🔎 ApiError 14 (Captcha needed) — {method}, "
                        f"params: {json.dumps({'owner_id': kwargs.get('owner_id'), 'post_id': kwargs.get('post_id'), 'comment_id': kwargs.get('comment_id')}, ensure_ascii=False)}"
                    )

                    err = getattr(e, "error", {}) or {}
                    if isinstance(err, dict):
                        if not err.get("redirect_uri") and raw_captcha.get("redirect_uri"):
                            err["redirect_uri"] = raw_captcha["redirect_uri"]
                        if not err.get("remixstlid") and raw_captcha.get("remixstlid"):
                            err["remixstlid"] = raw_captcha["remixstlid"]

                    redirect_uri = (err.get("redirect_uri") or "").strip() if isinstance(err, dict) else ""
                    if not redirect_uri:
                        redirect_uri = (raw_captcha.get("redirect_uri") or "").strip()

                    print(f"🔎 redirect_uri: {redirect_uri or '(отсутствует)'}")

                    result = _try_solve_captcha(method, kwargs, redirect_uri, raw_captcha)
                    if result is not None:
                        return result
                    raise e

                if code == 6:
                    print(f"⚠️  Превышение лимита запросов, пауза {delay:.2f} сек...")
                    time.sleep(delay)
                    delay = min(delay * 2, 10)
                else:
                    raise e

            else:
                # Исключение Captcha (vk_api потерял redirect_uri)
                print(
                    f"🔎 Captcha exception — {method}, "
                    f"params: {json.dumps({'owner_id': kwargs.get('owner_id'), 'post_id': kwargs.get('post_id'), 'comment_id': kwargs.get('comment_id')}, ensure_ascii=False)}"
                )

                redirect_uri = (getattr(e, "redirect_uri", None) or "").strip()
                if not redirect_uri:
                    redirect_uri = (raw_captcha.get("redirect_uri") or "").strip()

                print(f"🔎 redirect_uri: {redirect_uri or '(отсутствует)'}")

                result = _try_solve_captcha(method, kwargs, redirect_uri, raw_captcha)
                if result is not None:
                    return result
                raise e

        except requests.exceptions.RequestException as e:
            print(f"⚠️  Сетевая ошибка при вызове {method}: {e}. Повтор через {net_delay:.1f} сек...")
            time.sleep(net_delay)
            net_delay = min(net_delay * 2, 20.0)


# ---------------------------------------------------------------------------
#  Вспомогательные функции — замена ссылок
# ---------------------------------------------------------------------------
def _captcha_error_to_json(e: Exception) -> str:
    """Форматирует ошибку капчи в JSON для отладки."""
    try:
        if isinstance(e, ApiError):
            err = getattr(e, "error", None)
            if isinstance(err, dict):
                return json.dumps({"error": err}, ensure_ascii=False, indent=2)
        if isinstance(e, Captcha):
            data = {
                "error_code": 14,
                "error_msg": "Captcha needed",
                "captcha_sid": getattr(e, "sid", None),
                "redirect_uri": getattr(e, "redirect_uri", None),
                "captcha_img": e.get_url() if hasattr(e, "get_url") else getattr(e, "url", None),
            }
            return json.dumps({"error": data}, ensure_ascii=False, indent=2)
        return json.dumps({"exception": str(e), "type": type(e).__name__}, ensure_ascii=False, indent=2)
    except Exception as fallback:
        return json.dumps({"exception": str(e), "fallback_error": str(fallback)}, ensure_ascii=False, indent=2)

def replace_in_text(text: str, old: str, new: str) -> str:
    """
    Заменяет старую ссылку (или префикс) на новую в тексте.

    Если old заканчивается на '/' — это префикс: заменяется вся ссылка
    от домена до пробела/конца строки (включая любой хвост после old).

    Если old не заканчивается на '/' — точная замена конкретной ссылки.

    В обоих случаях корректно убирает мусорные повторы https:// перед доменом
    (результат предыдущих некорректных запусков).
    """
    if not text or not old:
        return text
    new = (new or "").strip()
    if not new:
        return text

    old_stripped = old.strip()

    # Извлекаем домен+путь без протокола
    # "https://vk.com/" -> "vk.com/"
    # "https://vk.com/club123" -> "vk.com/club123"
    old_no_scheme = re.sub(r"^https?://", "", old_stripped, flags=re.IGNORECASE)
    if not old_no_scheme:
        return text

    old_is_prefix = old_stripped.endswith("/")
    escaped = re.escape(old_no_scheme)

    if old_is_prefix:
        # Префикс: захватываем (мусорные https://) + домен/путь + весь хвост до пробела
        # (?:https?://)* — ноль или более повторов протокола (мусор)
        # escaped        — домен+путь (например vk\.com/)
        # \S*            — хвост ссылки до пробела/конца
        pattern = r"(?:https?://)*" + escaped + r"\S*"
    else:
        # Точная ссылка: (мусорные https://) + домен+путь + опциональный /
        pattern = r"(?:https?://)*" + escaped + r"/?"

    result = re.sub(pattern, new, text, flags=re.IGNORECASE)
    return result

# ---------------------------------------------------------------------------
#  Разрешение owner_id сообщества
# ---------------------------------------------------------------------------
def resolve_owner_id(screen_name):
    """Преобразует короткое имя или ссылку сообщества в отрицательный owner_id."""
    original = screen_name.strip()

    if "/" in original and ("vk.com" in original or "vk.ru" in original):
        parts = original.split("/")
        screen_name = parts[-1] if parts else original
    else:
        parsed = urlparse(original)
        if parsed.netloc:
            screen_name = parsed.path.strip("/")
        else:
            screen_name = original

    if screen_name.startswith(("club", "public", "event")):
        match = re.search(r"\d+", screen_name)
        if match:
            return -int(match.group())

    if screen_name.startswith("id"):
        return int(screen_name[2:])

    try:
        print(f"Разрешаем screen_name: {screen_name}")
        result = safe_request("utils.resolveScreenName", screen_name=screen_name)
        if result and result.get("type") in ("group", "page", "event"):
            return -result["object_id"]
        else:
            print(f"⚠️  Не удалось определить ID для {screen_name}")
            return None
    except ApiError as e:
        print(f"⚠️  Ошибка при разрешении имени {screen_name}: {e}")
        return None


# ---------------------------------------------------------------------------
#  Редактирование постов и комментариев
# ---------------------------------------------------------------------------
def edit_post(owner_id, post_id, new_text, attachments=None):
    params = {
        "owner_id": owner_id,
        "post_id": post_id,
        "message": new_text,
        "from_group": 1,
    }
    if attachments:
        attach_str = ",".join(
            f"{a['type']}{a[a['type']]['owner_id']}_{a[a['type']]['id']}" for a in attachments
        )
        params["attachments"] = attach_str
    try:
        safe_request("wall.edit", **params)
        print(f"  ✅ Пост {post_id} успешно изменён.")
        return True
    except (ApiError, Captcha):
        try:
            safe_request("wall.edit", owner_id=owner_id, post_id=post_id, message=new_text)
            print(f"  ✅ Пост {post_id} успешно изменён (без from_group).")
            return True
        except (ApiError, Captcha) as e2:
            code = getattr(e2, "code", None)
            if code in (15, 100):
                print(f"    ⚠️  Нет прав редактировать пост {post_id} (VK ошибка {code}).")
            else:
                print(f"    ❌ Ошибка редактирования поста {post_id}: {e2}")
            return False


def _enable_comments_on_post(owner_id, post) -> bool:
    """Включает комментарии в посте. Возвращает True при успехе."""
    post_id = post.get("id")
    if not post_id:
        return False
    for use_from_group in (True, False):
        params = {
            "owner_id": owner_id,
            "post_id": post_id,
        }
        if use_from_group:
            params["from_group"] = 1
        try:
            safe_request("wall.openComments", **params)
            print(f"    📂 Комментарии в посте {post_id} открыты.")
            return True
        except (ApiError, Captcha) as e:
            if use_from_group:
                continue
            code = getattr(e, "code", None)
            if code in (15, 100):
                print(f"    ⚠️  Не удалось открыть комментарии в посте {post_id} (VK ошибка {code}).")
            else:
                print(f"    ⚠️  Ошибка при открытии комментариев: {e}")
            return False
    return False


# ---------------------------------------------------------------------------
#  Определение родительского поста для комментариев
# ---------------------------------------------------------------------------
def _resolve_comments_root(owner_id: int, post: dict) -> tuple[int, int, dict]:
    """
    Для поста-комментария (post_type='reply') возвращает owner_id/post_id/объект
    родительской записи. Для обычного поста — исходные owner_id и post.id.
    """
    root_owner_id = owner_id
    root_post_id = post.get("id")

    post_type = post.get("post_type") or post.get("type")
    parent_id = post.get("post_id")

    if post_type == "reply" and parent_id:
        root_post_id = parent_id

    root_post = dict(post or {})
    root_post["id"] = root_post_id

    return int(root_owner_id), int(root_post_id), root_post


def edit_comment(owner_id, comment_id, new_text, attachments=None, post=None):
    params = {
        "owner_id": owner_id,
        "comment_id": comment_id,
        "message": new_text,
    }
    if attachments:
        attach_str = ",".join(
            f"{a['type']}{a[a['type']]['owner_id']}_{a[a['type']]['id']}" for a in attachments
        )
        params["attachments"] = attach_str
    try:
        safe_request("wall.editComment", **params)
        print(f"    ✅ Комментарий {comment_id} успешно изменён.")
        return True
    except (ApiError, Captcha) as e:
        code = getattr(e, "code", None)
        if code == 15 and post:
            print(f"    📂 Комментарии закрыты, открываем в посте {post.get('id')}…")
            if _enable_comments_on_post(owner_id, post):
                time.sleep(0.5)
                try:
                    safe_request("wall.editComment", **params)
                    print(f"    ✅ Комментарий {comment_id} успешно изменён.")
                    return True
                except (ApiError, Captcha) as e2:
                    code2 = getattr(e2, "code", None)
                    if code2 in (15, 100):
                        print(f"    ⚠️  Нет прав редактировать комментарий {comment_id} (VK ошибка {code2}).")
                    else:
                        print(f"    ❌ Ошибка редактирования комментария {comment_id}: {e2}")
                    return False
        if code in (15, 100):
            print(f"    ⚠️  Нет прав редактировать комментарий {comment_id} (VK ошибка {code}).")
        else:
            print(f"    ❌ Ошибка редактирования комментария {comment_id}: {e}")
        return False


def _delete_and_recreate_comment(
    owner_id: int,
    post: dict,
    comment_id: int,
    new_text: str,
    attachments=None,
) -> bool:
    """Удалить старый комментарий и создать новый с новой ссылкой."""
    post_id = post.get("id")
    if not post_id:
        return False

    attach_str = None
    if attachments:
        attach_str = ",".join(
            f"{a['type']}{a[a['type']]['owner_id']}_{a[a['type']]['id']}" for a in attachments
        )

    try:
        safe_request("wall.deleteComment", owner_id=owner_id, comment_id=comment_id)
        print(f"    🗑 Комментарий {comment_id} удалён, создаём новый с новой ссылкой...")
    except (ApiError, Captcha) as e:
        print(f"    ❌ Не удалось удалить комментарий {comment_id}: {e}")
        return False

    try:
        params = {
            "owner_id": owner_id,
            "post_id": post_id,
            "message": new_text,
        }
        if attach_str:
            params["attachments"] = attach_str
        safe_request("wall.createComment", **params)
        print(f"    ✅ Новый комментарий с новой ссылкой создан вместо {comment_id}.")
        return True
    except (ApiError, Captcha) as e:
        print(f"    ❌ Не удалось создать новый комментарий вместо {comment_id}: {e}")
        return False


# ---------------------------------------------------------------------------
#  Обработка сообщества
# ---------------------------------------------------------------------------
def process_community(community_url, old_link, new_link, max_post_age_days: Optional[int] = None):
    owner_id = resolve_owner_id(community_url)
    if owner_id is None:
        print(f"❌ Пропускаем {community_url}: не удалось определить ID")
        return

    print(f"\n📌 Обрабатываем сообщество ID = {owner_id}")

    # Для wall.search берём минимальный запрос:
    # убираем протокол, для vk.com берём только хвост после домена
    search_query = old_link.strip()
    if search_query:
        search_query = re.sub(r"^https?://", "", search_query, flags=re.IGNORECASE)
        m = re.search(r"^(?:www\.)?(vk\.com|vk\.ru)/(.+)$", search_query, flags=re.IGNORECASE)
        if m:
            search_query = m.group(2)

    offset = 0
    total_edited_posts = 0
    now_ts = time.time()
    limit_ts = (now_ts - max_post_age_days * 86400) if max_post_age_days else None
    processed_comment_ids: set[int] = set()

    while True:
        try:
            resp = safe_request(
                "wall.search", owner_id=owner_id, query=search_query, count=100, offset=offset,
            )
            if not resp or "items" not in resp:
                print(f"  ⚠️ Неожиданный ответ от wall.search: {resp}")
                break
            items = resp["items"]
            if not items:
                if offset == 0:
                    print(f"  ⏺️ По ссылке ничего не найдено в {owner_id}.")
                break
        except (ApiError, Captcha) as e:
            error_code = getattr(e, "code", "неизвестный")
            print(f"❌ Ошибка wall.search в {owner_id}: код {error_code}: {e}")
            if error_code in [15, 30, 100, 1051]:
                print(f"   Сообщество {owner_id} недоступно.")
            break

        to_process = []
        for post in items:
            if limit_ts is not None:
                post_ts = post.get("date") or 0
                if post_ts < limit_ts:
                    continue
            to_process.append(post)

        for post in to_process:
            try:
                edited = _process_post_with_comments(
                    owner_id, post, old_link, new_link, processed_comment_ids
                )
                total_edited_posts += edited
            except Exception as e:
                print(f"  ⚠️ Ошибка при обработке поста: {e}")
                if isinstance(e, (ApiError, Captcha)):
                    print(f"  Полный ответ VK:\n{_captcha_error_to_json(e)}")
                raise e

        if len(items) < 100:
            break
        offset += 100
        time.sleep(0.5)

    print(f"  ✅ Всего отредактировано постов: {total_edited_posts}")


def _process_post_with_comments(
    owner_id, post, old_link, new_link, processed_comment_ids=None
) -> int:
    if processed_comment_ids is None:
        processed_comment_ids = set()

    post_id = post["id"]
    text = post.get("text", "")
    new_text = replace_in_text(text, old_link, new_link)
    edited = 0

    if new_text != text:
        attachments = post.get("attachments", [])
        print(f"  ✏️  Редактируем пост {post_id}...")
        if edit_post(owner_id, post_id, new_text, attachments):
            edited = 1
    else:
        print(f"  ⏭️  Пост {post_id} – текст не изменился")

    comments_owner_id, comments_post_id, comments_post = _resolve_comments_root(owner_id, post)
    process_comments_for_post(
        comments_owner_id, comments_post_id, comments_post,
        old_link, new_link, processed_comment_ids,
    )
    return edited


def process_comment(owner_id, post, comment, old_link, new_link):
    comment_id = comment["id"]
    text = comment.get("text", "")
    if not text:
        return 0

    # Проверяем наличие домена/пути старой ссылки в тексте
    old_no_scheme = re.sub(r"^https?://", "", old_link.strip(), flags=re.IGNORECASE)
    if old_no_scheme and old_no_scheme not in text:
        return 0

    new_text = replace_in_text(text, old_link, new_link)
    if new_text == text:
        return 0

    attachments = comment.get("attachments", [])
    print(f"    ✏️  Редактируем комментарий {comment_id}...")
    if edit_comment(owner_id, comment_id, new_text, attachments, post=post):
        return 1

    if _delete_and_recreate_comment(owner_id, post, comment_id, new_text, attachments):
        return 1
    return 0


def process_comments_for_post(
    owner_id, post_id, post, old_link, new_link, processed_comment_ids=None
):
    if processed_comment_ids is None:
        processed_comment_ids = set()
    offset = 0
    total_edited_comments = 0
    comments_closed_retried = False

    while True:
        try:
            comments = safe_request(
                "wall.getComments",
                owner_id=owner_id,
                post_id=post_id,
                count=100,
                offset=offset,
                need_likes=0,
                need_threads=1,
                thread_items=10,
            )
        except (ApiError, Captcha) as e:
            code = getattr(e, "code", None)
            if code == 15 and not comments_closed_retried:
                print(f"    📂 Комментарии закрыты, открываем в посте {post_id}…")
                if _enable_comments_on_post(owner_id, post):
                    comments_closed_retried = True
                    time.sleep(0.5)
                    continue
            if code in (15, 100):
                print(f"    ⚠️  Нет прав получать комментарии к посту {post_id} (VK ошибка {code}).")
            else:
                print(f"    ⚠️  Не удалось получить комментарии к посту {post_id}: {e}")
            break

        items = comments.get("items", [])
        if not items:
            break

        for comment in items:
            cid = comment["id"]
            if cid not in processed_comment_ids:
                processed_comment_ids.add(cid)
                total_edited_comments += process_comment(
                    owner_id, post, comment, old_link, new_link
                )
            time.sleep(0.34)

            if "thread" in comment:
                for thread_comment in comment["thread"].get("items", []):
                    tcid = thread_comment["id"]
                    if tcid not in processed_comment_ids:
                        processed_comment_ids.add(tcid)
                        total_edited_comments += process_comment(
                            owner_id, post, thread_comment, old_link, new_link
                        )
                    time.sleep(0.34)

        if len(items) < 100:
            break
        offset += 100
        time.sleep(0.34)

    if total_edited_comments:
        print(f"    ✅ Комментариев отредактировано: {total_edited_comments}")


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------
def main():
    print("🔄 Массовая замена ссылок в постах и комментариях ВК")

    global VK_TOKEN
    if not VK_TOKEN:
        VK_TOKEN = input("Введите VK_TOKEN (или задайте в .env): ").strip() or None

    try:
        init_vk_api()
    except Exception as e:
        print(f"❌ Ошибка инициализации VK API: {e}")
        return

    old_link = input("Введите ссылку, которую нужно заменить: ").strip()
    new_link = input("Введите новую ссылку: ").strip()
    print("Введите ссылки на сообщества (по одной в строке, пустая строка — конец):")
    communities = []
    while True:
        line = input().strip()
        if not line:
            break
        communities.append(line)

    if not communities:
        print("❌ Не указано ни одного сообщества.")
        return

    print(f"\n🔍 Начинаем обработку {len(communities)} сообществ...")
    for comm in communities:
        process_community(comm, old_link, new_link)
        time.sleep(1)

    print("\n🎉 Работа завершена!")



if __name__ == "__main__":
    main()