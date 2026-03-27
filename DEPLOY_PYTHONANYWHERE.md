# Деплой на PythonAnywhere (zakcheks.pythonanywhere.com)

## 1. Загрузить код на PythonAnywhere

**Вариант A — через Git (если репозиторий на GitHub):**
- В панели PythonAnywhere: **Consoles** → **Bash**.
- Выполни:
  ```bash
  cd ~
  git clone https://github.com/ВАШ_ЛОГИН/vk_public_rewriter.git
  cd vk_public_rewriter
  ```
  (замени URL на свой репозиторий)

**Вариант B — загрузка файлов вручную:**
- **Files** → перейди в `/home/zakcheks/`.
- Создай папку, например `vk_public_rewriter`.
- Загрузи в неё файлы проекта:
  - `app.py`
  - `vk_link_rewriter.py`
  - `requirements.txt`
  - папку `templates/` (с `index.html`)

Не загружай папки `venv`, `web`, `.github` — на сервере они не нужны.

---

## 2. Виртуальное окружение и зависимости

В **Bash** (в папке проекта):

```bash
cd /home/zakcheks/vk_public_rewriter   # или как назвал папку

python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install flask flask-cors vk-api python-dotenv
```

На PythonAnywhere **PyQt6** не нужен (он для десктопного GUI). Если в `requirements.txt` есть PyQt6, можно не ставить его в venv.

---

## 3. Настроить WSGI

Открой файл:

**https://www.pythonanywhere.com/user/zakcheks/files/var/www/zakcheks_pythonanywhere_com_wsgi.py**

Замени его содержимое на:

```python
import sys

project_home = '/home/zakcheks/vk_public_rewriter'
if project_home not in sys.path:
    sys.path.insert(0, project_home)

from app import app as application
```

Сохрани файл. Virtualenv подхватится сам, если ты указал его во вкладке **Web** (шаг 4). Если папка проекта у тебя не `vk_public_rewriter`, замени путь в `project_home`.

---

## 4. Web-приложение в панели

- Открой вкладку **Web**.
- В блоке **Code** укажи:
  - **Source code:** `/home/zakcheks/vk_public_rewriter`
  - **WSGI configuration file:** `/var/www/zakcheks_pythonanywhere_com_wsgi.py`
- В блоке **Virtualenv** укажи: `/home/zakcheks/vk_public_rewriter/venv`
- Нажми **Reload** (зелёная кнопка) у своего домена.

---

## 5. Проверка

Открой в браузере:

- `https://zakcheks.pythonanywhere.com/` — должна открыться главная страница VK Link Rewriter.

Если видишь ошибку 500:
- На вкладке **Web** открой **Error log** и посмотри текст ошибки.
- В **Bash** проверь: `python3 -c "from app import app"` — не должно быть исключений.

---

## 6. Ограничения бесплатного аккаунта

- Приложение может «засыпать» при неактивности.
- Исходящие запросы к VK API разрешены; если появятся ограничения — смотри раздел **Whitelist** на вкладке **Web** и добавь нужные домены (например, `api.vk.com`).
- Для долгих операций (массовая замена) таймаут запроса может обрезать ответ; при необходимости рассмотри платный план с большим таймаутом.

---

## Краткий чеклист

| Шаг | Действие |
|-----|----------|
| 1 | Загрузить код в `/home/zakcheks/...` |
| 2 | Создать venv и установить зависимости |
| 3 | Вставить в `zakcheks_pythonanywhere_com_wsgi.py` код выше |
| 4 | В Web указать путь к коду, WSGI и venv |
| 5 | Нажать Reload и открыть сайт |
