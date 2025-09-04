from __future__ import annotations

import asyncio
import logging
from collections import deque
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Dict
from urllib.parse import urlparse

from db.crud import RestDB
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from http import HTTPStatus
from pydantic import BaseModel
from pymax import MaxClient

# --- App and logging ---

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent

logger = logging.getLogger("maxrest")
logging.basicConfig(level=logging.DEBUG)

app = FastAPI(title="Max REST API", version="0.2.0")


# --- Client manager ---

class ClientManager:
    """Менеджер клиентов MaxClient с кэшированием рабочих директорий и переподключением."""

    def __init__(self, db: RestDB, cache_base: Path) -> None:
        self._clients: Dict[str, MaxClient] = {}
        self._db = db
        self._cache_base = cache_base
        self._conn_locks: Dict[str, asyncio.Lock] = {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        self._max_concurrency_per_client: int = 32

    def get(self, phone: str) -> MaxClient:
        client = self._clients.get(phone)
        if client is None:
            raise HTTPException(status_code=404, detail="Клиент для номера не подключён")
        return client

    def get_or_create(self, phone: str, uri: str | None = None, work_dir: str | None = None) -> MaxClient:
        client = self._clients.get(phone)
        if client:
            return client

        session_dir = self._db.get_or_create_session_dir(phone, self._cache_base)
        kwargs: dict[str, Any] = {
            "phone": phone,
            "work_dir": work_dir or str(session_dir.resolve()),
        }
        if uri:
            kwargs["uri"] = uri
        client = MaxClient(**kwargs)
        self._clients[phone] = client
        self._conn_locks[phone] = asyncio.Lock()
        self._semaphores[phone] = asyncio.Semaphore(self._max_concurrency_per_client)
        return client

    async def ensure_connected(self, client: MaxClient) -> None:
        """Гарантирует активное подключение клиента либо выполняет переподключение и синхронизацию."""
        phone = client.phone
        lock = self._conn_locks.get(phone) or asyncio.Lock()
        self._conn_locks[phone] = lock
        async with lock:
            try:
                ws = getattr(client, "_ws", None)
                is_connected = bool(getattr(client, "is_connected", False))
                ws_closed = bool(getattr(ws, "closed", True)) if ws is not None else True
                if is_connected and ws is not None and not ws_closed:
                    return
            except Exception:
                pass

            try:
                await client.close()
            except Exception:
                logger.warning("Error during client.close() before reconnect", exc_info=True)

            _ = await client._connect(client.user_agent)
            try:
                if getattr(client, "_token", None):
                    await client._sync()
            except Exception:
                logger.warning("Sync after reconnect failed", exc_info=True)

    async def disconnect_all(self) -> None:
        tasks = [asyncio.create_task(c.close()) for c in list(self._clients.values())]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def call(self, phone: str, runner: Callable[[MaxClient], Awaitable[Any]]) -> Any:
        """Ограничивает параллелизм запросов на клиента, выполняет runner в семафоре."""
        sem = self._semaphores.get(phone)
        if sem is None:
            sem = asyncio.Semaphore(self._max_concurrency_per_client)
            self._semaphores[phone] = sem
        async with sem:
            client = self.get(phone)
            return await runner(client)


REST_DB = RestDB(PROJECT_ROOT / "restcache.db")
REST_CACHE_BASE = PROJECT_ROOT / "restcache"
CLIENTS = ClientManager(REST_DB, REST_CACHE_BASE)

INCOMING_MESSAGES: Dict[str, Deque[dict[str, Any]]] = {}

# --- Schemas ---

class ConnectRequest(BaseModel):
    """Подключение клиента и, при наличии токена, первичная синхронизация."""
    phone: str
    uri: str | None = None
    work_dir: str | None = None


class StatusRequest(BaseModel):
    """Статус клиента по номеру телефона."""
    phone: str


class RequestCodeRequest(BaseModel):
    """Запрос временного токена для подтверждения кода авторизации."""
    phone: str
    language: str | None = "ru"


class SendCodeRequest(BaseModel):
    """Подтверждение кода и сохранение постоянного токена авторизации."""
    phone: str
    code: str
    token: str


class SendMessageRequest(BaseModel):
    """Отправка сообщения в чат/диалог."""
    phone: str
    chat_id: int
    text: str
    notify: bool = False


class EditMessageRequest(BaseModel):
    """Редактирование отправленного сообщения."""
    phone: str
    chat_id: int
    message_id: int
    text: str


class DeleteMessageRequest(BaseModel):
    """Удаление одного или нескольких сообщений."""
    phone: str
    chat_id: int
    message_id: int | None = None
    message_ids: list[int] | None = None
    for_me: bool = False


class FetchHistoryRequest(BaseModel):
    """Получение истории чата."""
    phone: str
    chat_id: int
    from_time: int | None = None
    forward: int = 0
    backward: int = 200


class GetUsersRequest(BaseModel):
    """Получение информации о нескольких пользователях по их ID."""
    phone: str
    user_ids: list[int]


class GetUserRequest(BaseModel):
    """Получение информации об одном пользователе по его ID."""
    phone: str
    user_id: int


class GetCachedUserRequest(BaseModel):
    """Получение пользователя только из локального кеша клиента."""
    phone: str
    user_id: int


class HandleMessageRequest(BaseModel):
    """Регистрация обработчика входящих сообщений для клиента."""
    phone: str
    webhook: str | None = None


class GetMessagesRequest(BaseModel):
    """Получение последних входящих сообщений из буфера."""
    phone: str
    limit: int = 50


def _serialize_message(msg: Any) -> dict[str, Any]:
    """Сериализация объекта сообщения в JSON-совместимый словарь."""
    return {
        "id": getattr(msg, "id", None),
        "sender": getattr(msg, "sender", None),
        "text": getattr(msg, "text", None),
        "time": getattr(msg, "time", None),
        "status": getattr(msg, "status", None),
        "type": getattr(msg, "type", None),
    }


def _serialize_user(user: Any) -> dict[str, Any]:
    """Сериализация объекта пользователя."""
    names: list[str] = []
    try:
        raw = getattr(user, "names", [])
        names = [str(n) for n in raw] if isinstance(raw, list) else []
    except Exception:
        names = []
    return {
        "id": getattr(user, "id", None),
        "names": names,
    }


def _serialize_dialog(d: Any) -> dict[str, Any]:
    """Сериализация диалога для ответа API."""
    return {
        "id": getattr(d, "id", None),
        "type": getattr(d, "type", None),
        "owner": getattr(d, "owner", None),
        "last_message": _serialize_message(getattr(d, "last_message", None)) if getattr(d, "last_message", None) else None,
    }


def _serialize_chat(c: Any) -> dict[str, Any]:
    """Сериализация группового чата/канала для ответа API."""
    return {
        "id": getattr(c, "id", None),
        "type": getattr(c, "type", None),
        "owner": getattr(c, "owner", None),
        "last_message": _serialize_message(getattr(c, "last_message", None)) if getattr(c, "last_message", None) else None,
    }


def _validate_webhook_url(url: str) -> bool:
    """Базовая проверка URL вебхука: только http/https и наличие хоста."""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        if not parsed.netloc:
            return False
        if len(url) > 2048:
            return False
        return True
    except Exception:
        return False


def _make_response(data: Any | None = None, code: int = 200) -> JSONResponse:
    """Возвращает JSON-ответ единого формата с корректным HTTP-кодом.

    Формат: {"code": <int>, "status": <HTTP phrase>, "data": <payload?>}
    """
    try:
        phrase = HTTPStatus(code).phrase
    except Exception:
        phrase = "OK" if 200 <= code < 300 else "Error"
    body: dict[str, Any] = {"code": code, "status": phrase}
    if data is not None:
        body["data"] = data
    return JSONResponse(content=body, status_code=code)

@app.get("/health")
async def health() -> dict[str, Any]:
    """Проверка доступности сервиса."""
    return _make_response()


@app.post("/status")
async def status(req: StatusRequest) -> dict[str, Any]:
    """Текущий статус подключения клиента и факт наличия токена."""
    try:
        client = CLIENTS.get(req.phone)
    except HTTPException:
        return {"connected": False}
    try:
        ws = getattr(client, "_ws", None)
        connected = bool(getattr(client, "is_connected", False)) and ws is not None and not getattr(ws, "closed", True)
    except Exception:
        connected = False
    return _make_response({
        "connected": connected,
        "has_token": bool(getattr(client, "_token", None)),
    })


@app.post("/connect")
async def connect(req: ConnectRequest) -> dict[str, Any]:
    """Подключение к WebSocket и первичная синхронизация при наличии токена."""
    client = CLIENTS.get_or_create(req.phone, uri=req.uri, work_dir=req.work_dir)
    try:
        handshake = await CLIENTS.call(req.phone, lambda c: c._connect(c.user_agent))
        try:
            if getattr(client, "_token", None):
                await CLIENTS.call(req.phone, lambda c: c._sync())
        except Exception:
            logger.warning("Initial sync after connect failed", exc_info=True)
        return _make_response({
            "handshake": handshake,
            "has_token": bool(getattr(client, "_token", None)),
        })
    except Exception as e:
        logger.exception("Connect failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/disconnect")
async def disconnect(req: StatusRequest) -> dict[str, Any]:
    try:
        await CLIENTS.call(req.phone, lambda c: c.close())
        return _make_response(code=200)
    except Exception as e:
        logger.exception("Disconnect failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/request_code")
async def request_code(req: RequestCodeRequest) -> dict[str, Any]:
    """Запрос временного токена и отправки кода подтверждения на телефон."""
    client = CLIENTS.get(req.phone)
    try:
        if getattr(client, "_token", None):
            raise HTTPException(status_code=409, detail="Сессия уже авторизована; код не требуется")
        payload = await CLIENTS.call(req.phone, lambda c: c._request_code(req.phone, language=req.language or "ru"))
        return _make_response(payload)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Request code failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/send_code")
async def send_code(req: SendCodeRequest) -> dict[str, Any]:
    """Подтверждение кода и сохранение постоянного токена в локальной БД клиента."""
    client = CLIENTS.get(req.phone)
    try:
        payload = await CLIENTS.call(req.phone, lambda c: c._send_code(req.code, req.token))
        token_attrs = payload.get("tokenAttrs", {}) if isinstance(payload, dict) else {}
        login = token_attrs.get("LOGIN", {}) if isinstance(token_attrs, dict) else {}
        auth_token = login.get("token") if isinstance(login, dict) else None
        if auth_token:
            client._token = auth_token
            client._database.update_auth_token(client._device_id, client._token)
        try:
            await CLIENTS.call(req.phone, lambda c: c._sync())
        except Exception:
            logger.warning("Sync after login failed", exc_info=True)
        return _make_response({"token_saved": bool(auth_token), "payload": payload})
    except Exception as e:
        logger.exception("Send code failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync")
async def sync(req: StatusRequest) -> dict[str, Any]:
    """Принудительная синхронизация состояния клиента с сервером."""
    client = CLIENTS.get(req.phone)
    try:
        await CLIENTS.ensure_connected(client)
        await CLIENTS.call(req.phone, lambda c: c._sync())
        return _make_response()
    except Exception as e:
        logger.exception("Sync failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/send_message")
async def send_message(req: SendMessageRequest) -> dict[str, Any]:
    """Отправка текстового сообщения в указанный чат."""
    client = CLIENTS.get(req.phone)
    try:
        await CLIENTS.ensure_connected(client)
        try:
            msg = await CLIENTS.call(req.phone, lambda c: c.send_message(text=req.text, chat_id=req.chat_id, notify=req.notify))
        except Exception:
            await CLIENTS.ensure_connected(client)
            msg = await CLIENTS.call(req.phone, lambda c: c.send_message(text=req.text, chat_id=req.chat_id, notify=req.notify))
        if msg is None:
            raise RuntimeError("Message not sent")
        return _make_response({"message": _serialize_message(msg)})
    except Exception as e:
        logger.exception("Send message failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/edit_message")
async def edit_message(req: EditMessageRequest) -> dict[str, Any]:
    """Редактирование текста сообщения по его идентификатору."""
    client = CLIENTS.get(req.phone)
    try:
        await CLIENTS.ensure_connected(client)
        try:
            msg = await CLIENTS.call(req.phone, lambda c: c.edit_message(req.chat_id, req.message_id, req.text))
        except Exception:
            await CLIENTS.ensure_connected(client)
            msg = await CLIENTS.call(req.phone, lambda c: c.edit_message(req.chat_id, req.message_id, req.text))
        if msg is None:
            raise RuntimeError("Edit message failed")
        return _make_response({"message": _serialize_message(msg)})
    except Exception as e:
        logger.exception("Edit message failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delete_message")
async def delete_message(req: DeleteMessageRequest) -> dict[str, Any]:
    """Удаление одного/нескольких сообщений. Поддерживает флаг удаления только для себя."""
    client = CLIENTS.get(req.phone)
    try:
        await CLIENTS.ensure_connected(client)
        ids: list[int] = []
        if isinstance(req.message_ids, list) and req.message_ids:
            ids = req.message_ids
        elif isinstance(req.message_id, int):
            ids = [req.message_id]
        if not ids:
            raise HTTPException(status_code=422, detail="Укажите message_id или message_ids")

        try:
            ok = await CLIENTS.call(req.phone, lambda c: c.delete_message(req.chat_id, ids, req.for_me))
        except Exception:
            await CLIENTS.ensure_connected(client)
            ok = await CLIENTS.call(req.phone, lambda c: c.delete_message(req.chat_id, ids, req.for_me))
        if not ok:
            raise HTTPException(status_code=400, detail="Удаление отклонено сервером")
        return _make_response()
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Delete message failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/fetch_history")
async def fetch_history(req: FetchHistoryRequest) -> dict[str, Any]:
    """Получение последних сообщений в чате относительно отметки времени."""
    client = CLIENTS.get(req.phone)
    try:
        await CLIENTS.ensure_connected(client)
        try:
            messages = await CLIENTS.call(req.phone, lambda c: c.fetch_history(chat_id=req.chat_id, from_time=req.from_time, forward=req.forward, backward=req.backward))
        except Exception:
            await CLIENTS.ensure_connected(client)
            messages = await CLIENTS.call(req.phone, lambda c: c.fetch_history(chat_id=req.chat_id, from_time=req.from_time, forward=req.forward, backward=req.backward))
        return _make_response({"messages": [_serialize_message(m) for m in (messages or [])]})
    except Exception as e:
        logger.exception("Fetch history failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get_users")
async def get_users(req: GetUsersRequest) -> dict[str, Any]:
    """Получение списка пользователей с кешированием на стороне клиента."""
    client = CLIENTS.get(req.phone)
    try:
        await CLIENTS.ensure_connected(client)
        try:
            users = await CLIENTS.call(req.phone, lambda c: c.get_users(req.user_ids))
        except Exception:
            await CLIENTS.ensure_connected(client)
            users = await CLIENTS.call(req.phone, lambda c: c.get_users(req.user_ids))
        return _make_response({"users": [_serialize_user(u) for u in users]})
    except Exception as e:
        logger.exception("Get users failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get_user")
async def get_user(req: GetUserRequest) -> dict[str, Any]:
    """Получение информации о пользователе по ID (с кешированием)."""
    client = CLIENTS.get(req.phone)
    try:
        await CLIENTS.ensure_connected(client)
        try:
            user = await CLIENTS.call(req.phone, lambda c: c.get_user(req.user_id))
        except Exception:
            await CLIENTS.ensure_connected(client)
            user = await CLIENTS.call(req.phone, lambda c: c.get_user(req.user_id))
        return _make_response({"user": _serialize_user(user) if user else None})
    except Exception as e:
        logger.exception("Get user failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get_cached_user")
async def get_cached_user(req: GetCachedUserRequest) -> dict[str, Any]:
    """Получение пользователя только из локального кеша клиента."""
    client = CLIENTS.get(req.phone)
    try:
        user = client.get_cached_user(req.user_id)
        return _make_response({"user": _serialize_user(user) if user else None})
    except Exception as e:
        logger.exception("Get cached user failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/handle_message")
async def handle_message(req: HandleMessageRequest) -> dict[str, Any]:
    """Регистрация обработчика `on_message` и запуск буфера входящих сообщений.

    При указании `webhook` (http/https) события отправляются POST-запросом в best-effort режиме.
    """
    client = CLIENTS.get(req.phone)
    if req.phone not in INCOMING_MESSAGES:
        INCOMING_MESSAGES[req.phone] = deque(maxlen=1000)

    if req.webhook and not _validate_webhook_url(req.webhook):
        raise HTTPException(status_code=422, detail="Некорректный URL вебхука")

    async def _maybe_post_webhook(payload: dict[str, Any]) -> None:
        if not req.webhook:
            return
        try:
            import httpx # ДА импорты не в начала и мне всё равно 

            async with httpx.AsyncClient(timeout=5) as hc:
                await hc.post(req.webhook, json=payload)
        except Exception:
            logger.warning("Webhook post failed", exc_info=True)

    @client.on_message
    async def _on_message(msg: Any) -> None:
        try:
            payload = _serialize_message(msg)
            INCOMING_MESSAGES[req.phone].append(payload)
            await _maybe_post_webhook({"phone": req.phone, "message": payload})
        except Exception:
            logger.exception("on_message handler error")

    # гарантируем подключение, чтобы хендлер начал получать события
    try:
        await CLIENTS.ensure_connected(client)
    except Exception as e:
        logger.exception("ensure_connected for handle_message failed")
        raise HTTPException(status_code=500, detail=str(e))

    return _make_response()


@app.post("/get_messages")
async def get_messages(req: GetMessagesRequest) -> dict[str, Any]:
    """Возвращает последние N входящих сообщений из буфера."""
    items = INCOMING_MESSAGES.get(req.phone)
    if not items:
        return _make_response({"messages": []})
    limit = max(1, min(req.limit, 500))
    out = list(items)[-limit:]
    return _make_response({"messages": out})


@app.post("/list_dialogs")
async def list_dialogs(req: StatusRequest) -> dict[str, Any]:
    """Список диалогов клиента (при необходимости выполняется синхронизация)."""
    client = CLIENTS.get(req.phone)
    await CLIENTS.ensure_connected(client)
    try:
        if not getattr(client, "dialogs", None):
            await client._sync()
        return _make_response({"dialogs": [_serialize_dialog(d) for d in getattr(client, "dialogs", [])]})
    except Exception as e:
        logger.exception("List dialogs failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/list_chats")
async def list_chats(req: StatusRequest) -> dict[str, Any]:
    """Список групповых чатов клиента."""
    client = CLIENTS.get(req.phone)
    await CLIENTS.ensure_connected(client)
    try:
        if not getattr(client, "chats", None):
            await client._sync()
        return _make_response({"chats": [_serialize_chat(c) for c in getattr(client, "chats", [])]})
    except Exception as e:
        logger.exception("List chats failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/list_channels")
async def list_channels(req: StatusRequest) -> dict[str, Any]:
    """Список каналов клиента."""
    client = CLIENTS.get(req.phone)
    await CLIENTS.ensure_connected(client)
    try:
        if not getattr(client, "channels", None):
            await client._sync()
        return _make_response({"channels": [_serialize_chat(c) for c in getattr(client, "channels", [])]})
    except Exception as e:
        logger.exception("List channels failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Корректное закрытие всех активных клиентов при завершении приложения."""
    await CLIENTS.disconnect_all()
