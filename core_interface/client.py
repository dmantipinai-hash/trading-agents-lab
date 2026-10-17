#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""client.py — клиент Unix-сокета демона Trade Core (Фаза 2b, cutover v3).

Протокол v0.1: одно соединение = один запрос = один ответ (JSON + \\n).
hello выдаёт fencing-токен (строгая сверка identity), intent требует
токен. Каждый вызов открывает своё соединение — бот живёт по 50 минут,
сессий не держим, head-of-line блок невозможен.

Используется bot_v3 (ветка TRADE_CORE_DAEMON_SOCK) и проверками живости.
Ничего не знает о бирже и ключах.
"""
from __future__ import annotations

import json
import socket


class DaemonUnavailable(RuntimeError):
    """Демон недоступен (нет сокета / таймаут / битый ответ)."""


def _request(sock_path: str, payload: dict, timeout: float = 15.0) -> dict:
    """Один запрос-ответ по Unix-сокету. Любой сбой -> DaemonUnavailable."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect(sock_path)
            s.sendall(json.dumps(payload, ensure_ascii=False).encode("utf-8")
                      + b"\n")
            buf = b""
            while b"\n" not in buf:
                data = s.recv(65536)
                if not data:
                    break
                buf += data
            if not buf:
                raise DaemonUnavailable("пустой ответ демона")
            line = buf.split(b"\n", 1)[0]
            return json.loads(line.decode("utf-8"))
    except DaemonUnavailable:
        raise
    except (OSError, ValueError) as e:
        raise DaemonUnavailable(f"{type(e).__name__}: {e}") from e


def hello(sock_path: str, bot_id: str, environment: str,
          account_id: str, timeout: float = 5.0) -> dict:
    """Представиться демону; ok=False -> {ok, error, detail} (IDENTITY_*)."""
    return _request(sock_path, {
        "op": "hello",
        "identity": {"bot_id": bot_id, "environment": environment,
                     "account_id": account_id},
    }, timeout=timeout)


def intent(sock_path: str, intent_v2: dict, fencing_token: int,
           timeout: float = 30.0) -> dict:
    """Отправить StrategyIntent.v2 с fencing-токеном от hello."""
    return _request(sock_path, {
        "op": "intent",
        "intent": intent_v2,
        "fencing_token": fencing_token,
    }, timeout=timeout)


def query(sock_path: str, client_order_id: str, timeout: float = 10.0) -> dict:
    return _request(sock_path, {"op": "query",
                                "client_order_id": client_order_id},
                    timeout=timeout)


def cancel(sock_path: str, client_order_id: str, fencing_token: int,
           timeout: float = 10.0) -> dict:
    return _request(sock_path, {"op": "cancel",
                                "client_order_id": client_order_id,
                                "fencing_token": fencing_token},
                    timeout=timeout)


def status(sock_path: str, timeout: float = 5.0) -> dict:
    """Снимок состояния демона (supervisor-status.v1)."""
    return _request(sock_path, {"op": "status"}, timeout=timeout)


def events(sock_path: str, since_seq: int = 0, timeout: float = 10.0) -> list:
    """Exchange-события после seq (reconciliation)."""
    r = _request(sock_path, {"op": "events", "since_seq": since_seq},
                 timeout=timeout)
    return r.get("events", [])
