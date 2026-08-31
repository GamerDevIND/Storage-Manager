import os
import threading
from datetime import datetime
import asyncio
try:
    import aiofiles
    aiof = True
except ImportError:
    aiof = False
    pass

from dataclasses import dataclass, field
import inspect
from typing import Callable, Awaitable, Coroutine

class Logger:
    log_dir: str = os.path.join(".", "logs")
    log_file: str = os.path.join(log_dir, "log.log")
    
    _lock_sync = threading.RLock()
    _lock_async: asyncio.Lock | None = None
    with _lock_sync:
        if not os.path.exists(log_file): 
            with open(log_file, "w") as f:
                f.write("") 

    @classmethod
    def _get_async_lock(cls, clear_log_file = False) -> asyncio.Lock:
        if cls._lock_async is None:
            cls._lock_async = asyncio.Lock()
        if clear_log_file:
            with cls._lock_sync:
                with open(cls.log_file, "w") as f:
                    f.write("") 

        return cls._lock_async

    @classmethod
    def _format_message(cls, message: str, level: str) -> str:
        timestamp = datetime.now().strftime("%H:%M:%S - %d/%m/%Y")
        emoji: str = {
            "info": "ℹ️",
            "warn": "⚠️",
            "error": "🟥",
            "success": "✅",
            'debug': "🛠",
        }.get(level.lower().strip(), "📋")
        return f"{emoji} [{level.upper().strip()}] {message} - [{timestamp}]\n"

    @classmethod
    def _write_file_sync(cls, text: str, append: bool) -> None:
        os.makedirs(cls.log_dir, exist_ok=True)
        mode = "a" if append else "w"
        with open(cls.log_file, mode, encoding="utf-8") as f:
            f.write(text)

    @classmethod
    def log_sync(cls, message: str, level: str, append: bool = True, stdout: bool = True, save_to_file: bool = True) -> None:
        formatted = cls._format_message(message.strip(), level)
        with cls._lock_sync:
            if stdout:
                print(formatted)
            if save_to_file:
                cls._write_file_sync(formatted, append)

    @classmethod
    async def log_async(cls, message: str, level: str, append: bool = True, stdout: bool = True, save_to_file: bool = True) -> None:
        formatted = cls._format_message(message.strip(), level)
        async with cls._get_async_lock():
            if stdout:
                print(formatted)
            if not aiof:
                cls._write_file_sync(formatted, append)
            if save_to_file:
                os.makedirs(cls.log_dir, exist_ok=True)
                mode = "a" if append else "w"
                async with aiofiles.open(cls.log_file, mode, encoding="utf-8") as f:
                    await f.write(formatted)

    @classmethod
    def debug_sync(cls, *msg):
        cls.log_sync(" ".join(msg), "debug")

    @classmethod
    async def debug_async(cls, *msg):
        await cls.log_async(" ".join(msg), "debug")

    @classmethod
    def info_sync(cls, *msg: str) -> None:
        cls.log_sync(" ".join(msg), "info")

    @classmethod
    async def info_async(cls, *msg: str) -> None:
        await cls.log_async(" ".join(msg), "info")

    @classmethod
    def error_sync(cls, *msg: str) -> None:
        cls.log_sync(" ".join(msg), "error")

    @classmethod
    async def error_async(cls, *msg: str) -> None:
        await cls.log_async(" ".join(msg), "error")

    @classmethod
    def success_sync(cls, *msg: str) -> None:
        cls.log_sync(" ".join(msg), "success")

    @classmethod
    async def success_async(cls, *msg: str) -> None:
        await cls.log_async(" ".join(msg), "success")

    @classmethod
    def warn_sync(cls, *msg: str) -> None:
        cls.log_sync(" ".join(msg), "warn")

    @classmethod
    async def warn_async(cls, *msg: str) -> None:
        await cls.log_async(" ".join(msg), "warn")
@dataclass
class File:
    file_name: str
    size: float | int
    file_hash: str
    chunks: list[Blob] | None = field(default_factory=list)
    file_path: str | None = None
    metadata: dict = field(default_factory=dict)
    index_status: str = "PENDING"

@dataclass
class Blob:
    blob_hash: str
    size: float | int
    compressed_size: int | float
    compression_type: int
    ref_count: int = 0
    post_processed_hash: str | None = None
    metadata: dict = field(default_factory=dict)
    offset:int = 0
    data: bytes | None = None

@dataclass
class FileChunk:
    file_hash:str
    chunk_hash: str
    chunk_size:int | float
    chunk_offset: int | float
    chunk_idx: int
    chunk_data:bytes
    exact_chunk_data: None | bytes = None


async def call_async(func:Callable | Coroutine | Awaitable, *args, **kwargs):
    if inspect.isawaitable(func):
        return await func
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    elif callable(func):
        result = func(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result
    else:
        return func
