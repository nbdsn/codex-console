"""
Telegram Bot 管理器
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Set

from ..config.settings import get_settings
from ..database.models import Account
from ..database.session import get_db

logger = logging.getLogger(__name__)


class TelegramBotManager:
    """负责 Telegram 轮询、命令处理与批量注册联动。"""

    def __init__(self) -> None:
        self._runner_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._update_offset: int = 0
        self._active_batch_ids: Set[str] = set()
        self._batch_watchers: Dict[str, asyncio.Task] = {}

    async def start(self) -> None:
        """启动 TG 机器人（配置完整时）。"""
        if self._running:
            return

        settings = get_settings()
        token = settings.telegram_bot_token.get_secret_value() if settings.telegram_bot_token else ""
        admin_id = settings.telegram_admin_id.strip() if settings.telegram_admin_id else ""
        if not token or not admin_id:
            logger.info("Telegram Bot 未启动：未配置 token 或管理员 ID")
            return

        self._running = True
        self._runner_task = asyncio.create_task(self._poll_loop())
        logger.info("Telegram Bot 已启动")

    async def stop(self) -> None:
        """停止 TG 机器人。"""
        self._running = False
        if self._runner_task:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
            self._runner_task = None

        for watcher in self._batch_watchers.values():
            watcher.cancel()
        self._batch_watchers.clear()
        self._active_batch_ids.clear()

    async def reload(self) -> None:
        """重载配置并重启 TG 机器人。"""
        await self.stop()
        await self.start()

    @staticmethod
    def _api_url(token: str, method: str) -> str:
        return f"https://api.telegram.org/bot{token}/{method}"

    def _request(self, token: str, method: str, payload: Optional[dict] = None) -> Dict[str, Any]:
        url = self._api_url(token, method)
        headers = {"Content-Type": "application/json"}
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=35) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)

    def _request_get(self, token: str, method: str, params: Optional[dict] = None) -> Dict[str, Any]:
        query = f"?{urllib.parse.urlencode(params or {})}" if params else ""
        url = f"{self._api_url(token, method)}{query}"
        with urllib.request.urlopen(url, timeout=40) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)

    async def _safe_request(self, token: str, method: str, payload: Optional[dict] = None) -> Dict[str, Any]:
        return await asyncio.to_thread(self._request, token, method, payload)

    async def _safe_request_get(self, token: str, method: str, params: Optional[dict] = None) -> Dict[str, Any]:
        return await asyncio.to_thread(self._request_get, token, method, params)

    async def _send_message(self, token: str, chat_id: int, text: str) -> None:
        try:
            await self._safe_request(token, "sendMessage", {"chat_id": chat_id, "text": text})
        except Exception as e:
            logger.warning(f"发送 Telegram 消息失败: {e}")

    async def _set_commands(self, token: str) -> None:
        commands = [
            {"command": "query", "description": "查询总账号与进行中的任务状态"},
            {"command": "register", "description": "批量注册，例: /register 50"},
            {"command": "stop", "description": "停止当前由 TG 发起的注册任务"},
            {"command": "help", "description": "查看帮助"},
        ]
        try:
            await self._safe_request(token, "setMyCommands", {"commands": commands})
        except Exception as e:
            logger.warning(f"设置 Telegram 命令失败: {e}")

    @staticmethod
    def _is_admin(user_id: Any, admin_id: str) -> bool:
        return str(user_id).strip() == str(admin_id).strip()

    def _count_accounts(self) -> int:
        with get_db() as db:
            return db.query(Account).count()

    @staticmethod
    def _active_batches():
        from ..web.routes.registration import batch_tasks

        return {
            bid: data for bid, data in batch_tasks.items()
            if not data.get("finished", False) and not data.get("cancelled", False)
        }

    async def _handle_query(self, token: str, chat_id: int) -> None:
        active = self._active_batches()
        total_accounts = self._count_accounts()

        if not active:
            text = (
                f"总账号数: {total_accounts}\n"
                "正在进行的任务: 无"
            )
            await self._send_message(token, chat_id, text)
            return

        lines = [f"总账号数: {total_accounts}", f"正在进行的任务: {len(active)} 个"]
        for bid, status in active.items():
            total = int(status.get("total", 0))
            completed = int(status.get("completed", 0))
            success = int(status.get("success", 0))
            failed = int(status.get("failed", 0))
            remaining = max(total - completed, 0)
            lines.append(
                f"- {bid[:8]}: 注册{completed}/{total}, 成功{success}, 失败{failed}, 剩余{remaining}"
            )
        await self._send_message(token, chat_id, "\n".join(lines))

    async def _start_batch_from_tg(self, token: str, chat_id: int, count: int) -> None:
        from ..web.routes.registration import BatchRegistrationRequest, create_and_start_batch_registration

        request = BatchRegistrationRequest(
            count=count,
            email_service_type="tempmail",
            mode="pipeline",
            concurrency=2,
            interval_min=4,
            interval_max=40,
            auto_upload_cpa=True,
            cpa_service_ids=[],
        )
        result = await create_and_start_batch_registration(request)
        self._active_batch_ids.add(result.batch_id)
        watcher = asyncio.create_task(self._watch_batch_progress(token, chat_id, result.batch_id))
        self._batch_watchers[result.batch_id] = watcher

        await self._send_message(
            token,
            chat_id,
            f"批量注册任务已启动\n任务ID: {result.batch_id}\n数量: {count}\n模式: 流水线\n并发: 2\n间隔: 4-40 秒\n邮箱: 默认临时邮箱\n自动上传: CPA(已启用)"
        )

    async def _watch_batch_progress(self, token: str, chat_id: int, batch_id: str) -> None:
        from ..web.routes.registration import batch_tasks

        last_chunk = 0
        try:
            while True:
                status = batch_tasks.get(batch_id)
                if not status:
                    break

                success = int(status.get("success", 0))
                chunk = success // 10
                while chunk > last_chunk:
                    last_chunk += 1
                    total_accounts = self._count_accounts()
                    await self._send_message(
                        token,
                        chat_id,
                        f"男版迪士尼我已经薅了 {last_chunk * 10} 个奥特曼了，现在库里有 {total_accounts} 个奥特曼。"
                    )

                if status.get("finished", False):
                    total = int(status.get("total", 0))
                    completed = int(status.get("completed", 0))
                    failed = int(status.get("failed", 0))
                    remaining = max(total - completed, 0)
                    await self._send_message(
                        token,
                        chat_id,
                        f"任务 {batch_id[:8]} 已结束\n注册: {completed}/{total}\n成功: {success}\n失败: {failed}\n剩余: {remaining}"
                    )
                    break

                await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"批量任务进度监控异常: {e}")
        finally:
            self._active_batch_ids.discard(batch_id)
            self._batch_watchers.pop(batch_id, None)

    async def _stop_active_batches(self, token: str, chat_id: int) -> None:
        from ..web.routes.registration import batch_tasks
        from ..web.task_manager import task_manager

        stopped = 0
        candidate_ids = set(self._active_batch_ids) | set(self._active_batches().keys())
        for batch_id in list(candidate_ids):
            status = batch_tasks.get(batch_id)
            if not status or status.get("finished", False):
                self._active_batch_ids.discard(batch_id)
                continue
            status["cancelled"] = True
            task_manager.cancel_batch(batch_id)
            stopped += 1

        if stopped == 0:
            await self._send_message(token, chat_id, "没有可停止的 TG 注册任务。")
        else:
            await self._send_message(token, chat_id, f"已提交停止请求，共 {stopped} 个任务正在收工。")

    async def _handle_command(self, token: str, admin_id: str, message: Dict[str, Any]) -> None:
        text = (message.get("text") or "").strip()
        if not text.startswith("/"):
            return

        chat_id = message.get("chat", {}).get("id")
        user_id = message.get("from", {}).get("id")
        if chat_id is None:
            return

        if not self._is_admin(user_id, admin_id):
            await self._send_message(token, chat_id, "你不是管理员，无法使用该机器人。")
            return

        parts = text.split()
        cmd = parts[0].split("@")[0].lower()

        if cmd in ("/start", "/help"):
            await self._send_message(
                token,
                chat_id,
                "可用命令:\n"
                "/query - 查询总账号数与进行中的任务\n"
                "/register 数量 - 启动批量注册(1-2000)\n"
                "/stop - 停止当前 TG 发起任务"
            )
            return

        if cmd == "/query":
            await self._handle_query(token, chat_id)
            return

        if cmd == "/register":
            if len(parts) < 2:
                await self._send_message(token, chat_id, "用法: /register 50")
                return
            try:
                count = int(parts[1])
            except ValueError:
                await self._send_message(token, chat_id, "注册数量必须是数字。")
                return
            if count < 1 or count > 2000:
                await self._send_message(token, chat_id, "注册数量必须在 1-2000 之间。")
                return
            await self._start_batch_from_tg(token, chat_id, count)
            return

        if cmd == "/stop":
            await self._stop_active_batches(token, chat_id)
            return

        await self._send_message(token, chat_id, "未知命令，发送 /help 查看用法。")

    async def _poll_loop(self) -> None:
        settings = get_settings()
        token = settings.telegram_bot_token.get_secret_value() if settings.telegram_bot_token else ""
        admin_id = settings.telegram_admin_id.strip() if settings.telegram_admin_id else ""
        if not token or not admin_id:
            self._running = False
            return

        await self._set_commands(token)

        while self._running:
            try:
                resp = await self._safe_request_get(
                    token,
                    "getUpdates",
                    {"timeout": 25, "offset": self._update_offset, "allowed_updates": json.dumps(["message"])}
                )
                if not resp.get("ok", False):
                    logger.warning(f"Telegram getUpdates 返回异常: {resp}")
                    await asyncio.sleep(3)
                    continue

                for item in resp.get("result", []):
                    update_id = item.get("update_id")
                    if isinstance(update_id, int):
                        self._update_offset = max(self._update_offset, update_id + 1)
                    message = item.get("message")
                    if message:
                        await self._handle_command(token, admin_id, message)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Telegram 轮询异常: {e}")
                await asyncio.sleep(3)


telegram_bot_manager = TelegramBotManager()
