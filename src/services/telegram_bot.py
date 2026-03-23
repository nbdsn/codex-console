"""
Telegram Bot 管理器
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Set, List

from ..config.settings import get_settings
from ..database.models import Account
from ..database.session import get_db
from ..database import crud

logger = logging.getLogger(__name__)

TG_REGISTER_MAX_COUNT = 2_000_000
TG_UPLOAD_OPTION_DIRECT = "直接注册"
TG_UPLOAD_OPTION_CPA = "上传CPA"
TG_UPLOAD_OPTION_SUB2API = "上传Sub2API"
TG_UPLOAD_OPTION_TEAM = "上传Team Manager"


class TelegramBotManager:
    """负责 Telegram 轮询、命令处理与批量注册联动。"""

    def __init__(self) -> None:
        self._runner_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._update_offset: int = 0
        self._active_batch_ids: Set[str] = set()
        self._batch_watchers: Dict[str, asyncio.Task] = {}
        self._insecure_ssl_fallback: bool = False
        self._chat_states: Dict[int, Dict[str, Any]] = {}

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

    @staticmethod
    def _make_ssl_context(verify: bool = True) -> ssl.SSLContext:
        if verify:
            return ssl.create_default_context()
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    @staticmethod
    def _is_cert_error(err: Exception) -> bool:
        return "CERTIFICATE_VERIFY_FAILED" in str(err)

    def _request(self, token: str, method: str, payload: Optional[dict] = None) -> Dict[str, Any]:
        url = self._api_url(token, method)
        headers = {"Content-Type": "application/json"}
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        verify = not self._insecure_ssl_fallback
        try:
            with urllib.request.urlopen(req, timeout=35, context=self._make_ssl_context(verify=verify)) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except Exception as e:
            if verify and self._is_cert_error(e):
                self._insecure_ssl_fallback = True
                logger.warning("Telegram SSL 证书校验失败，已自动切换为兼容模式（不校验证书）")
                with urllib.request.urlopen(req, timeout=35, context=self._make_ssl_context(verify=False)) as resp:
                    body = resp.read().decode("utf-8")
                    return json.loads(body)
            raise

    def _request_get(self, token: str, method: str, params: Optional[dict] = None) -> Dict[str, Any]:
        query = f"?{urllib.parse.urlencode(params or {})}" if params else ""
        url = f"{self._api_url(token, method)}{query}"
        verify = not self._insecure_ssl_fallback
        try:
            with urllib.request.urlopen(url, timeout=40, context=self._make_ssl_context(verify=verify)) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except Exception as e:
            if verify and self._is_cert_error(e):
                self._insecure_ssl_fallback = True
                logger.warning("Telegram SSL 证书校验失败，已自动切换为兼容模式（不校验证书）")
                with urllib.request.urlopen(url, timeout=40, context=self._make_ssl_context(verify=False)) as resp:
                    body = resp.read().decode("utf-8")
                    return json.loads(body)
            raise

    async def _safe_request(self, token: str, method: str, payload: Optional[dict] = None) -> Dict[str, Any]:
        return await asyncio.to_thread(self._request, token, method, payload)

    async def _safe_request_get(self, token: str, method: str, params: Optional[dict] = None) -> Dict[str, Any]:
        return await asyncio.to_thread(self._request_get, token, method, params)

    async def _send_message(self, token: str, chat_id: int, text: str, reply_markup: Optional[dict] = None) -> None:
        try:
            payload: Dict[str, Any] = {"chat_id": chat_id, "text": text}
            if reply_markup:
                payload["reply_markup"] = reply_markup
            await self._safe_request(token, "sendMessage", payload)
        except Exception as e:
            logger.warning(f"发送 Telegram 消息失败: {e}")

    async def _set_commands(self, token: str) -> None:
        commands = [
            {"command": "query", "description": "查询总账号与进行中的任务状态"},
            {"command": "register", "description": "启动批量注册流程（先输入数量再选上传）"},
            {"command": "stop", "description": "停止当前由 TG 发起的注册任务"},
            {"command": "help", "description": "查看帮助"},
        ]
        try:
            await self._safe_request(token, "setMyCommands", {"commands": commands})
        except Exception as e:
            logger.warning(f"设置 Telegram 命令失败: {e}")

    async def _ensure_polling_mode(self, token: str) -> None:
        """确保 bot 处于 long-polling 模式，避免 webhook/getUpdates 冲突。"""
        try:
            resp = await self._safe_request(token, "deleteWebhook", {"drop_pending_updates": False})
            if resp.get("ok", False):
                logger.info("Telegram 已切换为轮询模式(deleteWebhook 成功)")
            else:
                logger.warning(f"deleteWebhook 返回异常: {resp}")
        except Exception as e:
            logger.warning(f"切换 Telegram 轮询模式失败: {e}")

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
            if not data.get("finished", False)
        }

    @staticmethod
    def _build_upload_choice_keyboard() -> dict:
        return {
            "keyboard": [
                [{"text": TG_UPLOAD_OPTION_DIRECT}],
                [{"text": TG_UPLOAD_OPTION_CPA}],
                [{"text": TG_UPLOAD_OPTION_SUB2API}],
                [{"text": TG_UPLOAD_OPTION_TEAM}],
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True,
        }

    @staticmethod
    def _hide_keyboard() -> dict:
        return {"remove_keyboard": True}

    def _pick_first_enabled_service_id(self, service_kind: str) -> Optional[int]:
        with get_db() as db:
            if service_kind == "cpa":
                services = crud.get_cpa_services(db, enabled=True)
            elif service_kind == "sub2api":
                services = crud.get_sub2api_services(db, enabled=True)
            elif service_kind == "tm":
                services = crud.get_tm_services(db, enabled=True)
            else:
                services = []
            return services[0].id if services else None

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
            mode = status.get("mode", "pipeline")
            lines.append(
                f"- {bid[:8]}({mode}): 注册{completed}/{total}, 成功{success}, 失败{failed}, 剩余{remaining}"
            )
        await self._send_message(token, chat_id, "\n".join(lines))

    async def _start_batch_from_tg(self, token: str, chat_id: int, count: int, upload_mode: str) -> None:
        from ..web.routes.registration import BatchRegistrationRequest, create_and_start_batch_registration

        auto_upload_cpa = False
        auto_upload_sub2api = False
        auto_upload_tm = False
        cpa_service_ids: List[int] = []
        sub2api_service_ids: List[int] = []
        tm_service_ids: List[int] = []

        if upload_mode == TG_UPLOAD_OPTION_CPA:
            service_id = self._pick_first_enabled_service_id("cpa")
            if not service_id:
                await self._send_message(token, chat_id, "没有可用的 CPA 服务，请先在 Web 设置里启用后再试。", reply_markup=self._hide_keyboard())
                return
            auto_upload_cpa = True
            cpa_service_ids = [service_id]
        elif upload_mode == TG_UPLOAD_OPTION_SUB2API:
            service_id = self._pick_first_enabled_service_id("sub2api")
            if not service_id:
                await self._send_message(token, chat_id, "没有可用的 Sub2API 服务，请先在 Web 设置里启用后再试。", reply_markup=self._hide_keyboard())
                return
            auto_upload_sub2api = True
            sub2api_service_ids = [service_id]
        elif upload_mode == TG_UPLOAD_OPTION_TEAM:
            service_id = self._pick_first_enabled_service_id("tm")
            if not service_id:
                await self._send_message(token, chat_id, "没有可用的 Team Manager 服务，请先在 Web 设置里启用后再试。", reply_markup=self._hide_keyboard())
                return
            auto_upload_tm = True
            tm_service_ids = [service_id]

        request = BatchRegistrationRequest(
            count=count,
            email_service_type="tempmail",
            mode="pipeline",
            concurrency=2,
            interval_min=4,
            interval_max=40,
            auto_upload_cpa=auto_upload_cpa,
            cpa_service_ids=cpa_service_ids,
            auto_upload_sub2api=auto_upload_sub2api,
            sub2api_service_ids=sub2api_service_ids,
            auto_upload_tm=auto_upload_tm,
            tm_service_ids=tm_service_ids,
        )
        try:
            result = await create_and_start_batch_registration(request)
        except Exception as e:
            logger.exception(f"TG 启动批量注册失败(chat_id={chat_id}, count={count}, mode={upload_mode}): {e}")
            await self._send_message(
                token,
                chat_id,
                f"创建批量任务失败: {e}\n请先检查 Web 后台邮箱/上传配置，或稍后重试。",
                reply_markup=self._hide_keyboard()
            )
            return
        self._active_batch_ids.add(result.batch_id)
        watcher = asyncio.create_task(self._watch_batch_progress(token, chat_id, result.batch_id))
        self._batch_watchers[result.batch_id] = watcher

        upload_desc = upload_mode
        await self._send_message(
            token,
            chat_id,
            f"批量注册任务已启动\n任务ID: {result.batch_id}\n数量: {count}\n模式: 流水线\n并发: 2\n间隔: 4-40 秒\n邮箱: 默认临时邮箱\n任务选项: {upload_desc}",
            reply_markup=self._hide_keyboard()
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

    async def _handle_pending_register_flow(self, token: str, chat_id: int, text: str) -> bool:
        state = self._chat_states.get(chat_id)
        if not state:
            return False

        stage = state.get("stage")
        if stage == "await_count":
            try:
                count = int(text)
            except ValueError:
                await self._send_message(token, chat_id, "注册数量必须是数字，请重新输入 1-2000000。")
                return True
            if count < 1 or count > TG_REGISTER_MAX_COUNT:
                await self._send_message(token, chat_id, f"注册数量必须在 1-{TG_REGISTER_MAX_COUNT} 之间，请重新输入。")
                return True
            state["count"] = count
            state["stage"] = "await_upload_mode"
            await self._send_message(
                token,
                chat_id,
                "请选择注册动作（必须选一个）:\n1) 直接注册\n2) 上传CPA\n3) 上传Sub2API\n4) 上传 Team Manager",
                reply_markup=self._build_upload_choice_keyboard()
            )
            return True

        if stage == "await_upload_mode":
            upload_mode = text.strip()
            if upload_mode not in {
                TG_UPLOAD_OPTION_DIRECT, TG_UPLOAD_OPTION_CPA, TG_UPLOAD_OPTION_SUB2API, TG_UPLOAD_OPTION_TEAM
            }:
                await self._send_message(token, chat_id, "请选择菜单中的一个动作后再继续。", reply_markup=self._build_upload_choice_keyboard())
                return True

            count = int(state.get("count", 0))
            self._chat_states.pop(chat_id, None)
            await self._send_message(
                token,
                chat_id,
                f"收到，正在创建任务：数量 {count}，动作 {upload_mode}。\n如果数量较大会稍慢，我会在创建成功后回复任务ID。",
                reply_markup=self._hide_keyboard()
            )
            # 任务创建改为后台执行，避免阻塞 getUpdates 导致“卡住”体感。
            asyncio.create_task(self._start_batch_from_tg(token, chat_id, count, upload_mode))
            return True

        return False

    async def _handle_command(self, token: str, admin_id: str, message: Dict[str, Any]) -> None:
        text = (message.get("text") or "").strip()

        chat_id = message.get("chat", {}).get("id")
        user_id = message.get("from", {}).get("id")
        if chat_id is None:
            return

        if not self._is_admin(user_id, admin_id):
            await self._send_message(token, chat_id, "你不是管理员，无法使用该机器人。")
            return

        logger.info(f"收到 TG 输入(chat_id={chat_id}, user_id={user_id}): {text}")

        if not text.startswith("/"):
            handled = await self._handle_pending_register_flow(token, chat_id, text)
            if not handled:
                await self._send_message(token, chat_id, "未知输入，发送 /help 查看命令。")
            return

        parts = text.split()
        cmd = parts[0].split("@")[0].lower()

        if cmd in ("/start", "/help"):
            await self._send_message(
                token,
                chat_id,
                "可用命令:\n"
                "/query - 查询总账号数与进行中的任务\n"
                "/register - 启动批量注册流程(1-2000000)\n"
                "/stop - 停止当前 TG 发起任务"
            )
            return

        if cmd == "/query":
            await self._handle_query(token, chat_id)
            return

        if cmd == "/register":
            self._chat_states[chat_id] = {"stage": "await_count"}
            await self._send_message(token, chat_id, "请输入注册数量（1-2000000）：")
            return

        if cmd == "/stop":
            self._chat_states.pop(chat_id, None)
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

        await self._ensure_polling_mode(token)
        await self._set_commands(token)
        backoff_seconds = 3

        while self._running:
            try:
                resp = await self._safe_request_get(
                    token,
                    "getUpdates",
                    {"timeout": 25, "offset": self._update_offset, "allowed_updates": json.dumps(["message"])}
                )
                if not resp.get("ok", False):
                    logger.warning(f"Telegram getUpdates 返回异常: {resp}")
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds = min(backoff_seconds * 2, 30)
                    continue

                for item in resp.get("result", []):
                    update_id = item.get("update_id")
                    if isinstance(update_id, int):
                        self._update_offset = max(self._update_offset, update_id + 1)
                    message = item.get("message")
                    if message:
                        await self._handle_command(token, admin_id, message)
                backoff_seconds = 3
            except asyncio.CancelledError:
                raise
            except urllib.error.HTTPError as e:
                # 409 通常是同一 token 存在并发轮询；429 是频率限制。两者都使用温和退避避免刷屏。
                if e.code == 409:
                    logger.warning("Telegram 轮询冲突(409): 检测到另一个实例正在轮询，10 秒后重试")
                    await asyncio.sleep(10)
                    backoff_seconds = min(max(backoff_seconds, 10), 30)
                    continue
                if e.code == 429:
                    logger.warning("Telegram 触发频率限制(429): 30 秒后重试")
                    await asyncio.sleep(30)
                    backoff_seconds = 30
                    continue
                logger.warning(f"Telegram 轮询 HTTP 异常({e.code}): {e}")
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)
            except Exception as e:
                logger.warning(f"Telegram 轮询异常: {e}")
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)


telegram_bot_manager = TelegramBotManager()
