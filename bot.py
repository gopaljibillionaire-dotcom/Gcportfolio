import asyncio
import datetime
import json
import logging
import os
import psutil
import secrets
import sys
import time
from typing import Dict, Any, Optional

from aiohttp import web, ClientSession
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.telegram import TelegramAPIServer
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    PreCheckoutQuery, LabeledPrice, FSInputFile, BufferedInputFile
)
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from motor.motor_asyncio import AsyncIOMotorClient

import config

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("VideoStreamBot")

START_TIME = time.time()

# Custom Session setup if using custom Local Bot API Server
session = AiohttpSession(
    api=TelegramAPIServer.from_base(config.TELEGRAM_API_URL)
) if config.TELEGRAM_API_URL != "https://api.telegram.org" else None

bot = Bot(token=config.BOT_TOKEN, session=session)
dp = Dispatcher()
router = Router()
dp.include_router(router)

def make_btn(text: str, callback_data: Optional[str] = None, url: Optional[str] = None, style: Optional[str] = None) -> InlineKeyboardButton:
    kwargs = {"text": text}
    if callback_data:
        kwargs["callback_data"] = callback_data
    if url:
        kwargs["url"] = url
    if style:
        try:
            return InlineKeyboardButton(**kwargs, style=style)
        except TypeError:
            pass
    return InlineKeyboardButton(**kwargs)

# ==========================================
# 1. OPTIMIZED DATABASE MANAGER (Low Footprint)
# ==========================================
class DatabaseManager:
    def __init__(self, uri: str, db_name: str):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client[db_name]
        self.users = self.db["users"]
        self.files = self.db["files"]
        self.subscriptions = self.db["subscriptions"]
        self.payments = self.db["payments"]
        self.settings = self.db["settings"]

    async def init_indexes(self):
        # Single-field lightweight indexes to keep RAM < 50MB
        await self.users.create_index("user_id", unique=True)
        await self.files.create_index("stream_token", unique=True)
        await self.files.create_index("owner_user_id")
        
        # MongoDB TTL Index: Auto-delete expired free files automatically from DB
        await self.files.create_index("expires_at", expireAfterSeconds=0)
        await self.subscriptions.create_index("user_id")

        if not await self.settings.find_one({"_id": "global"}):
            await self.settings.insert_one({
                "_id": "global",
                "maintenance": False,
                "free_link_expiry_hours": config.FREE_LINK_EXPIRY_HOURS
            })

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        return await self.users.find_one({"user_id": user_id}, {"_id": 0})

    async def create_or_update_user(self, user_id: int, username: str, first_name: str, referrer_id: Optional[int] = None):
        # Compact user schema (minimal bytes stored per user)
        await self.users.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id": user_id,
                "u": username[:20],
                "fn": first_name[:20],
                "is_banned": False
            }},
            upsert=True
        )

    async def is_banned(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user and user.get("is_banned", False))

    async def get_active_subscription(self, user_id: int) -> Optional[Dict[str, Any]]:
        now = datetime.datetime.utcnow()
        return await self.subscriptions.find_one({
            "user_id": user_id,
            "payment_status": "successful",
            "expires_at": {"$gt": now}
        })

    async def add_subscription(self, user_id: int, plan: str, amount: float, currency: str, method: str, days: int = 30) -> str:
        now = datetime.datetime.utcnow()
        active_sub = await self.get_active_subscription(user_id)
        start_time = active_sub["expires_at"] if active_sub else now
        expires_at = start_time + datetime.timedelta(days=days)
        sub_id = secrets.token_hex(4)

        doc = {
            "sub_id": sub_id,
            "user_id": user_id,
            "plan": plan,
            "payment_status": "successful",
            "expires_at": expires_at
        }
        await self.subscriptions.insert_one(doc)
        await self.reactivate_user_files(user_id)
        return sub_id

    async def create_file_record(self, owner_id: int, file_id: str, filename: str, mime_type: str, size: int, is_premium: bool) -> Dict[str, Any]:
        now = datetime.datetime.utcnow()
        # Non-premium links auto-expire in MongoDB via TTL index
        expiry = now + datetime.timedelta(hours=config.FREE_LINK_EXPIRY_HOURS) if not is_premium else now + datetime.timedelta(days=365)
        
        token = secrets.token_urlsafe(12)
        doc = {
            "owner_user_id": owner_id,
            "tg_file_id": file_id,
            "name": filename[:40],
            "mime": mime_type[:20],
            "size": size,
            "expires_at": expiry,
            "is_active": True,
            "stream_token": token
        }
        await self.files.insert_one(doc)
        return doc

    async def get_file_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        return await self.files.find_one({"stream_token": token})

    async def reactivate_user_files(self, user_id: int):
        await self.files.update_many(
            {"owner_user_id": user_id},
            {"$set": {"is_active": True}}
        )

    # Database Purge & Clean Methods
    async def clear_expired_files(self) -> int:
        res = await self.files.delete_many({"expires_at": {"$lte": datetime.datetime.utcnow()}})
        return res.deleted_count

    async def clear_all_files(self) -> int:
        res = await self.files.delete_many({})
        return res.deleted_count

    async def reset_entire_database(self):
        await self.users.delete_many({})
        await self.files.delete_many({})
        await self.subscriptions.delete_many({})
        await self.payments.delete_many({})

db: DatabaseManager = None

# ==========================================
# 2. HTTP STREAM & DOWNLOAD SERVER
# ==========================================
class HTTPServerManager:
    def __init__(self, bot: Bot, db: DatabaseManager):
        self.bot = bot
        self.db = db
        self.app = web.Application()
        self.setup_routes()

    def setup_routes(self):
        self.app.router.add_get("/health", self.handle_health)
        self.app.router.add_get("/stream/{token}", self.handle_stream)
        self.app.router.add_get("/download/{token}", self.handle_download)

    async def handle_health(self, request):
        return web.json_response({"status": "ok", "uptime_seconds": time.time() - START_TIME})

    async def _process_file_request(self, request, token: str, is_download: bool):
        file_rec = await self.db.get_file_by_token(token)
        if not file_rec:
            return web.Response(text="404 Not Found: File expired or deleted from DB", status=404)

        if not file_rec.get("is_active", True):
            return web.Response(text="403 Forbidden: Link Disabled", status=403)

        # Standard Telegram Bot API limit check (20MB max without local bot API server)
        if config.TELEGRAM_API_URL == "https://api.telegram.org" and file_rec.get("size", 0) > 20 * 1024 * 1024:
            return web.Response(
                text="502 Bad Gateway: File exceeds 20MB limit. Standard Telegram API cannot fetch >20MB files.",
                status=502
            )

        try:
            tg_file = await self.bot.get_file(file_rec["tg_file_id"])
            file_path = tg_file.file_path
            file_url = f"{config.TELEGRAM_API_URL}/file/bot{config.BOT_TOKEN}/{file_path}"
        except TelegramBadRequest as e:
            logger.error(f"Telegram API file error: {e}")
            return web.Response(text=f"502 Bad Gateway: Telegram API error - {e}", status=502)
        except Exception as e:
            logger.error(f"Failed to fetch Telegram file: {e}")
            return web.Response(text=f"502 Bad Gateway: {e}", status=502)

        disposition = "attachment" if is_download else "inline"
        filename = file_rec.get("name", "video.mp4")
        mime = file_rec.get("mime", "video/mp4")

        async with ClientSession() as session_client:
            req_headers = {}
            if "Range" in request.headers:
                req_headers["Range"] = request.headers["Range"]

            async with session_client.get(file_url, headers=req_headers) as resp:
                if resp.status not in (200, 206):
                    return web.Response(text=f"502 Bad Gateway: Telegram server status {resp.status}", status=502)

                response = web.StreamResponse(
                    status=resp.status,
                    headers={
                        "Content-Type": mime,
                        "Content-Disposition": f'{disposition}; filename="{filename}"',
                        "Accept-Ranges": "bytes",
                    }
                )
                if "Content-Range" in resp.headers:
                    response.headers["Content-Range"] = resp.headers["Content-Range"]
                if "Content-Length" in resp.headers:
                    response.headers["Content-Length"] = resp.headers["Content-Length"]

                await response.prepare(request)
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    await response.write(chunk)
                return response

    async def handle_stream(self, request):
        token = request.match_info.get("token")
        return await self._process_file_request(request, token, is_download=False)

    async def handle_download(self, request):
        token = request.match_info.get("token")
        return await self._process_file_request(request, token, is_download=True)

# ==========================================
# 3. AIOGRAM BOT HANDLERS & HELPERS
# ==========================================
async def verify_channel_member(user_id: int) -> bool:
    if not config.REQUIRED_CHANNEL_ID:
        return True
    try:
        member = await bot.get_chat_member(chat_id=config.REQUIRED_CHANNEL_ID, user_id=user_id)
        return member.status in ["creator", "administrator", "member"]
    except Exception:
        return True

def get_channel_verify_kb() -> InlineKeyboardMarkup:
    channel_link = f"https://t.me/{config.REQUIRED_CHANNEL_USERNAME}" if config.REQUIRED_CHANNEL_USERNAME else "#"
    return InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("📢 Join Channel", url=channel_link, style="primary")],
        [make_btn("✅ Verify Membership", callback_data="check_membership", style="success")]
    ])

def get_main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("📤 Upload / Forward Video", callback_data="upload_info", style="primary")],
        [make_btn("💎 Premium", callback_data="premium_page", style="success"), make_btn("📁 My Files", callback_data="my_files", style="primary")],
        [make_btn("👤 Account", callback_data="account_page", style="primary"), make_btn("❓ Help", callback_data="help_page", style="primary")]
    ])

@router.message.outer_middleware()
@router.callback_query.outer_middleware()
async def main_middleware(handler, event, data):
    user = event.from_user
    if not user:
        return await handler(event, data)

    if await db.is_banned(user.id):
        msg = "🚫 You are banned from using this bot."
        if isinstance(event, CallbackQuery):
            await event.answer(msg, show_alert=True)
        else:
            await event.answer(msg)
        return

    await db.create_or_update_user(user.id, user.username or "", user.first_name or "")
    return await handler(event, data)

@router.message(CommandStart())
async def cmd_start(message: Message):
    if not await verify_channel_member(message.from_user.id):
        await message.answer("🔒 Please join our required channel first.", reply_markup=get_channel_verify_kb())
        return

    await message.answer("🎬 **Welcome to Video Stream & Download Bot!**\n\nSend or forward any file to generate direct streaming & download links.", reply_markup=get_main_menu_kb(), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "check_membership")
async def cb_check_membership(call: CallbackQuery):
    if await verify_channel_member(call.from_user.id):
        await call.message.edit_text("✅ Membership verified! Welcome.", reply_markup=get_main_menu_kb())
    else:
        await call.answer("❌ You haven't joined the channel yet!", show_alert=True)

@router.callback_query(F.data == "upload_info")
async def cb_upload_info(call: CallbackQuery):
    await call.message.edit_text("📤 **How to Upload:**\nForward any video or audio file to this chat to generate high-speed streaming links.", reply_markup=get_main_menu_kb(), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "help_page")
async def cb_help_page(call: CallbackQuery):
    text = (
        "❓ **Bot Support & Limits**\n\n"
        "1. Send any video file to get immediate links.\n"
        "2. **Free Links**: Expire automatically in 24 hours.\n"
        "3. **Premium**: Permanent storage and unlimited link validity."
    )
    await call.message.edit_text(text, reply_markup=get_main_menu_kb(), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "account_page")
async def cb_account_page(call: CallbackQuery):
    user_id = call.from_user.id
    sub = await db.get_active_subscription(user_id)
    file_count = await db.files.count_documents({"owner_user_id": user_id})

    status_str = "💎 Premium Active" if sub else "🆓 Free Tier"
    exp_str = sub["expires_at"].strftime("%Y-%m-%d %H:%M UTC") if sub else "N/A"

    text = (
        f"👤 **Account Information**\n\n"
        f"• **ID**: `{user_id}`\n"
        f"• **Status**: {status_str}\n"
        f"• **Expiry**: {exp_str}\n"
        f"• **Active Files**: {file_count}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("💎 Upgrade Premium", callback_data="premium_page", style="success")],
        [make_btn("📁 My Files", callback_data="my_files", style="primary")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# --- FILE PROCESSING ---
@router.message(F.video | F.document | F.animation | F.audio)
async def handle_media_upload(message: Message):
    if not await verify_channel_member(message.from_user.id):
        await message.answer("🔒 Please join our channel first.", reply_markup=get_channel_verify_kb())
        return

    proc_msg = await message.answer("⏳ Processing file...")
    
    media = message.video or message.document or message.animation or message.audio
    file_id = media.file_id
    file_name = getattr(media, "file_name", "video.mp4") or "video.mp4"
    mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"
    file_size = getattr(media, "file_size", 0)

    is_premium = bool(await db.get_active_subscription(message.from_user.id))

    rec = await db.create_file_record(
        owner_id=message.from_user.id,
        file_id=file_id,
        filename=file_name,
        mime_type=mime_type,
        size=file_size,
        is_premium=is_premium
    )

    stream_url = f"{config.BASE_URL}/stream/{rec['stream_token']}"
    download_url = f"{config.BASE_URL}/download/{rec['stream_token']}"

    expiry_info = "Unlimited (Premium)" if is_premium else f"{config.FREE_LINK_EXPIRY_HOURS} Hours"

    text = (
        f"✅ **File Link Generated!**\n\n"
        f"📁 **Filename**: `{file_name[:30]}`\n"
        f"📦 **Size**: {round(file_size / (1024*1024), 2)} MB\n"
        f"⏰ **Validity**: {expiry_info}\n\n"
        f"🎬 **Stream**: `{stream_url}`\n\n"
        f"⬇️ **Download**: `{download_url}`"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("🎬 Stream Video", url=stream_url), make_btn("⬇️ Direct Download", url=download_url)]
    ])

    await proc_msg.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "my_files")
async def cb_my_files(call: CallbackQuery):
    cursor = db.files.find({"owner_user_id": call.from_user.id}).sort("expires_at", -1).limit(10)
    files = await cursor.to_list(length=10)

    if not files:
        await call.message.edit_text("📁 You have no active files stored.", reply_markup=get_main_menu_kb())
        return

    text = "📁 **Your Files (Latest 10)**:\n\n"
    btns = []
    for f in files:
        stream_url = f"{config.BASE_URL}/stream/{f['stream_token']}"
        text += f"• `{f['name']}`\n🔗 `{stream_url}`\n\n"

    btns.append([make_btn("🔙 Main Menu", callback_data="main_menu")])
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "main_menu")
async def cb_main_menu(call: CallbackQuery):
    await call.message.edit_text("🎬 **Welcome to Video Stream Bot!**", reply_markup=get_main_menu_kb())

# --- ADMIN PANEL & DATABASE CLEANUP ---
@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        return

    total_users = await db.users.count_documents({})
    total_files = await db.files.count_documents({})
    mem = psutil.virtual_memory()

    text = (
        f"⚙️ **ADMIN DASHBOARD**\n\n"
        f"👥 **Total Users**: {total_users}\n"
        f"🎬 **Total Active Files**: {total_files}\n"
        f"🧠 **Server RAM**: {mem.percent}%\n"
        f"⏱ **Uptime**: {int(time.time() - START_TIME)}s"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("🗑 Clear Expired Files", callback_data="admin_clear_expired")],
        [make_btn("⚠️ Delete ALL Files", callback_data="admin_clear_all_files")],
        [make_btn("🔥 RESET Entire Database", callback_data="admin_reset_db_confirm")]
    ])
    await message.answer(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "admin_clear_expired")
async def cb_admin_clear_expired(call: CallbackQuery):
    if call.from_user.id not in config.ADMIN_IDS:
        return
    count = await db.clear_expired_files()
    await call.answer(f"✅ Cleared {count} expired files from Database!", show_alert=True)

@router.callback_query(F.data == "admin_clear_all_files")
async def cb_admin_clear_all_files(call: CallbackQuery):
    if call.from_user.id not in config.ADMIN_IDS:
        return
    count = await db.clear_all_files()
    await call.answer(f"🗑 Deleted all {count} file records from MongoDB!", show_alert=True)

@router.callback_query(F.data == "admin_reset_db_confirm")
async def cb_admin_reset_db_confirm(call: CallbackQuery):
    if call.from_user.id not in config.ADMIN_IDS:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("❌ YES, DELETE EVERYTHING", callback_data="admin_reset_db_execute", style="danger")],
        [make_btn("🔙 Cancel", callback_data="admin_cancel")]
    ])
    await call.message.edit_text("⚠️ **ARE YOU SURE?** This will wipe all users, files, and subscriptions from MongoDB.", reply_markup=kb)

@router.callback_query(F.data == "admin_reset_db_execute")
async def cb_admin_reset_db_execute(call: CallbackQuery):
    if call.from_user.id not in config.ADMIN_IDS:
        return
    await db.reset_entire_database()
    await call.message.edit_text("💥 **Database has been completely wiped and reset!**")

@router.callback_query(F.data == "admin_cancel")
async def cb_admin_cancel(call: CallbackQuery):
    await call.message.delete()

# ==========================================
# 4. APPLICATION LAUNCHER
# ==========================================
async def main():
    global db
    db = DatabaseManager(config.MONGO_URI, config.DATABASE_NAME)
    await db.init_indexes()

    # Web Engine for Streaming
    http_manager = HTTPServerManager(bot, db)
    runner = web.AppRunner(http_manager.app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.PORT)
    await site.start()
    logger.info(f"HTTP Server started on port {config.PORT}")

    logger.info("Starting Bot Polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
