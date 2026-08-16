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
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    PreCheckoutQuery, LabeledPrice, FSInputFile, BufferedInputFile
)
from aiogram.enums import ParseMode, ContentType
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

# Graceful InlineKeyboardButton Creator
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
# 1. DATABASE MANAGER (MongoDB Atlas)
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
        self.logs = self.db["logs"]

    async def init_indexes(self):
        await self.users.create_index("user_id", unique=True)
        await self.files.create_index("stream_token", unique=True)
        await self.files.create_index("download_token", unique=True)
        await self.files.create_index("owner_user_id")
        await self.subscriptions.create_index("user_id")
        await self.subscriptions.create_index("expires_at")
        
        # Init settings if missing
        if not await self.settings.find_one({"_id": "global"}):
            await self.settings.insert_one({
                "_id": "global",
                "maintenance": False,
                "free_link_expiry_hours": config.FREE_LINK_EXPIRY_HOURS,
                "referral_enabled": True
            })

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        return await self.users.find_one({"user_id": user_id})

    async def create_or_update_user(self, user_id: int, username: str, first_name: str, referrer_id: Optional[int] = None):
        user = await self.get_user(user_id)
        if not user:
            data = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "joined_at": datetime.datetime.utcnow(),
                "is_banned": False,
                "referrer_id": referrer_id if referrer_id != user_id else None
            }
            await self.users.insert_one(data)
        else:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {"username": username, "first_name": first_name}}
            )

    async def is_banned(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user and user.get("is_banned", False))

    async def set_ban_status(self, user_id: int, status: bool):
        await self.users.update_one({"user_id": user_id}, {"$set": {"is_banned": status}})

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
        sub_id = secrets.token_hex(8)

        doc = {
            "subscription_id": sub_id,
            "user_id": user_id,
            "plan": plan,
            "amount": amount,
            "currency": currency,
            "payment_method": method,
            "payment_status": "successful",
            "created_at": now,
            "starts_at": start_time,
            "expires_at": expires_at,
            "transaction_id": secrets.token_hex(12)
        }
        await self.subscriptions.insert_one(doc)
        await self.reactivate_user_files(user_id)
        return sub_id

    async def create_file_record(self, owner_id: int, file_id: str, unique_id: str, filename: str, mime_type: str, size: int, duration: int, is_premium: bool) -> Dict[str, Any]:
        now = datetime.datetime.utcnow()
        expiry = now + datetime.timedelta(hours=config.FREE_LINK_EXPIRY_HOURS) if not is_premium else datetime.datetime.utcnow() + datetime.timedelta(days=3650)
        
        doc = {
            "owner_user_id": owner_id,
            "telegram_file_id": file_id,
            "file_unique_id": unique_id,
            "original_filename": filename,
            "mime_type": mime_type,
            "file_size": size,
            "duration": duration,
            "created_at": now,
            "expires_at": expiry,
            "is_active": True,
            "access_type": "premium" if is_premium else "free",
            "stream_token": secrets.token_urlsafe(24),
            "download_token": secrets.token_urlsafe(24)
        }
        await self.files.insert_one(doc)
        return doc

    async def get_file_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        return await self.files.find_one({
            "$or": [{"stream_token": token}, {"download_token": token}]
        })

    async def reactivate_user_files(self, user_id: int):
        await self.files.update_many(
            {"owner_user_id": user_id, "access_type": "premium"},
            {"$set": {"is_active": True}}
        )

    async def log_action(self, action: str, user_id: int, details: Optional[Dict[str, Any]] = None):
        await self.logs.insert_one({
            "action": action,
            "user_id": user_id,
            "details": details or {},
            "timestamp": datetime.datetime.utcnow()
        })

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
            return web.Response(text="404 Not Found", status=404)

        if not file_rec.get("is_active", True):
            return web.Response(text="403 Forbidden: Link Disabled", status=403)

        # Expiry Check
        is_premium = bool(await self.db.get_active_subscription(file_rec["owner_user_id"]))
        if not is_premium and file_rec["expires_at"] < datetime.datetime.utcnow():
            return web.Response(text="⏰ This link has expired.", status=410)

        try:
            tg_file = await self.bot.get_file(file_rec["telegram_file_id"])
            file_path = tg_file.file_path
            file_url = f"https://api.telegram.org/file/bot{config.BOT_TOKEN}/{file_path}"
        except Exception as e:
            logger.error(f"Failed to resolve Telegram file: {e}")
            return web.Response(text="502 Bad Gateway: Unable to fetch Telegram file", status=502)

        # Stream proxy setup
        headers = {}
        disposition = "attachment" if is_download else "inline"
        filename = file_rec.get("original_filename", "video.mp4")
        headers["Content-Type"] = file_rec.get("mime_type", "video/mp4")
        headers["Content-Disposition"] = f'{disposition}; filename="{filename}"'

        async with ClientSession() as session:
            req_headers = {}
            if "Range" in request.headers:
                req_headers["Range"] = request.headers["Range"]

            async with session.get(file_url, headers=req_headers) as resp:
                response = web.StreamResponse(
                    status=resp.status,
                    headers={
                        "Content-Type": headers["Content-Type"],
                        "Content-Disposition": headers["Content-Disposition"],
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
bot = Bot(token=config.BOT_TOKEN, default=None)
dp = Dispatcher()
router = Router()
dp.include_router(router)
db: DatabaseManager = None

async def verify_channel_member(user_id: int) -> bool:
    if not config.REQUIRED_CHANNEL_ID:
        return True
    try:
        member = await bot.get_chat_member(chat_id=config.REQUIRED_CHANNEL_ID, user_id=user_id)
        return member.status in ["creator", "administrator", "member"]
    except Exception as e:
        logger.error(f"Channel check error: {e}")
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
        [make_btn("🔗 My Links", callback_data="my_links", style="primary"), make_btn("❓ Help", callback_data="help_page", style="primary")],
        [make_btn("👤 Account", callback_data="account_page", style="primary"), make_btn("📊 My Statistics", callback_data="my_stats", style="primary")]
    ])

# --- MIDDLEWARE & FILTERS ---
@router.message.outer_middleware()
@router.callback_query.outer_middleware()
async def main_middleware(handler, event, data):
    user = event.from_user
    if not user:
        return await handler(event, data)

    # Maintenance Check
    settings = await db.settings.find_one({"_id": "global"}) or {}
    if settings.get("maintenance") and user.id not in config.ADMIN_IDS:
        msg = "🛠 Bot is temporarily under maintenance. Please check back later!"
        if isinstance(event, CallbackQuery):
            await event.answer(msg, show_alert=True)
        else:
            await event.answer(msg)
        return

    # Ban Check
    if await db.is_banned(user.id):
        msg = "🚫 You are banned from using this bot."
        if isinstance(event, CallbackQuery):
            await event.answer(msg, show_alert=True)
        else:
            await event.answer(msg)
        return

    # User Record Sync
    referrer = None
    if isinstance(event, Message) and event.text and event.text.startswith("/start "):
        parts = event.text.split()
        if len(parts) > 1 and parts[1].isdigit():
            referrer = int(parts[1])

    await db.create_or_update_user(user.id, user.username or "", user.first_name or "", referrer)
    return await handler(event, data)

# --- START & VERIFICATION ---
@router.message(CommandStart())
async def cmd_start(message: Message):
    if not await verify_channel_member(message.from_user.id):
        await message.answer("🔒 Please join our required channel first to access the bot.", reply_markup=get_channel_verify_kb())
        return

    await message.answer("🎬 **Welcome to Video Stream & Download Bot!**\n\nSend or forward any authorized video/file to get streaming and download links.", reply_markup=get_main_menu_kb(), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "check_membership")
async def cb_check_membership(call: CallbackQuery):
    if await verify_channel_member(call.from_user.id):
        await call.message.edit_text("✅ Membership verified! Welcome to the main menu.", reply_markup=get_main_menu_kb())
    else:
        await call.answer("❌ You haven't joined the required channel yet!", show_alert=True)

# --- NAVIGATION CALLBACKS ---
@router.callback_query(F.data == "upload_info")
async def cb_upload_info(call: CallbackQuery):
    await call.message.edit_text("📤 **How to Upload / Forward:**\n\nSimply forward any video, document, or audio file directly to this chat. The bot will process it immediately and return secure streaming and download URLs.", reply_markup=get_main_menu_kb(), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "help_page")
async def cb_help_page(call: CallbackQuery):
    text = (
        "❓ **Bot Support & Help**\n\n"
        "1. Send any video file to receive direct links.\n"
        "2. **Free Users**: Links automatically expire in 24 hours.\n"
        "3. **Premium Users**: Permanent availability with zero expiration while subscribed.\n"
        "4. Your files are retained safely in the database even after premium expires."
    )
    await call.message.edit_text(text, reply_markup=get_main_menu_kb(), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "account_page")
async def cb_account_page(call: CallbackQuery):
    user_id = call.from_user.id
    user_data = await db.get_user(user_id)
    sub = await db.get_active_subscription(user_id)
    file_count = await db.files.count_documents({"owner_user_id": user_id})
    active_count = await db.files.count_documents({"owner_user_id": user_id, "is_active": True})

    status_str = "💎 Premium Active" if sub else "🆓 Free Tier"
    exp_str = sub["expires_at"].strftime("%Y-%m-%d %H:%M UTC") if sub else "N/A"

    text = (
        f"👤 **Account Information**\n\n"
        f"• **Name**: {call.from_user.first_name}\n"
        f"• **ID**: `{user_id}`\n"
        f"• **Status**: {status_str}\n"
        f"• **Expiry**: {exp_str}\n"
        f"• **Total Files**: {file_count}\n"
        f"• **Active Links**: {active_count}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("💎 Upgrade Premium", callback_data="premium_page", style="success")],
        [make_btn("📁 My Files", callback_data="my_files", style="primary"), make_btn("🔗 My Links", callback_data="my_links", style="primary")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# --- FILE PROCESSING ---
@router.message(F.video | F.document | F.animation | F.audio)
async def handle_media_upload(message: Message):
    if not await verify_channel_member(message.from_user.id):
        await message.answer("🔒 Please join our required channel first.", reply_markup=get_channel_verify_kb())
        return

    proc_msg = await message.answer("⏳ Processing your file...")
    
    media = message.video or message.document or message.animation or message.audio
    file_id = media.file_id
    unique_id = media.file_unique_id
    file_name = getattr(media, "file_name", "media_file.mp4") or "media_file.mp4"
    mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"
    file_size = getattr(media, "file_size", 0)
    duration = getattr(media, "duration", 0)

    is_premium = bool(await db.get_active_subscription(message.from_user.id))

    rec = await db.create_file_record(
        owner_id=message.from_user.id,
        file_id=file_id,
        unique_id=unique_id,
        filename=file_name,
        mime_type=mime_type,
        size=file_size,
        duration=duration,
        is_premium=is_premium
    )

    stream_url = f"{config.BASE_URL}/stream/{rec['stream_token']}"
    download_url = f"{config.BASE_URL}/download/{rec['download_token']}"

    expiry_info = "Unlimited (Premium)" if is_premium else f"{config.FREE_LINK_EXPIRY_HOURS} Hours"

    text = (
        f"✅ **File Processed Successfully!**\n\n"
        f"📁 **Filename**: `{file_name}`\n"
        f"📦 **Size**: {round(file_size / (1024*1024), 2)} MB\n"
        f"⏰ **Validity**: {expiry_info}\n\n"
        f"🎬 **Stream Link**: `{stream_url}`\n\n"
        f"⬇️ **Download Link**: `{download_url}`"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("🎬 Stream Video", url=stream_url), make_btn("⬇️ Direct Download", url=download_url)],
        [make_btn("🗑 Disable Link", callback_data=f"disable_file_{rec['stream_token']}", style="danger")]
    ])

    await proc_msg.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# --- MY FILES & LINKS ---
@router.callback_query(F.data == "my_files")
async def cb_my_files(call: CallbackQuery):
    cursor = db.files.find({"owner_user_id": call.from_user.id}).sort("created_at", -1).limit(10)
    files = await cursor.to_list(length=10)

    if not files:
        await call.message.edit_text("📁 You have not uploaded any files yet.", reply_markup=get_main_menu_kb())
        return

    text = "📁 **Your Uploaded Files (Latest 10)**:\n\n"
    btns = []
    for f in files:
        status = "🟢 Active" if f.get("is_active") else "🔴 Disabled/Expired"
        text += f"• `{f['original_filename']}` ({status})\n"
        btns.append([make_btn(f"🔗 {f['original_filename'][:20]}", callback_data=f"view_file_{f['stream_token']}")])

    btns.append([make_btn("♻️ Reactivate All My Files", callback_data="reactivate_files", style="success")])
    btns.append([make_btn("🔙 Main Menu", callback_data="main_menu")])

    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data.startswith("view_file_"))
async def cb_view_file(call: CallbackQuery):
    token = call.data.replace("view_file_", "")
    rec = await db.get_file_by_token(token)
    if not rec or rec["owner_user_id"] != call.from_user.id:
        await call.answer("File not found or unauthorized.", show_alert=True)
        return

    stream_url = f"{config.BASE_URL}/stream/{rec['stream_token']}"
    download_url = f"{config.BASE_URL}/download/{rec['download_token']}"
    status = "🟢 Active" if rec.get("is_active") else "🔴 Inactive"

    text = (
        f"📄 **File Details**:\n\n"
        f"• **Name**: `{rec['original_filename']}`\n"
        f"• **Status**: {status}\n"
        f"• **Stream**: `{stream_url}`\n"
        f"• **Download**: `{download_url}`"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("🎬 Stream", url=stream_url), make_btn("⬇️ Download", url=download_url)],
        [make_btn("🗑 Remove Record", callback_data=f"delete_file_{rec['stream_token']}", style="danger")],
        [make_btn("🔙 Back to Files", callback_data="my_files")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data.startswith("delete_file_"))
async def cb_delete_file(call: CallbackQuery):
    token = call.data.replace("delete_file_", "")
    await db.files.delete_one({"stream_token": token, "owner_user_id": call.from_user.id})
    await call.answer("Record permanently deleted.")
    await cb_my_files(call)

@router.callback_query(F.data == "reactivate_files")
async def cb_reactivate_files(call: CallbackQuery):
    sub = await db.get_active_subscription(call.from_user.id)
    if not sub:
        await call.answer("⚠️ Active Premium Subscription required to reactivate files!", show_alert=True)
        return
    await db.reactivate_user_files(call.from_user.id)
    await call.answer("✅ All eligible premium files reactivated!", show_alert=True)

# --- PREMIUM & PAYMENTS ---
@router.callback_query(F.data == "premium_page")
async def cb_premium_page(call: CallbackQuery):
    text = (
        "💎 **PREMIUM SUBSCRIPTION — 30 DAYS**\n\n"
        "⚡ **Features:**\n"
        "• Permanent file links (No 24-hour expiry)\n"
        "• Instant reactivation of past files\n"
        "• Priority streaming speed\n\n"
        "💰 **Plans Available:**\n"
        f"• 🇮🇳 **₹{config.PREMIUM_PRICE_INR}** / Month\n"
        f"• 💵 **${config.PREMIUM_PRICE_USD}** / Month\n"
        f"• ⭐ **{config.PREMIUM_PRICE_XTR} Telegram Stars** / Month"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn(f"⭐ Pay {config.PREMIUM_PRICE_XTR} Stars", callback_data="pay_stars", style="success")],
        [make_btn(f"💳 Pay ₹{config.PREMIUM_PRICE_INR}", callback_data="pay_inr", style="primary"), make_btn(f"💵 Pay ${config.PREMIUM_PRICE_USD}", callback_data="pay_usd", style="primary")],
        [make_btn("🔙 Main Menu", callback_data="main_menu")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# Telegram Stars Payment
@router.callback_query(F.data == "pay_stars")
async def cb_pay_stars(call: CallbackQuery):
    prices = [LabeledPrice(label="30 Days Premium", amount=config.PREMIUM_PRICE_XTR)]
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title="30 Days Premium Subscription",
        description="Unlock permanent video links and stream features.",
        payload="premium_30_days_stars",
        currency="XTR",
        prices=prices,
        provider_token=""
    )

@router.pre_checkout_query()
async def process_pre_checkout(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@router.message(F.successful_payment)
async def process_successful_payment(message: Message):
    pay_info = message.successful_payment
    await db.add_subscription(
        user_id=message.from_user.id,
        plan="1 Month Stars",
        amount=pay_info.total_amount,
        currency=pay_info.currency,
        method="telegram_stars",
        days=30
    )
    await message.answer("🎉 **Payment Successful!** Your Premium subscription has been activated for 30 days.", parse_mode=ParseMode.MARKDOWN)

# Manual External Payment Flow
@router.callback_query(F.data.in_(["pay_inr", "pay_usd"]))
async def cb_manual_pay_instructions(call: CallbackQuery):
    currency_str = "₹" if call.data == "pay_inr" else "$"
    amount_str = f"{config.PREMIUM_PRICE_INR}" if call.data == "pay_inr" else f"{config.PREMIUM_PRICE_USD}"

    text = (
        f"💳 **Manual Payment Instructions ({currency_str}{amount_str})**\n\n"
        f"Send payment via UPI / PayPal to admin, then click **Submit Proof** below and upload a screenshot or transaction ID.\n\n"
        f"Admin Contact: @{config.REQUIRED_CHANNEL_USERNAME or 'Support'}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("📤 Submit Payment Proof", callback_data=f"submit_proof_{call.data}")],
        [make_btn("🔙 Back", callback_data="premium_page")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data.startswith("submit_proof_"))
async def cb_prompt_proof(call: CallbackQuery):
    await call.message.edit_text(" Please send your payment proof (Screenshot or Document) now in response to this message.")

@router.message(F.photo | F.document)
async def handle_payment_proof(message: Message):
    if not config.ADMIN_IDS:
        return

    proof_file_id = message.photo[-1].file_id if message.photo else message.document.file_id
    pay_id = secrets.token_hex(6)

    await db.payments.insert_one({
        "payment_id": pay_id,
        "user_id": message.from_user.id,
        "status": "pending",
        "proof_file_id": proof_file_id,
        "created_at": datetime.datetime.utcnow()
    })

    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("✅ Approve", callback_data=f"approve_pay_{pay_id}", style="success"), make_btn("❌ Reject", callback_data=f"reject_pay_{pay_id}", style="danger")]
    ])

    admin_msg = (
        f"💳 **NEW PAYMENT VERIFICATION REQUEST**\n\n"
        f"• **User**: {message.from_user.full_name} (@{message.from_user.username or 'N/A'})\n"
        f"• **User ID**: `{message.from_user.id}`\n"
        f"• **Payment ID**: `{pay_id}`"
    )

    for admin_id in config.ADMIN_IDS:
        try:
            if message.photo:
                await bot.send_photo(admin_id, proof_file_id, caption=admin_msg, reply_markup=admin_kb, parse_mode=ParseMode.MARKDOWN)
            else:
                await bot.send_document(admin_id, proof_file_id, caption=admin_msg, reply_markup=admin_kb, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error(f"Failed to forward proof to admin {admin_id}: {e}")

    await message.answer("✅ **Payment proof submitted!** An administrator will verify it shortly.")

# Admin Approval / Rejection
@router.callback_query(F.data.startswith("approve_pay_"))
async def cb_approve_payment(call: CallbackQuery):
    if call.from_user.id not in config.ADMIN_IDS:
        return

    pay_id = call.data.replace("approve_pay_", "")
    pay_doc = await db.payments.find_one({"payment_id": pay_id})

    if not pay_doc or pay_doc["status"] != "pending":
        await call.answer("Payment request already processed or invalid.", show_alert=True)
        return

    await db.payments.update_one({"payment_id": pay_id}, {"$set": {"status": "approved"}})
    await db.add_subscription(pay_doc["user_id"], "Manual Plan", 0.0, "INR", "manual", days=30)

    try:
        await bot.send_message(pay_doc["user_id"], "🎉 **Your payment has been approved!** 30 days Premium activated.")
    except Exception:
        pass

    await call.message.edit_caption(caption=f"{call.message.caption}\n\n✅ **APPROVED BY ADMIN**")

@router.callback_query(F.data.startswith("reject_pay_"))
async def cb_reject_payment(call: CallbackQuery):
    if call.from_user.id not in config.ADMIN_IDS:
        return

    pay_id = call.data.replace("reject_pay_", "")
    await db.payments.update_one({"payment_id": pay_id}, {"$set": {"status": "rejected"}})
    await call.message.edit_caption(caption=f"{call.message.caption}\n\n❌ **REJECTED BY ADMIN**")

# --- ADMIN PANEL & MANAGEMENT ---
@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        return

    total_users = await db.users.count_documents({})
    total_files = await db.files.count_documents({})
    active_subs = await db.subscriptions.count_documents({"expires_at": {"$gt": datetime.datetime.utcnow()}})

    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent()

    text = (
        f"⚙️ **ADMIN DASHBOARD**\n\n"
        f"👥 **Total Users**: {total_users}\n"
        f"🎬 **Total Files**: {total_files}\n"
        f"💎 **Active Premium**: {active_subs}\n\n"
        f"🧠 **RAM Usage**: {mem.percent}% ({round(mem.used/(1024**2), 1)} MB)\n"
        f"⚙️ **CPU Usage**: {cpu}%\n"
        f"⏱ **Uptime**: {int(time.time() - START_TIME)}s"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [make_btn("📢 Broadcast Message", callback_data="admin_broadcast")],
        [make_btn("📤 Export DB", callback_data="admin_export_db"), make_btn("📥 Import DB", callback_data="admin_import_db")],
        [make_btn("🔧 Maintenance Mode", callback_data="admin_toggle_maint")]
    ])
    await message.answer(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "admin_toggle_maint")
async def cb_admin_toggle_maint(call: CallbackQuery):
    if call.from_user.id not in config.ADMIN_IDS:
        return
    settings = await db.settings.find_one({"_id": "global"}) or {}
    curr = not settings.get("maintenance", False)
    await db.settings.update_one({"_id": "global"}, {"$set": {"maintenance": curr}})
    await call.answer(f"Maintenance mode set to: {curr}", show_alert=True)

@router.callback_query(F.data == "admin_export_db")
async def cb_admin_export_db(call: CallbackQuery):
    if call.from_user.id not in config.ADMIN_IDS:
        return

    users = await db.users.find({}, {"_id": 0}).to_list(None)
    files = await db.files.find({}, {"_id": 0}).to_list(None)
    subs = await db.subscriptions.find({}, {"_id": 0}).to_list(None)

    backup_data = {
        "users": users,
        "files": files,
        "subscriptions": subs,
        "exported_at": datetime.datetime.utcnow().isoformat()
    }

    json_bytes = json.dumps(backup_data, default=str).encode("utf-8")
    doc = BufferedInputFile(json_bytes, filename="database_backup.json")
    await bot.send_document(call.from_user.id, doc, caption="📦 **MongoDB JSON Database Backup**")

# --- BACKGROUND EXPIRY TASK ---
async def subscription_expiry_scheduler():
    while True:
        try:
            now = datetime.datetime.utcnow()
            # Find newly expired subscriptions
            expired_subs = db.subscriptions.find({"expires_at": {"$lte": now}, "is_processed": {"$ne": True}})
            async for sub in expired_subs:
                user_id = sub["user_id"]
                # Deactivate user files
                await db.files.update_many(
                    {"owner_user_id": user_id, "access_type": "premium"},
                    {"$set": {"is_active": False}}
                )
                await db.subscriptions.update_one({"_id": sub["_id"]}, {"$set": {"is_processed": True}})
                try:
                    await bot.send_message(user_id, "⚠️ **Your Premium subscription has expired.**\nYour files are safe in storage! Renew Premium anytime to reactivate them.")
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Expiry scheduler error: {e}")
        await asyncio.sleep(300) # Run every 5 minutes

# ==========================================
# 4. APPLICATION LAUNCHER
# ==========================================
async def main():
    global db
    db = DatabaseManager(config.MONGO_URI, config.DATABASE_NAME)
    await db.init_indexes()

    # Web App Engine for HTTP Streaming
    http_manager = HTTPServerManager(bot, db)
    runner = web.AppRunner(http_manager.app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.PORT)
    await site.start()
    logger.info(f"HTTP Streaming Server running on port {config.PORT}")

    # Launch Expiry Scheduler Task
    asyncio.create_task(subscription_expiry_scheduler())

    # Start Aiogram Polling
    logger.info("Starting Telegram Bot Engine...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
