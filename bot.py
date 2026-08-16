import os
import sys
import time
import math
import logging
import asyncio
import re
from datetime import datetime, timezone

# -------------------------------------------------------------------
# 1. Asyncio Event Loop Fix (Crucial for Python 3.14+ on Heroku)
# -------------------------------------------------------------------
try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from aiohttp import web
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from pyrogram.errors import (
    UserNotParticipant,
    FloodWait,
    PeerIdInvalid,
    ChatAdminRequired,
)
from motor.motor_asyncio import AsyncIOMotorClient
import config

# -------------------------------------------------------------------
# 2. Logging Configuration
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(name)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("FileStreamBot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

BOT_START_TIME = time.time()

# -------------------------------------------------------------------
# 3. Database Initialization (MongoDB Async)
# -------------------------------------------------------------------
mongo_client = AsyncIOMotorClient(config.MONGO_URI)
db = mongo_client[config.DATABASE_NAME]
users_col = db["users"]
files_col = db["files"]
stats_col = db["stats"]


class Database:
    @staticmethod
    async def add_user(user_id: int, name: str, username: str = None):
        user = await users_col.find_one({"user_id": user_id})
        if not user:
            new_user = {
                "user_id": user_id,
                "name": name,
                "username": username,
                "banned": False,
                "created_at": datetime.now(timezone.utc),
                "last_active": datetime.now(timezone.utc),
            }
            await users_col.insert_one(new_user)
            logger.info(f"New user added to DB: {user_id} ({name})")
        else:
            await users_col.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "name": name,
                        "username": username,
                        "last_active": datetime.now(timezone.utc),
                    }
                },
            )

    @staticmethod
    async def is_banned(user_id: int) -> bool:
        user = await users_col.find_one({"user_id": user_id})
        return user.get("banned", False) if user else False

    @staticmethod
    async def ban_user(user_id: int):
        await users_col.update_one({"user_id": user_id}, {"$set": {"banned": True}})

    @staticmethod
    async def unban_user(user_id: int):
        await users_col.update_one({"user_id": user_id}, {"$set": {"banned": False}})

    @staticmethod
    async def get_all_users():
        return users_col.find({})

    @staticmethod
    async def count_users() -> int:
        return await users_col.count_documents({})

    @staticmethod
    async def save_file(file_data: dict):
        await files_col.insert_one(file_data)

    @staticmethod
    async def get_file(file_id: str):
        return await files_col.find_one({"file_id": file_id})

    @staticmethod
    async def increment_downloads(file_id: str):
        await files_col.update_one({"file_id": file_id}, {"$inc": {"downloads": 1}})

    @staticmethod
    async def count_files() -> int:
        return await files_col.count_documents({})


# -------------------------------------------------------------------
# 4. Pyrogram Client Setup
# -------------------------------------------------------------------
app = Client(
    "file_stream_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
    sleep_threshold=60,
    max_concurrent_transmissions=10,
)

# -------------------------------------------------------------------
# 5. Helper Functions & Utilities
# -------------------------------------------------------------------
def get_readable_bytes(size: int) -> str:
    if not size:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    s = round(size / p, 2)
    return f"{s} {units[i]}"


def get_readable_time(seconds: int) -> str:
    count = 0
    time_list = []
    time_suffix_list = ["s", "m", "h", "days"]
    while count < 4:
        count += 1
        remainder, result = divmod(seconds, 60) if count < 3 else divmod(seconds, 24)
        if count == 1:
            time_list.append(int(result))
        elif count == 2:
            time_list.append(int(result))
        elif count == 3:
            time_list.append(int(result))
        elif count == 4:
            time_list.append(int(remainder))

    return "".join(
        f"{time_list[i]}{time_suffix_list[i]} "
        for i in range(len(time_list) - 1, -1, -1)
        if time_list[i] != 0
    )


async def check_force_sub(client: Client, user_id: int) -> bool:
    if not config.REQUIRED_CHANNEL_ID:
        return True
    try:
        member = await client.get_chat_member(config.REQUIRED_CHANNEL_ID, user_id)
        if member.status in [
            enums.ChatMemberStatus.OWNER,
            enums.ChatMemberStatus.ADMINISTRATOR,
            enums.ChatMemberStatus.MEMBER,
        ]:
            return True
        return False
    except UserNotParticipant:
        return False
    except Exception as e:
        logger.error(f"Force Sub Error for {user_id}: {e}")
        return True


def parse_range_header(range_header: str, file_size: int):
    if not range_header or "=" not in range_header:
        return 0, file_size - 1
    unit, ranges = range_header.split("=", 1)
    if unit.strip().lower() != "bytes":
        return 0, file_size - 1

    start_str, end_str = ranges.split("-", 1)
    start = int(start_str) if start_str.strip() else 0
    end = int(end_str) if end_str.strip() else file_size - 1

    start = max(0, min(start, file_size - 1))
    end = max(start, min(end, file_size - 1))
    return start, end


# -------------------------------------------------------------------
# 6. HTTP Web Server Handlers (aiohttp)
# -------------------------------------------------------------------
async def index_handler(request: web.Request) -> web.Response:
    uptime = get_readable_time(int(time.time() - BOT_START_TIME))
    total_users = await Database.count_users()
    total_files = await Database.count_files()

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Telegram File Stream Engine</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #0f172a; color: #f8fafc; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
            .card {{ background: #1e293b; padding: 2.5rem; border-radius: 1rem; box-shadow: 0 10px 25px -5px rgba(0,0,0,0.3); max-width: 480px; width: 100%; border: 1px solid #334155; }}
            h1 {{ font-size: 1.5rem; margin-top: 0; color: #38bdf8; text-align: center; }}
            .status {{ display: flex; align-items: center; justify-content: center; gap: 0.5rem; color: #4ade80; font-weight: 600; margin-bottom: 2rem; }}
            .dot {{ height: 10px; width: 10px; background-color: #4ade80; border-radius: 50%; display: inline-block; box-shadow: 0 0 8px #4ade80; }}
            .stat-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1.5rem; }}
            .stat-box {{ background: #0f172a; padding: 1rem; border-radius: 0.5rem; text-align: center; border: 1px solid #334155; }}
            .stat-value {{ font-size: 1.25rem; font-weight: bold; color: #f8fafc; }}
            .stat-label {{ font-size: 0.85rem; color: #94a3b8; margin-top: 0.25rem; }}
            .footer {{ text-align: center; font-size: 0.8rem; color: #64748b; margin-top: 1.5rem; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>File Stream Service</h1>
            <div class="status"><span class="dot"></span> Operational</div>
            <div class="stat-grid">
                <div class="stat-box"><div class="stat-value">{total_users}</div><div class="stat-label">Total Users</div></div>
                <div class="stat-box"><div class="stat-value">{total_files}</div><div class="stat-label">Files Streamed</div></div>
            </div>
            <div class="stat-box" style="grid-column: span 2;">
                <div class="stat-value">{uptime}</div>
                <div class="stat-label">System Uptime</div>
            </div>
            <div class="footer">Powered by Pyrogram MTProto & aiohttp</div>
        </div>
    </body>
    </html>
    """
    return web.Response(text=html_content, content_type="text/html")


async def stream_media_handler(request: web.Request) -> web.StreamResponse:
    try:
        chat_id = int(request.match_info["chat_id"])
        message_id = int(request.match_info["message_id"])

        message: Message = await app.get_messages(chat_id, message_id)
        if not message or message.empty:
            return web.Response(status=444, text="Media not found or deleted.")

        media = (
            message.document
            or message.video
            or message.audio
            or message.voice
            or message.photo
        )

        if not media:
            return web.Response(status=404, text="No streamable media in message.")

        file_size = getattr(media, "file_size", 0)
        mime_type = (
            getattr(media, "mime_type", "application/octet-stream")
            or "application/octet-stream"
        )
        file_name = getattr(media, "file_name", f"stream_{message_id}.mp4")

        range_header = request.headers.get("Range")

        if range_header:
            from_bytes, until_bytes = parse_range_header(range_header, file_size)
            length = until_bytes - from_bytes + 1
            status = 206
            headers = {
                "Content-Type": mime_type,
                "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
                "Content-Length": str(length),
                "Content-Disposition": f'inline; filename="{file_name}"',
                "Accept-Ranges": "bytes",
            }
        else:
            from_bytes = 0
            until_bytes = file_size - 1
            length = file_size
            status = 200
            headers = {
                "Content-Type": mime_type,
                "Content-Length": str(length),
                "Content-Disposition": f'inline; filename="{file_name}"',
                "Accept-Ranges": "bytes",
            }

        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)

        # Stream directly using Pyrogram MTProto stream_media
        async for chunk in app.stream_media(
            message, offset=from_bytes, limit=length
        ):
            await response.write(chunk)

        return response
    except Exception as e:
        logger.error(f"Error during file stream: {e}")
        return web.Response(status=500, text=str(e))


async def watch_player_handler(request: web.Request) -> web.Response:
    chat_id = request.match_info["chat_id"]
    message_id = request.match_info["message_id"]

    stream_url = f"{config.BASE_URL}/stream/{chat_id}/{message_id}"

    html_player = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Web Video Player</title>
        <link href="https://vjs.zencdn.net/8.3.0/video-js.css" rel="stylesheet" />
        <style>
            body {{ margin: 0; padding: 0; background-color: #000; display: flex; justify-content: center; align-items: center; min-height: 100vh; }}
            .video-container {{ width: 100%; max-width: 1100px; max-height: 100vh; }}
            .video-js {{ width: 100%; height: 80vh; border-radius: 8px; overflow: hidden; }}
        </style>
    </head>
    <body>
        <div class="video-container">
            <video id="stream-player" class="video-js vjs-big-play-centered vjs-theme-city" controls preload="auto" data-setup="{{}}">
                <source src="{stream_url}" type="video/mp4" />
                <p class="vjs-no-js">To view this video please enable JavaScript, and consider upgrading to a web browser that supports HTML5 video.</p>
            </video>
        </div>
        <script src="https://vjs.zencdn.net/8.3.0/video.min.js"></script>
    </body>
    </html>
    """
    return web.Response(text=html_player, content_type="text/html")


# -------------------------------------------------------------------
# 7. Telegram Bot Commands & Handlers
# -------------------------------------------------------------------
@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    user_id = message.from_user.id
    name = message.from_user.first_name or "User"
    username = message.from_user.username

    await Database.add_user(user_id, name, username)

    if await Database.is_banned(user_id):
        return await message.reply_text("⛔ **Your account has been restricted.**")

    # Force Subscribe Check
    if not await check_force_sub(client, user_id):
        invite_link = f"https://t.me/{config.REQUIRED_CHANNEL_USERNAME}"
        buttons = [
            [InlineKeyboardButton("📢 Join Channel", url=invite_link)],
            [
                InlineKeyboardButton(
                    "🔄 Try Again",
                    callback_data=f"check_sub_{message.text.split()[1] if len(message.text.split()) > 1 else 'none'}",
                )
            ],
        ]
        return await message.reply_text(
            f"⚠️ **Access Denied!**\n\nYou must join our official channel to use this bot.\nJoin via the button below and tap **Try Again**.",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    # Deep Linking logic: /start stream_CHATID_MSGID
    text_args = message.text.split()
    if len(text_args) > 1:
        param = text_args[1]
        if param.startswith("stream_"):
            try:
                parts = param.split("_")
                c_id = int(parts[1])
                m_id = int(parts[2])

                stream_url = f"{config.BASE_URL}/stream/{c_id}/{m_id}"
                watch_url = f"{config.BASE_URL}/watch/{c_id}/{m_id}"

                buttons = [
                    [
                        InlineKeyboardButton("🚀 Fast Stream", url=watch_url),
                        InlineKeyboardButton("📥 Direct Download", url=stream_url),
                    ]
                ]

                return await message.reply_text(
                    f"🔗 **Your Requested File Links:**\n\n"
                    f"🌐 **Stream URL:** `{watch_url}`\n"
                    f"📥 **Download URL:** `{stream_url}`",
                    reply_markup=InlineKeyboardMarkup(buttons),
                )
            except Exception as e:
                logger.error(f"Deep link parsing error: {e}")
                return await message.reply_text("❌ **Invalid or expired stream link.**")

    # Standard Welcome Message
    buttons = [
        [
            InlineKeyboardButton("ℹ️ Help", callback_data="help_btn"),
            InlineKeyboardButton("📊 Stats", callback_data="stats_btn"),
        ],
        [InlineKeyboardButton("🌐 Official Channel", url=f"https://t.me/{config.REQUIRED_CHANNEL_USERNAME}")],
    ]
    await message.reply_text(
        f"👋 **Hello {name}!**\n\n"
        f"I am an advanced **File to Stream & Direct Download Bot**.\n"
        f"Send or forward any video, document, or audio file to get instant streaming links!\n\n"
        f"✨ *Supports video seeking and files up to 2 GB.*",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


@app.on_message(filters.private & (filters.document | filters.video | filters.audio | filters.voice))
async def handle_incoming_file(client: Client, message: Message):
    user_id = message.from_user.id
    name = message.from_user.first_name or "User"
    username = message.from_user.username

    await Database.add_user(user_id, name, username)

    if await Database.is_banned(user_id):
        return await message.reply_text("⛔ **Your account has been restricted.**")

    if not await check_force_sub(client, user_id):
        invite_link = f"https://t.me/{config.REQUIRED_CHANNEL_USERNAME}"
        buttons = [
            [InlineKeyboardButton("📢 Join Channel", url=invite_link)],
            [InlineKeyboardButton("🔄 Refresh", callback_data="check_sub_none")],
        ]
        return await message.reply_text(
            "⚠️ **Please join our channel first to process your file!**",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    media = (
        message.document
        or message.video
        or message.audio
        or message.voice
    )
    file_name = getattr(media, "file_name", f"file_{message.id}")
    file_size = getattr(media, "file_size", 0)
    readable_size = get_readable_bytes(file_size)

    processing_msg = await message.reply_text("⚡ **Generating Stream Links...**")

    stream_url = f"{config.BASE_URL}/stream/{message.chat.id}/{message.id}"
    watch_url = f"{config.BASE_URL}/watch/{message.chat.id}/{message.id}"

    # Store file metadata in MongoDB
    file_record = {
        "file_id": f"{message.chat.id}_{message.id}",
        "chat_id": message.chat.id,
        "message_id": message.id,
        "user_id": user_id,
        "file_name": file_name,
        "file_size": file_size,
        "created_at": datetime.now(timezone.utc),
        "downloads": 0,
    }
    await Database.save_file(file_record)

    buttons = [
        [
            InlineKeyboardButton("🌐 Watch Online", url=watch_url),
            InlineKeyboardButton("📥 Direct Download", url=stream_url),
        ],
        [
            InlineKeyboardButton(
                "📋 Share Link",
                url=f"https://t.me/share/url?url={config.BASE_URL}/start?startapp=stream_{message.chat.id}_{message.id}",
            )
        ],
    ]

    await processing_msg.edit_text(
        f"📄 **File Name:** `{file_name}`\n"
        f"📦 **File Size:** `{readable_size}`\n\n"
        f"🔗 **Stream Link:**\n`{watch_url}`\n\n"
        f"📥 **Download Link:**\n`{stream_url}`",
        reply_markup=InlineKeyboardMarkup(buttons),
        disable_web_page_preview=True,
    )


# -------------------------------------------------------------------
# 8. Admin Panel Commands
# -------------------------------------------------------------------
@app.on_message(filters.command("stats") & filters.private)
async def stats_command(client: Client, message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        return

    total_users = await Database.count_users()
    total_files = await Database.count_files()
    uptime = get_readable_time(int(time.time() - BOT_START_TIME))

    await message.reply_text(
        f"📊 **Bot Operational Statistics:**\n\n"
        f"👤 **Total Users:** `{total_users}`\n"
        f"📁 **Total Files Streamed:** `{total_files}`\n"
        f"⏱️ **Uptime:** `{uptime}`"
    )


@app.on_message(filters.command("ban") & filters.private)
async def ban_command(client: Client, message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        return

    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("Usage: `/ban <user_id>`")

    try:
        target_id = int(args[1])
        await Database.ban_user(target_id)
        await message.reply_text(f"✅ User `{target_id}` has been banned.")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@app.on_message(filters.command("unban") & filters.private)
async def unban_command(client: Client, message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        return

    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("Usage: `/unban <user_id>`")

    try:
        target_id = int(args[1])
        await Database.unban_user(target_id)
        await message.reply_text(f"✅ User `{target_id}` has been unbanned.")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_command(client: Client, message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        return

    if not message.reply_to_message:
        return await message.reply_text("Reply to a message to broadcast.")

    broadcast_msg = message.reply_to_message
    status_msg = await message.reply_text("🚀 **Starting Broadcast...**")

    users = await Database.get_all_users()
    total_users = await Database.count_users()

    success, failed, done = 0, 0, 0

    async for user in users:
        user_id = user["user_id"]
        try:
            await broadcast_msg.copy(chat_id=user_id)
            success += 1
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await broadcast_msg.copy(chat_id=user_id)
            success += 1
        except Exception:
            failed += 1

        done += 1
        if done % 20 == 0:
            await status_msg.edit_text(
                f"⏳ **Broadcasting...**\n\n"
                f"Progress: `{done}/{total_users}`\n"
                f"Success: `{success}` | Failed: `{failed}`"
            )

    await status_msg.edit_text(
        f"✅ **Broadcast Complete!**\n\n"
        f"Total: `{total_users}`\n"
        f"Success: `{success}`\n"
        f"Failed: `{failed}`"
    )


# -------------------------------------------------------------------
# 9. Callback Query Handlers
# -------------------------------------------------------------------
@app.on_callback_query()
async def callback_handler(client: Client, callback: CallbackQuery):
    data = callback.data

    if data == "help_btn":
        help_text = (
            "📖 **How to use this bot:**\n\n"
            "1️⃣ Send any Telegram video or document.\n"
            "2️⃣ Get direct downloadable & streaming links instantly.\n"
            "3️⃣ Open the watch link in Chrome, VLC, or MX Player for high-speed playback."
        )
        buttons = [[InlineKeyboardButton("🔙 Back", callback_data="home_btn")]]
        await callback.message.edit_text(
            help_text, reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif data == "home_btn":
        name = callback.from_user.first_name or "User"
        buttons = [
            [
                InlineKeyboardButton("ℹ️ Help", callback_data="help_btn"),
                InlineKeyboardButton("📊 Stats", callback_data="stats_btn"),
            ],
            [InlineKeyboardButton("📢 Channel", url=f"https://t.me/{config.REQUIRED_CHANNEL_USERNAME}")],
        ]
        await callback.message.edit_text(
            f"👋 **Welcome Back, {name}!**\nSend any file to begin streaming.",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    elif data == "stats_btn":
        total_users = await Database.count_users()
        total_files = await Database.count_files()
        buttons = [[InlineKeyboardButton("🔙 Back", callback_data="home_btn")]]
        await callback.message.edit_text(
            f"📊 **Public Stats:**\n\nUsers: `{total_users}`\nFiles Streamed: `{total_files}`",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    elif data.startswith("check_sub_"):
        param = data.split("_")[2]
        if await check_force_sub(client, callback.from_user.id):
            await callback.answer("✅ Channel membership verified!", show_alert=True)
            await callback.message.delete()
        else:
            await callback.answer(
                "❌ You have not joined the channel yet!", show_alert=True
            )


# -------------------------------------------------------------------
# 10. Server Application Startup
# -------------------------------------------------------------------
async def start_services():
    logger.info("Starting Pyrogram MTProto Client...")
    await app.start()
    bot_info = await app.get_me()
    logger.info(f"Bot started successfully as @{bot_info.username}")

    # Setup web application
    web_app = web.Application()
    web_app.router.add_get("/", index_handler)
    web_app.router.add_get("/stream/{chat_id}/{message_id}", stream_media_handler)
    web_app.router.add_get("/watch/{chat_id}/{message_id}", watch_player_handler)

    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.PORT)
    await site.start()

    logger.info(f"aiohttp Web Server listening on port {config.PORT}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        logger.info("Bot execution stopped manually.")
