import os
import sys
import time
import math
import logging
import asyncio
import secrets
from datetime import datetime, timezone

# -------------------------------------------------------------------
# 1. Asyncio Event Loop Setup
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
    CallbackQuery,
)
from pyrogram.types import InlineKeyboardButton as PyrogramInlineKeyboardButton
from pyrogram.errors import UserNotParticipant, PeerIdInvalid, UsernameNotOccupied
from motor.motor_asyncio import AsyncIOMotorClient
import config

# -------------------------------------------------------------------
# Custom Button Wrapper (Safely passes style without Pyrogram errors)
# -------------------------------------------------------------------
def InlineKeyboardButton(text: str, callback_data: str = None, url: str = None, style: str = None, **kwargs):
    btn_kwargs = {}
    if callback_data is not None:
        btn_kwargs["callback_data"] = callback_data
    if url is not None:
        btn_kwargs["url"] = url
    for k, v in kwargs.items():
        if k in ["web_app", "login_url", "user_id", "switch_inline_query", "switch_inline_query_current_chat", "switch_inline_query_chosen_chat", "callback_game", "pay"]:
            btn_kwargs[k] = v
    return PyrogramInlineKeyboardButton(text=text, **btn_kwargs)

# -------------------------------------------------------------------
# 2. Logging Setup
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
# 3. Database Layer (MongoDB Async Motor)
# -------------------------------------------------------------------
mongo_client = AsyncIOMotorClient(config.MONGO_URI)
db = mongo_client[config.DATABASE_NAME]
users_col = db["users"]
files_col = db["files"]

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
    async def count_users() -> int:
        return await users_col.count_documents({})

    @staticmethod
    async def save_file(file_data: dict):
        await files_col.insert_one(file_data)

    @staticmethod
    async def count_files() -> int:
        return await files_col.count_documents({})

async def cleanup_database_indexes():
    try:
        await files_col.drop_index("download_token_1")
        logger.info("Successfully dropped legacy download_token_1 index.")
    except Exception:
        pass

loop.create_task(cleanup_database_indexes())

# -------------------------------------------------------------------
# 4. Pyrogram MTProto Client
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
# 5. Utilities & Force-Sub Helper
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
        time_list.append(int(result) if count < 4 else int(remainder))

    return "".join(
        f"{time_list[i]}{time_suffix_list[i]} "
        for i in range(len(time_list) - 1, -1, -1)
        if time_list[i] != 0
    )

async def check_force_sub(client: Client, user_id: int) -> bool:
    if not config.REQUIRED_CHANNEL_USERNAME:
        return True
    try:
        # Resolves via public username to prevent "Peer id invalid"
        member = await client.get_chat_member(f"@{config.REQUIRED_CHANNEL_USERNAME}", user_id)
        return member.status in [
            enums.ChatMemberStatus.OWNER,
            enums.ChatMemberStatus.ADMINISTRATOR,
            enums.ChatMemberStatus.MEMBER,
        ]
    except UserNotParticipant:
        return False
    except (PeerIdInvalid, UsernameNotOccupied) as e:
        logger.error(f"Force Sub Error ({user_id}): Username @{config.REQUIRED_CHANNEL_USERNAME} not recognized - {e}")
        return True
    except Exception as e:
        logger.error(f"Force Sub Error ({user_id}): {e}")
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
# 6. Web Server & Streaming Route Handlers
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
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background-color: #0f172a; color: #f8fafc; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
            .card {{ background: #1e293b; padding: 2.5rem; border-radius: 1rem; box-shadow: 0 10px 25px -5px rgba(0,0,0,0.3); max-width: 480px; width: 100%; border: 1px solid #334155; }}
            h1 {{ font-size: 1.5rem; margin-top: 0; color: #38bdf8; text-align: center; }}
            .status {{ display: flex; align-items: center; justify-content: center; gap: 0.5rem; color: #4ade80; font-weight: 600; margin-bottom: 2rem; }}
            .dot {{ height: 10px; width: 10px; background-color: #4ade80; border-radius: 50%; display: inline-block; box-shadow: 0 0 8px #4ade80; }}
            .stat-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1.5rem; }}
            .stat-box {{ background: #0f172a; padding: 1rem; border-radius: 0.5rem; text-align: center; border: 1px solid #334155; }}
            .stat-value {{ font-size: 1.25rem; font-weight: bold; color: #f8fafc; }}
            .stat-label {{ font-size: 0.85rem; color: #94a3b8; margin-top: 0.25rem; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>File Stream Service</h1>
            <div class="status"><span class="dot"></span> Online</div>
            <div class="stat-grid">
                <div class="stat-box"><div class="stat-value">{total_users}</div><div class="stat-label">Total Users</div></div>
                <div class="stat-box"><div class="stat-value">{total_files}</div><div class="stat-label">Files Streamed</div></div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{uptime}</div>
                <div class="stat-label">System Uptime</div>
            </div>
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
            return web.Response(status=404, text="File not found in Bin Channel.")

        media = (
            message.document
            or message.video
            or message.audio
            or message.voice
            or message.photo
        )
        if not media:
            return web.Response(status=404, text="No streamable media found.")

        file_size = getattr(media, "file_size", 0)
        raw_mime = getattr(media, "mime_type", None)
        mime_type = raw_mime if raw_mime and "/" in raw_mime else "video/mp4"
        file_name = getattr(media, "file_name", None) or f"video_{message_id}.mp4"

        range_header = request.headers.get("Range")
        if range_header:
            from_bytes, until_bytes = parse_range_header(range_header, file_size)
        else:
            from_bytes = 0
            until_bytes = file_size - 1

        length = until_bytes - from_bytes + 1
        status = 206 if range_header else 200

        headers = {
            "Content-Type": mime_type,
            "Content-Disposition": f'inline; filename="{file_name}"',
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Content-Length": str(length),
        }

        if range_header:
            headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

        if request.method == "HEAD":
            return web.Response(status=status, headers=headers)

        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)

        # Precise byte-slicing streaming engine
        current_pos = 0
        async for chunk in app.stream_media(message):
            chunk_len = len(chunk)
            chunk_end = current_pos + chunk_len - 1

            if current_pos <= until_bytes and chunk_end >= from_bytes:
                start_in_chunk = max(0, from_bytes - current_pos)
                end_in_chunk = min(chunk_len, until_bytes - current_pos + 1)
                await response.write(chunk[start_in_chunk:end_in_chunk])

            current_pos += chunk_len
            if current_pos > until_bytes:
                break

        return response
    except Exception as e:
        logger.error(f"Stream error: {e}")
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
        <title>Stream Player</title>
        <link href="https://vjs.zencdn.net/8.3.0/video-js.css" rel="stylesheet" />
        <style>
            body {{ margin: 0; padding: 0; background-color: #000; display: flex; justify-content: center; align-items: center; min-height: 100vh; }}
            .video-container {{ width: 100%; max-width: 1100px; padding: 10px; box-sizing: border-box; }}
            .video-js {{ width: 100%; height: 85vh; border-radius: 8px; overflow: hidden; }}
        </style>
    </head>
    <body>
        <div class="video-container">
            <video id="stream-player" class="video-js vjs-big-play-centered" controls preload="metadata" data-setup="{{}}">
                <source src="{stream_url}" type="video/mp4" />
                <p class="vjs-no-js">
                    To view this video please enable JavaScript, or consider upgrading to a web browser that supports HTML5 video.
                </p>
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
        return await message.reply_text("⛔ Your account is restricted.")

    if not await check_force_sub(client, user_id):
        invite_link = f"https://t.me/{config.REQUIRED_CHANNEL_USERNAME}"
        buttons = [
            [InlineKeyboardButton(text="Join Channel", url=invite_link, style="primary")],
            [InlineKeyboardButton(text="Try Again", callback_data="check_sub_none", style="success")],
        ]
        return await message.reply_text(
            "⚠️ You must join our channel to use this bot.",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    user_keyboard = [
        [
            InlineKeyboardButton(text="Help & Info", callback_data="btn_help", style="primary"),
            InlineKeyboardButton(text="Bot Stats", callback_data="btn_stats", style="success"),
        ],
        [
            InlineKeyboardButton(text="Join Channel", url=f"https://t.me/{config.REQUIRED_CHANNEL_USERNAME}", style="primary")
        ]
    ]

    if user_id in config.ADMIN_IDS:
        user_keyboard.append(
            [InlineKeyboardButton(text="Admin Dashboard", callback_data="admin_dashboard", style="danger")]
        )

    await message.reply_text(
        f"👋 **Welcome {name}!**\n\nSend or forward any file to get instant high-speed streaming & download links.",
        reply_markup=InlineKeyboardMarkup(user_keyboard),
    )

@app.on_message(filters.command("admin") & filters.private)
async def admin_panel_cmd(client: Client, message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        return

    admin_keyboard = [
        [
            InlineKeyboardButton(text="Live Analytics", callback_data="admin_stats", style="primary"),
            InlineKeyboardButton(text="System Health", callback_data="admin_sys_health", style="success")
        ],
        [
            InlineKeyboardButton(text="Close Panel", callback_data="btn_close", style="danger")
        ]
    ]

    await message.reply_text(
        "🎛️ **Admin Control Panel**\nSelect an operation below:",
        reply_markup=InlineKeyboardMarkup(admin_keyboard)
    )

@app.on_message(filters.private & (filters.document | filters.video | filters.audio | filters.voice))
async def handle_incoming_file(client: Client, message: Message):
    user_id = message.from_user.id
    name = message.from_user.first_name or "User"
    username = message.from_user.username

    await Database.add_user(user_id, name, username)

    if await Database.is_banned(user_id):
        return await message.reply_text("⛔ Your account is restricted.")

    if not await check_force_sub(client, user_id):
        invite_link = f"https://t.me/{config.REQUIRED_CHANNEL_USERNAME}"
        buttons = [
            [InlineKeyboardButton(text="Join Channel", url=invite_link, style="primary")],
            [InlineKeyboardButton(text="Refresh", callback_data="check_sub_none", style="success")],
        ]
        return await message.reply_text(
            "⚠️ Please join our channel first to process files.",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    proc_msg = await message.reply_text("⚡ Forwarding file to storage server...")

    try:
        log_msg = await message.forward(config.BIN_CHANNEL)
    except PeerIdInvalid:
        logger.error(f"BIN_CHANNEL Error: Peer ID {config.BIN_CHANNEL} invalid. Add @Filetostreamrobot as admin to the channel.")
        return await proc_msg.edit_text("❌ Error: Bot is not added as Administrator in the BIN_CHANNEL.")
    except Exception as e:
        logger.error(f"BIN_CHANNEL Forward Error: {e}")
        return await proc_msg.edit_text("❌ Error: Could not forward file to BIN_CHANNEL.")

    media = (
        log_msg.document
        or log_msg.video
        or log_msg.audio
        or log_msg.voice
    )

    file_name = getattr(media, "file_name", None) or f"video_{log_msg.id}.mp4"
    file_size = getattr(media, "file_size", 0)
    readable_size = get_readable_bytes(file_size)

    stream_url = f"{config.BASE_URL}/stream/{log_msg.chat.id}/{log_msg.id}"
    watch_url = f"{config.BASE_URL}/watch/{log_msg.chat.id}/{log_msg.id}"

    token = secrets.token_hex(8)
    file_record = {
        "file_id": f"{log_msg.chat.id}_{log_msg.id}",
        "chat_id": log_msg.chat.id,
        "message_id": log_msg.id,
        "user_id": user_id,
        "file_name": file_name,
        "file_size": file_size,
        "download_token": token,
        "created_at": datetime.now(timezone.utc),
        "downloads": 0,
    }
    await Database.save_file(file_record)

    action_buttons = [
        [
            InlineKeyboardButton(text="Watch Online", url=watch_url, style="success"),
            InlineKeyboardButton(text="Direct Download", url=stream_url, style="primary"),
        ]
    ]

    await proc_msg.edit_text(
        f"📄 **File Name:** `{file_name}`\n"
        f"📦 **File Size:** `{readable_size}`\n\n"
        f"🔗 **Stream Link:**\n`{watch_url}`\n\n"
        f"📥 **Download Link:**\n`{stream_url}`",
        reply_markup=InlineKeyboardMarkup(action_buttons),
        disable_web_page_preview=True,
    )

# -------------------------------------------------------------------
# 8. Callbacks
# -------------------------------------------------------------------
@app.on_callback_query()
async def callback_handler(client: Client, callback: CallbackQuery):
    data = callback.data
    user_id = callback.from_user.id

    if data == "admin_dashboard":
        if user_id not in config.ADMIN_IDS:
            return await callback.answer("Unauthorized access", show_alert=True)

        admin_keyboard = [
            [
                InlineKeyboardButton(text="Live Analytics", callback_data="admin_stats", style="primary"),
                InlineKeyboardButton(text="System Health", callback_data="admin_sys_health", style="success")
            ],
            [
                InlineKeyboardButton(text="Close Panel", callback_data="btn_close", style="danger")
            ]
        ]
        await callback.message.edit_text(
            "🎛️ **Admin Control Panel**\nSelect an operation below:",
            reply_markup=InlineKeyboardMarkup(admin_keyboard)
        )

    elif data == "admin_stats":
        if user_id not in config.ADMIN_IDS:
            return await callback.answer("Unauthorized", show_alert=True)

        total_users = await Database.count_users()
        total_files = await Database.count_files()
        uptime = get_readable_time(int(time.time() - BOT_START_TIME))

        back_btn = [[InlineKeyboardButton(text="Back to Admin", callback_data="admin_dashboard", style="primary")]]
        await callback.message.edit_text(
            f"📊 **Admin Analytics Dashboard**\n\n"
            f"👤 **Total Users:** `{total_users}`\n"
            f"📁 **Total Files Streamed:** `{total_files}`\n"
            f"⏱️ **System Uptime:** `{uptime}`\n"
            f"📡 **Storage Channel:** `{config.BIN_CHANNEL}`",
            reply_markup=InlineKeyboardMarkup(back_btn)
        )

    elif data == "admin_sys_health":
        if user_id not in config.ADMIN_IDS:
            return await callback.answer("Unauthorized", show_alert=True)
        back_btn = [[InlineKeyboardButton(text="Back to Admin", callback_data="admin_dashboard", style="primary")]]
        await callback.message.edit_text(
            f"⚙️ **System Health Status**\n\n"
            f"🟢 **Bot Status:** Active\n"
            f"🟢 **Web Server:** Active (Port {config.PORT})\n"
            f"🟢 **Database:** Connected",
            reply_markup=InlineKeyboardMarkup(back_btn)
        )

    elif data == "btn_help":
        back_btn = [[InlineKeyboardButton(text="Back", callback_data="btn_home", style="primary")]]
        await callback.message.edit_text(
            "📖 **Help Guide:**\n\nSend any video or file directly to this bot to generate fast direct download and streaming links.",
            reply_markup=InlineKeyboardMarkup(back_btn)
        )

    elif data == "btn_stats":
        total_users = await Database.count_users()
        total_files = await Database.count_files()
        back_btn = [[InlineKeyboardButton(text="Back", callback_data="btn_home", style="primary")]]
        await callback.message.edit_text(
            f"📊 **Public Statistics**\n\nTotal Users: `{total_users}`\nTotal Files: `{total_files}`",
            reply_markup=InlineKeyboardMarkup(back_btn)
        )

    elif data == "btn_home":
        name = callback.from_user.first_name or "User"
        user_keyboard = [
            [
                InlineKeyboardButton(text="Help & Info", callback_data="btn_help", style="primary"),
                InlineKeyboardButton(text="Bot Stats", callback_data="btn_stats", style="success"),
            ],
            [
                InlineKeyboardButton(text="Join Channel", url=f"https://t.me/{config.REQUIRED_CHANNEL_USERNAME}", style="primary")
            ]
        ]
        if user_id in config.ADMIN_IDS:
            user_keyboard.append(
                [InlineKeyboardButton(text="Admin Dashboard", callback_data="admin_dashboard", style="danger")]
            )
        await callback.message.edit_text(
            f"👋 **Welcome back {name}!**\nSend any file to start streaming.",
            reply_markup=InlineKeyboardMarkup(user_keyboard)
        )

    elif data == "btn_close":
        await callback.message.delete()

    elif data.startswith("check_sub_"):
        if await check_force_sub(client, user_id):
            await callback.answer("Verified!", show_alert=True)
            await callback.message.delete()
        else:
            await callback.answer("You have not joined the channel yet!", show_alert=True)

# -------------------------------------------------------------------
# 9. Server Startup
# -------------------------------------------------------------------
async def start_services():
    logger.info("Starting Pyrogram Client...")
    await app.start()
    bot_info = await app.get_me()
    logger.info(f"Bot active as @{bot_info.username}")

    web_app = web.Application()
    web_app.router.add_get("/", index_handler)
    web_app.router.add_route("*", "/stream/{chat_id}/{message_id}", stream_media_handler)
    web_app.router.add_get("/watch/{chat_id}/{message_id}", watch_player_handler)

    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.PORT)
    await site.start()

    logger.info(f"Web server running on port {config.PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        logger.info("Bot execution stopped.")
