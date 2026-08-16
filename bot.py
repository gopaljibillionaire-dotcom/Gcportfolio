import os
import asyncio
from aiohttp import web
from pyrogram import Client, filters
from pyrogram.types import Message
import config

# Initialize Pyrogram Bot Client
app = Client(
    "file_stream_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)

# Parse HTTP Range Headers for video seeking/fast-forwarding
def parse_range(range_header: str, file_size: int):
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

# HTTP Route Handler for streaming media up to 2 GB
async def stream_handler(request: web.Request) -> web.StreamResponse:
    try:
        chat_id = int(request.match_info["chat_id"])
        message_id = int(request.match_info["message_id"])

        message: Message = await app.get_messages(chat_id, message_id)
        media = message.document or message.video or message.audio or message.voice
        
        if not media:
            return web.Response(status=404, text="Media file not found.")

        file_size = media.file_size
        mime_type = getattr(media, "mime_type", "application/octet-stream") or "application/octet-stream"
        file_name = getattr(media, "file_name", f"file_{message_id}")

        range_header = request.headers.get("Range")
        
        if range_header:
            from_bytes, until_bytes = parse_range(range_header, file_size)
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

        # Stream media chunks directly from Telegram MTProto
        async for chunk in app.stream_media(message, offset=from_bytes, limit=length):
            await response.write(chunk)

        return response
    except Exception as e:
        return web.Response(status=500, text=str(e))

# Command Handler: Generate stream link for forwarded files
@app.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def generate_link(client: Client, message: Message):
    stream_url = f"{config.BASE_URL}/stream/{message.chat.id}/{message.id}"
    await message.reply_text(
        f"**File Stream Link Generated!**\n\n"
        f"🔗 **Download / Stream URL:**\n`{stream_url}`",
        disable_web_page_preview=True
    )

# Main Application Runner
async def main():
    await app.start()
    
    server = web.Application()
    server.router.add_get("/stream/{chat_id}/{message_id}", stream_handler)
    
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.PORT)
    await site.start()
    
    print(f"Server started on port {config.PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
