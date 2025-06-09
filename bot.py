from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure, CollectionInvalid, DuplicateKeyError
from flask import Flask
from threading import Thread
import os
import re
from datetime import datetime, UTC, timedelta
import asyncio
import urllib.parse
from fuzzywuzzy import process
from concurrent.futures import ThreadPoolExecutor

# Configs - নিশ্চিত করুন এই ভেরিয়েবলগুলো আপনার এনভায়রনমেন্টে সেট করা আছে।
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
# CHANNEL_ID এখন আর সরাসরি @app.on_message(filters.chat(CHANNEL_ID)) এর জন্য ব্যবহার করা হচ্ছে না
# এটি শুধুমাত্র গ্লোবাল নোটিফিকেশনের জন্য ব্যবহার করা যেতে পারে, যদি আপনার প্রয়োজন হয়।
# তবে sync_channels ফাংশন সব সংযুক্ত চ্যানেল থেকে ডেটা আনবে।
CHANNEL_ID = int(os.getenv("CHANNEL_ID")) 
RESULTS_COUNT = int(os.getenv("RESULTS_COUNT", 10))
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(",")))
DATABASE_URL = os.getenv("DATABASE_URL")
UPDATE_CHANNEL = os.getenv("UPDATE_CHANNEL", "https://t.me/CTGMovieOfficial")
START_PIC = os.getenv("START_PIC", "https://i.ibb.co/prnGXMr3/photo-2025-05-16-05-15-45-7504908428624527364.jpg")

app = Client("movie_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# MongoDB setup
mongo = MongoClient(DATABASE_URL)
db = mongo["movie_bot"]
movies_col = db["movies"]
feedback_col = db["feedback"]
stats_col = db["stats"]
users_col = db["users"]
settings_col = db["settings"]
requests_col = db["requests"]
connected_channels_col = db["connected_channels"] # নতুন কালেকশন

# Indexing - Optimized for faster search
try:
    movies_col.drop_index("message_id_1")
    print("Existing 'message_id_1' index dropped successfully (if it existed).")
except Exception as e:
    if "index not found" not in str(e):
        print(f"Error dropping existing index 'message_id_1': {e}")
    else:
        print("'message_id_1' index not found, proceeding with creation.")

try:
    # `message_id` এবং `source_channel_id` এর উপর কম্পোজিট ইউনিক ইনডেক্স
    movies_col.create_index([("message_id", ASCENDING), ("source_channel_id", ASCENDING)], unique=True, background=True)
    print("Compound index 'message_id_source_channel_id' (unique) ensured successfully.")
except DuplicateKeyError as e:
    print(f"Error: Cannot create unique index on 'message_id' and 'source_channel_id' due to duplicate entries. "
          f"Please clean your database manually if this persists. Error: {e}")
except OperationFailure as e:
    print(f"Error creating index 'message_id_source_channel_id': {e}")

movies_col.create_index("language", background=True)
movies_col.create_index([("title_clean", ASCENDING)], background=True)
movies_col.create_index([("language", ASCENDING), ("title_clean", ASCENDING)], background=True)
movies_col.create_index([("views_count", ASCENDING)], background=True)
print("All other necessary indexes for movies_col ensured successfully.")

# Indexing for connected_channels_col
try:
    connected_channels_col.create_index("channel_id", unique=True, background=True)
    print("Index 'channel_id' (unique) ensured successfully for connected_channels_col.")
except DuplicateKeyError as e:
    print(f"Error: Cannot create unique index on 'channel_id' due to duplicate entries in connected_channels_col. Error: {e}")
except OperationFailure as e:
    print(f"Error creating index 'channel_id' for connected_channels_col: {e}")


# Flask App for health check
flask_app = Flask(__name__)
@flask_app.route("/")
def home():
    return "Bot is running!"
Thread(target=lambda: flask_app.run(host="0.0.0.0", port=8080)).start()

# Initialize a global ThreadPoolExecutor for running blocking functions (like fuzzywuzzy)
thread_pool_executor = ThreadPoolExecutor(max_workers=5)

# Helpers
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9]', '', text.lower())

def extract_language(text):
    langs = ["Bengali", "Hindi", "English"]
    return next((lang for lang in langs if lang.lower() in text.lower()), None)

def extract_year(text):
    match = re.search(r'\b(19|20)\d{2}\b', text)
    return int(match.group(0)) if match else None

async def delete_message_later(chat_id, message_id, delay=300): # ডিলে 300 সেকেন্ড (5 মিনিট) সেট করা হয়েছে
    await asyncio.sleep(delay)
    try:
        await app.delete_messages(chat_id, message_id)
    except Exception as e:
        if "MESSAGE_ID_INVALID" not in str(e) and "MESSAGE_DELETE_FORBIDDEN" not in str(e) and "MESSAGE_NOT_MODIFIED" not in str(e):
            print(f"Error deleting message {message_id} in chat {chat_id}: {e}")

def find_corrected_matches(query_clean, all_movie_titles_data, score_cutoff=70, limit=5):
    if not all_movie_titles_data:
        return []

    choices = [item["title_clean"] for item in all_movie_titles_data]

    matches_raw = process.extract(query_clean, choices, limit=limit)

    corrected_suggestions = []
    for matched_clean_title, score in matches_raw:
        if score >= score_cutoff:
            for movie_data in all_movie_titles_data:
                if movie_data["title_clean"] == matched_clean_title:
                    corrected_suggestions.append({
                        "title": movie_data["original_title"],
                        "message_id": movie_data["message_id"],
                        "language": movie_data["language"],
                        "views_count": movie_data.get("views_count", 0),
                        "source_channel_id": movie_data["source_channel_id"]
                    })
                    break
    return corrected_suggestions

# Global dictionary to keep track of last start command time per user
user_last_start_time = {}

# --- Movie Saving Logic ---
async def process_and_save_message(msg: Message, source_channel_id: int):
    """Processes a message and saves movie data to the database."""
    text = msg.text or msg.caption
    if not text:
        return

    movie_to_save = {
        "message_id": msg.id,
        "source_channel_id": source_channel_id, # কোন চ্যানেল থেকে এসেছে তা ট্র্যাক করুন
        "title": text,
        "date": msg.date,
        "year": extract_year(text),
        "language": extract_language(text),
        "title_clean": clean_text(text),
        "views_count": 0,
        "likes": 0,
        "dislikes": 0,
        "rated_by": []
    }

    # Use update_one with upsert=True and a composite key for message_id and source_channel_id
    result = movies_col.update_one(
        {"message_id": msg.id, "source_channel_id": source_channel_id},
        {"$set": movie_to_save},
        upsert=True
    )

    if result.upserted_id is not None:
        print(f"New movie saved from channel {source_channel_id}: {text.splitlines()[0][:50]}...")
        # গ্লোবাল নোটিফিকেশন লজিক
        setting = settings_col.find_one({"key": "global_notify"})
        if setting and setting.get("value"):
            for user in users_col.find({"notify": {"$ne": False}}):
                try:
                    m = await app.send_message(
                        user["_id"],
                        f"🎬 নতুন মুভি আপলোড হয়েছে:\n**{text.splitlines()[0][:100]}**\nএখনই সার্চ করে দেখুন!",
                        disable_web_page_preview=True
                    )
                    asyncio.create_task(delete_message_later(m.chat.id, m.id))
                    await asyncio.sleep(0.05) # Rate limit for sending notifications
                except Exception as e:
                    if "PEER_ID_INVALID" in str(e) or "USER_IS_BOT" in str(e) or "USER_DEACTIVATED_REQUIRED" in str(e) or "BOT_BLOCKED" in str(e) or "USER_BLOCKED_BOT" in str(e):
                        print(f"Skipping notification to invalid/blocked user {user['_id']}: {e}")
                    else:
                        print(f"Failed to send notification to user {user['_id']}: {e}")

# --- Background Channel Sync Task ---
async def sync_channels():
    """Periodically syncs messages from all connected channels."""
    while True:
        connected_channels = list(connected_channels_col.find({"is_active": True}))
        if not connected_channels:
            print("No active channels connected. Waiting...")
            await asyncio.sleep(300) # Wait 5 minutes if no channels are connected
            continue

        for ch in connected_channels:
            channel_id = ch["channel_id"]
            last_synced_message_id = ch.get("last_synced_message_id", 0)

            print(f"Attempting to sync channel {channel_id} from message_id {last_synced_message_id}...")
            try:
                new_messages_count = 0
                temp_last_synced_id = last_synced_message_id

                async for message in app.get_history(channel_id, offset_id=last_synced_message_id, reverse=True, limit=50): # Fetch in chunks
                    if message.id > temp_last_synced_id: # Ensure we process only truly new messages
                        await process_and_save_message(message, channel_id)
                        temp_last_synced_id = message.id
                        new_messages_count += 1
                        await asyncio.sleep(0.05) # Small delay to avoid hitting API limits

                if new_messages_count > 0:
                    connected_channels_col.update_one(
                        {"channel_id": channel_id},
                        {"$set": {"last_synced_message_id": temp_last_synced_id, "last_synced_date": datetime.now(UTC)}}
                    )
                    print(f"Finished syncing channel {channel_id}. Saved {new_messages_count} new messages. Last synced ID: {temp_last_synced_id}")
                else:
                    print(f"No new messages found in channel {channel_id}.")

            except Exception as e:
                print(f"Error syncing channel {channel_id}: {e}")
                if "CHANNEL_PRIVATE" in str(e) or "CHAT_ID_INVALID" in str(e) or "PEER_ID_INVALID" in str(e) or "User not found" in str(e):
                    print(f"Channel {channel_id} is private, invalid, or bot was removed. Deactivating.")
                    connected_channels_col.update_one(
                        {"channel_id": channel_id},
                        {"$set": {"is_active": False, "status_note": f"Deactivated due to error: {e}"}}
                    )
                elif "MESSAGE_ID_INVALID" in str(e) and last_synced_message_id == 0:
                    print(f"Channel {channel_id} has no messages or invalid message_id 0. Setting last_synced_message_id to 1 to proceed.")
                    connected_channels_col.update_one(
                        {"channel_id": channel_id},
                        {"$set": {"last_synced_message_id": 1}}
                    )
                else:
                    pass
            await asyncio.sleep(10) # Delay between processing each connected channel

        await asyncio.sleep(300) # Wait 5 minutes before re-syncing all channels again

# Start the sync task when the bot is ready
@app.on_ready
async def on_ready_callback(client):
    print("Bot is ready. Starting channel sync task...")
    asyncio.create_task(sync_channels())


# --- Telegram Message Handlers ---

@app.on_message(filters.command("start"))
async def start(_, msg: Message):
    user_id = msg.from_user.id
    current_time = datetime.now(UTC)

    # Rate limiting for /start command
    if user_id in user_last_start_time:
        time_since_last_start = current_time - user_last_start_time[user_id]
        if time_since_last_start < timedelta(seconds=3): # 3 second cooldown
            print(f"User {user_id} sent /start too quickly. Ignoring.")
            return

    user_last_start_time[user_id] = current_time

    if len(msg.command) > 1 and msg.command[1].startswith("watch_"):
        parts = msg.command[1].replace("watch_", "").split("_")
        if len(parts) != 2:
            error_msg = await msg.reply_text("লিঙ্কটি অবৈধ।")
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
            return
        
        message_id = int(parts[0])
        source_channel_id = int(parts[1])

        try:
            copied_message = await app.copy_message(
                chat_id=msg.chat.id,
                from_chat_id=source_channel_id,
                message_id=message_id,
                protect_content=True
            )
            
            movie_data = movies_col.find_one({"message_id": message_id, "source_channel_id": source_channel_id})
            if movie_data:
                likes_count = movie_data.get('likes', 0)
                dislikes_count = movie_data.get('dislikes', 0)
                
                rating_buttons = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(f"👍 লাইক ({likes_count})", callback_data=f"like_{message_id}_{source_channel_id}_{user_id}"),
                        InlineKeyboardButton(f"👎 ডিসলাইক ({dislikes_count})", callback_data=f"dislike_{message_id}_{source_channel_id}_{user_id}")
                    ]
                ])
                rating_message = await app.send_message(
                    chat_id=msg.chat.id,
                    text="মুভিটি কেমন লাগলো? রেটিং দিন:",
                    reply_markup=rating_buttons,
                    reply_to_message_id=copied_message.id
                )
                asyncio.create_task(delete_message_later(rating_message.chat.id, rating_message.id))
                asyncio.create_task(delete_message_later(copied_message.chat.id, copied_message.id))

            movies_col.update_one(
                {"message_id": message_id, "source_channel_id": source_channel_id},
                {"$inc": {"views_count": 1}}
            )

        except Exception as e:
            error_msg = await msg.reply_text("মুভিটি খুঁজে পাওয়া যায়নি বা লোড করা যায়নি। এটি ব্যক্তিগত বা আপনার প্রবেশাধিকার নেই।")
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
            print(f"Error copying message from start payload: {e}")
        return

    users_col.update_one(
        {"_id": msg.from_user.id},
        {"$set": {"joined": datetime.now(UTC), "notify": True}},
        upsert=True
    )
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("আপডেট চ্যানেল", url=UPDATE_CHANNEL)],
        [InlineKeyboardButton("অ্যাডমিনের সাথে যোগাযোগ", url="https://t.me/ctgmovies23")]
    ])
    start_message = await msg.reply_photo(photo=START_PIC, caption="আমাকে মুভির নাম লিখে পাঠান, আমি খুঁজে দেবো।", reply_markup=btns)
    asyncio.create_task(delete_message_later(start_message.chat.id, start_message.id))

@app.on_message(filters.command("feedback") & filters.private)
async def feedback(_, msg: Message):
    if len(msg.command) < 2:
        error_msg = await msg.reply("অনুগ্রহ করে /feedback এর পর আপনার মতামত লিখুন।")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return
    feedback_col.insert_one({
        "user": msg.from_user.id,
        "text": msg.text.split(None, 1)[1],
        "time": datetime.now(UTC)
    })
    m = await msg.reply("আপনার মতামতের জন্য ধন্যবাদ!")
    asyncio.create_task(delete_message_later(m.chat.id, m.id))

@app.on_message(filters.command("broadcast") & filters.user(ADMIN_IDS))
async def broadcast(_, msg: Message):
    if len(msg.command) < 2:
        error_msg = await msg.reply("ব্যবহার: /broadcast আপনার মেসেজ এখানে")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return
    count = 0
    message_to_send = msg.text.split(None, 1)[1]
    for user in users_col.find():
        try:
            await app.send_message(user["_id"], message_to_send)
            count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            if "PEER_ID_INVALID" in str(e) or "USER_IS_BLOCKED" in str(e) or "USER_BOT" in str(e) or "USER_DEACTIVATED_REQUIRED" in str(e) or "USER_BLOCKED_BOT" in str(e):
                print(f"Skipping broadcast to invalid/blocked user {user['_id']}: {e}")
            else:
                print(f"Failed to broadcast to user {user['_id']}: {e}")
    reply_msg = await msg.reply(f"{count} জন ব্যবহারকারীর কাছে ব্রডকাস্ট পাঠানো হয়েছে।")
    asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))

@app.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def stats(_, msg: Message):
    stats_msg = await msg.reply(
        f"মোট ব্যবহারকারী: {users_col.count_documents({})}\n"
        f"মোট মুভি: {movies_col.count_documents({})}\n"
        f"মোট সংযুক্ত চ্যানেল: {connected_channels_col.count_documents({})}\n"
        f"মোট ফিডব্যাক: {feedback_col.count_documents({})}\n"
        f"মোট অনুরোধ: {requests_col.count_documents({})}"
    )
    asyncio.create_task(delete_message_later(stats_msg.chat.id, stats_msg.id))

@app.on_message(filters.command("notify") & filters.user(ADMIN_IDS))
async def notify_command(_, msg: Message):
    if len(msg.command) != 2 or msg.command[1] not in ["on", "off"]:
        error_msg = await msg.reply("ব্যবহার: /notify on অথবা /notify off")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return
    new_value = True if msg.command[1] == "on" else False
    settings_col.update_one(
        {"key": "global_notify"},
        {"$set": {"value": new_value}},
        upsert=True
    )
    status = "চালু" if new_value else "বন্ধ"
    reply_msg = await msg.reply(f"✅ গ্লোবাল নোটিফিকেশন {status} করা হয়েছে!")
    asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))

@app.on_message(filters.command("delete_movie") & filters.user(ADMIN_IDS))
async def delete_specific_movie(_, msg: Message):
    if len(msg.command) < 2:
        error_msg = await msg.reply("অনুগ্রহ করে মুভির টাইটেল দিন। ব্যবহার: `/delete_movie <মুভির টাইটেল>`")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return
    
    movie_title_to_delete = msg.text.split(None, 1)[1].strip()
    
    movie_to_delete = movies_col.find_one({"title": {"$regex": re.escape(movie_title_to_delete), "$options": "i"}})

    if not movie_to_delete:
        cleaned_title_to_delete = clean_text(movie_title_to_delete)
        movie_to_delete = movies_col.find_one({"title_clean": {"$regex": f"^{re.escape(cleaned_title_to_delete)}$", "$options": "i"}})

    if movie_to_delete:
        movies_col.delete_one({"_id": movie_to_delete["_id"]})
        reply_msg = await msg.reply(f"মুভি **{movie_to_delete['title']}** সফলভাবে ডিলিট করা হয়েছে।")
        asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))
    else:
        error_msg = await msg.reply(f"**{movie_title_to_delete}** নামের কোনো মুভি খুঁজে পাওয়া যায়নি।")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))

@app.on_message(filters.command("delete_all_movies") & filters.user(ADMIN_IDS))
async def delete_all_movies_command(_, msg: Message):
    confirmation_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("হ্যাঁ, সব ডিলিট করুন", callback_data="confirm_delete_all_movies")],
        [InlineKeyboardButton("না, বাতিল করুন", callback_data="cancel_delete_all_movies")]
    ])
    reply_msg = await msg.reply("আপনি কি নিশ্চিত যে আপনি ডাটাবেস থেকে **সব মুভি** ডিলিট করতে চান? এই প্রক্রিয়াটি অপরিবর্তনীয়!", reply_markup=confirmation_button)
    asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))

# --- New Admin Commands for Channel Management ---
@app.on_message(filters.command("connect") & filters.user(ADMIN_IDS))
async def connect_channel(_, msg: Message):
    if len(msg.command) < 2:
        error_msg = await msg.reply("অনুগ্রহ করে চ্যানেলের আইডি দিন। ব্যবহার: `/connect <channel_id>`\n"
                                     "চ্যানেল আইডি -100 দিয়ে শুরু হয়, যেমন: `-1001234567890`")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return

    try:
        channel_id = int(msg.command[1])
    except ValueError:
        error_msg = await msg.reply("অনুগ্রহ করে একটি বৈধ চ্যানেলের আইডি দিন (পূর্ণসংখ্যা)।")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return

    if connected_channels_col.find_one({"channel_id": channel_id}):
        error_msg = await msg.reply(f"এই চ্যানেল (`{channel_id}`) ইতিমধ্যেই সংযুক্ত আছে।")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return

    try:
        # বট চ্যানেলের সদস্য কিনা এবং তার পর্যাপ্ত অনুমতি আছে কিনা তা যাচাই করুন
        chat_member = await app.get_chat_member(channel_id, app.me.id)
        if not chat_member.can_read_messages:
            error_msg = await msg.reply(f"বটকে চ্যানেল `{channel_id}` এর সদস্য হতে হবে এবং মেসেজ পড়ার অনুমতি থাকতে হবে।")
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
            return
        
        # চ্যানেলের তথ্য পেতে
        chat_info = await app.get_chat(channel_id)
        channel_title = chat_info.title
        
        connected_channels_col.insert_one({
            "channel_id": channel_id,
            "channel_name": channel_title,
            "added_by": msg.from_user.id,
            "added_date": datetime.now(UTC),
            "last_synced_message_id": 0, # Initial sync from start
            "is_active": True,
            "status_note": "Active"
        })
        
        success_msg = await msg.reply(f"চ্যানেল **{channel_title}** (`{channel_id}`) সফলভাবে সংযুক্ত হয়েছে। "
                                       "নতুন পোস্ট স্বয়ংক্রিয়ভাবে সেভ হবে।")
        asyncio.create_task(delete_message_later(success_msg.chat.id, success_msg.id))

    except Exception as e:
        error_msg_text = f"চ্যানেল সংযোগ করতে সমস্যা হয়েছে: {e}\n"
        if "CHAT_ID_INVALID" in str(e) or "PEER_ID_INVALID" in str(e):
            error_msg_text = f"ভুল চ্যানেল আইডি। নিশ্চিত করুন আইডি সঠিক এবং বট চ্যানেলের সদস্য।: {e}"
        elif "CHANNEL_PRIVATE" in str(e):
            error_msg_text = f"চ্যানেলটি ব্যক্তিগত। নিশ্চিত করুন বটকে অ্যাডমিন হিসেবে যোগ করা হয়েছে।: {e}"
        
        error_msg = await msg.reply(error_msg_text)
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        print(f"Error connecting channel {channel_id}: {e}")

@app.on_message(filters.command("disconnect") & filters.user(ADMIN_IDS))
async def disconnect_channel(_, msg: Message):
    if len(msg.command) < 2:
        error_msg = await msg.reply("অনুগ্রহ করে চ্যানেলের আইডি দিন। ব্যবহার: `/disconnect <channel_id>`")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return

    try:
        channel_id = int(msg.command[1])
    except ValueError:
        error_msg = await msg.reply("অনুগ্রহ করে একটি বৈধ চ্যানেলের আইডি দিন (পূর্ণসংখ্যা)।")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return
    
    confirm_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("হ্যাঁ, ডিসকানেক্ট করুন", callback_data=f"confirm_disconnect_{channel_id}")],
        [InlineKeyboardButton("না, বাতিল করুন", callback_data=f"cancel_disconnect_{channel_id}")]
    ])

    reply_msg = await msg.reply(f"আপনি কি নিশ্চিত যে আপনি চ্যানেল `{channel_id}` ডিসকানেক্ট করতে চান?", reply_markup=confirm_markup)
    asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id, delay=60))

@app.on_message(filters.command("list_connected") & filters.user(ADMIN_IDS))
async def list_connected_channels(_, msg: Message):
    connected_channels = list(connected_channels_col.find({}))
    if not connected_channels:
        reply_msg = await msg.reply("কোনো চ্যানেল সংযুক্ত নেই।")
        asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))
        return

    list_text = "🔗 **সংযুক্ত চ্যানেলসমূহ:**\n\n"
    for ch in connected_channels:
        list_text += f"**নাম:** {ch.get('channel_name', 'N/A')}\n"
        list_text += f"**আইডি:** `{ch['channel_id']}`\n"
        list_text += f"**সংযুক্ত করেছেন:** `{ch.get('added_by', 'N/A')}`\n"
        list_text += f"**শেষ সিঙ্ক আইডি:** `{ch.get('last_synced_message_id', 'N/A')}`\n"
        list_text += f"**সক্রিয়:** {'✅ হ্যাঁ' if ch.get('is_active', False) else '❌ না'}\n"
        list_text += f"**স্ট্যাটাস:** {ch.get('status_note', 'N/A')}\n\n"
    
    reply_msg = await msg.reply(list_text)
    asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))


@app.on_callback_query(filters.regex(r"^noresult_(wrong|notyet|uploaded|coming)_(\d+)_([^ ]+)$") & filters.user(ADMIN_IDS))
async def handle_admin_reply(_, cq: CallbackQuery):
    parts = cq.data.split("_", 3)
    reason = parts[1]
    user_id = int(parts[2])
    encoded_query = parts[3]
    original_query = urllib.parse.unquote_plus(encoded_query)

    messages = {
        "wrong": f"❌ আপনি **'{original_query}'** নামে ভুল সার্চ করেছেন। অনুগ্রহ করে সঠিক নাম লিখে আবার চেষ্টা করুন।",
        "notyet": f"⏳ **'{original_query}'** মুভিটি এখনো আমাদের কাছে আসেনি। অনুগ্রহ করে কিছু সময় পর আবার চেষ্টা করুন।",
        "uploaded": f"📤 **'{original_query}'** মুভিটি ইতিমধ্যে আপলোড করা হয়েছে। সঠিক নামে আবার সার্চ করুন।",
        "coming": f"🚀 **'{original_query}'** মুভিটি খুব শিগগিরই আমাদের চ্যানেলে আসবে। অনুগ্রহ করে অপেক্ষা করুন."
    }

    try:
        m_sent = await app.send_message(user_id, messages[reason])
        asyncio.create_task(delete_message_later(m_sent.chat.id, m_sent.id))
        await cq.answer("ব্যবহারকারীকে জানানো হয়েছে ✅", show_alert=True)
        await cq.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"✅ উত্তর দেওয়া হয়েছে: {messages[reason].split(' ')[0]}", callback_data="noop")
        ]]))
    except Exception as e:
        await cq.answer("ব্যবহারকারীকে মেসেজ পাঠানো যায়নি ❌", show_alert=True)
        print(f"Error sending admin reply to user {user_id}: {e}")

@app.on_message(filters.command("popular") & (filters.private | filters.group))
async def popular_movies(_, msg: Message):
    popular_movies_list = list(movies_col.find(
        {"views_count": {"$exists": True}}
    ).sort("views_count", -1).limit(RESULTS_COUNT))

    if popular_movies_list:
        buttons = []
        for movie in popular_movies_list:
            if "title" in movie and "message_id" in movie and "source_channel_id" in movie:
                buttons.append([
                    InlineKeyboardButton(
                        text=f"{movie['title'][:40]} ({movie.get('views_count', 0)} ভিউ)",
                        url=f"https://t.me/{app.me.username}?start=watch_{movie['message_id']}_{movie['source_channel_id']}"
                    )
                ])
        
        reply_markup = InlineKeyboardMarkup(buttons)
        m = await msg.reply_text(
            "🔥 বর্তমানে সবচেয়ে জনপ্রিয় মুভিগুলো:\n\n",
            reply_markup=reply_markup,
            quote=True
        )
        asyncio.create_task(delete_message_later(m.chat.id, m.id))
    else:
        m = await msg.reply_text("দুঃখিত, বর্তমানে কোনো জনপ্রিয় মুভি পাওয়া যায়নি।", quote=True)
        asyncio.create_task(delete_message_later(m.chat.id, m.id))

@app.on_message(filters.command("request") & filters.private)
async def request_movie(_, msg: Message):
    if len(msg.command) < 2:
        error_msg = await msg.reply("অনুগ্রহ করে /request এর পর মুভির নাম লিখুন। উদাহরণ: `/request The Creator`", quote=True)
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return
    
    movie_name = msg.text.split(None, 1)[1].strip()
    user_id = msg.from_user.id
    username = msg.from_user.username or msg.from_user.first_name

    requests_col.insert_one({
        "user_id": user_id,
        "username": username,
        "movie_name": movie_name,
        "request_time": datetime.now(UTC),
        "status": "pending"
    })

    m = await msg.reply(f"আপনার অনুরোধ **'{movie_name}'** সফলভাবে জমা দেওয়া হয়েছে। এডমিনরা এটি পর্যালোচনা করবেন।", quote=True)
    asyncio.create_task(delete_message_later(m.chat.id, m.id))

    encoded_movie_name = urllib.parse.quote_plus(movie_name)
    admin_request_btns = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ সম্পন্ন হয়েছে", callback_data=f"req_fulfilled_{user_id}_{encoded_movie_name}"),
        InlineKeyboardButton("❌ বাতিল করা হয়েছে", callback_data=f"req_rejected_{user_id}_{encoded_movie_name}")
    ]])

    for admin_id in ADMIN_IDS:
        try:
            await app.send_message(
                admin_id,
                f"❗ *নতুন মুভির অনুরোধ!*\n\n"
                f"🎬 মুভির নাম: `{movie_name}`\n"
                f"👤 ইউজার: [{username}](tg://user?id={user_id}) (`{user_id}`)",
                reply_markup=admin_request_btns,
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"Could not notify admin {admin_id} about request: {e}")

@app.on_message(filters.text & (filters.group | filters.private))
async def search(_, msg: Message):
    query = msg.text.strip()
    if not query:
        return

    if msg.chat.type == "group":
        if len(query) < 3:
            return
        if msg.reply_to_message or msg.from_user.is_bot:
            return
        if not re.search(r'[a-zA-Z0-9]', query):
            return

    user_id = msg.from_user.id
    users_col.update_one(
        {"_id": user_id},
        {"$set": {"last_query": query}, "$setOnInsert": {"joined": datetime.now(UTC)}},
        upsert=True
    )

    loading_message = await msg.reply("🔎 লোড হচ্ছে, অনুগ্রহ করে অপেক্ষা করুন...", quote=True)
    
    query_clean = clean_text(query)
    
    matched_movies_direct = list(movies_col.find(
        {"$or": [
            {"title_clean": {"$regex": f"^{re.escape(query_clean)}", "$options": "i"}},
            {"title": {"$regex": re.escape(query), "$options": "i"}}
        ]}
    ).limit(RESULTS_COUNT))

    if matched_movies_direct:
        try:
            await loading_message.delete()
        except Exception as e:
            print(f"Could not delete loading message: {e}")

        buttons = []
        for movie in matched_movies_direct:
            if "title" in movie and "message_id" in movie and "source_channel_id" in movie:
                buttons.append([
                    InlineKeyboardButton(
                        text=f"{movie['title'][:40]} ({movie.get('views_count', 0)} ভিউ)",
                        url=f"https://t.me/{app.me.username}?start=watch_{movie['message_id']}_{movie['source_channel_id']}"
                    )
                ])
        
        m = await msg.reply("🎬 নিচের রেজাল্টগুলো পাওয়া গেছে:", reply_markup=InlineKeyboardMarkup(buttons), quote=True)
        asyncio.create_task(delete_message_later(m.chat.id, m.id))
        return

    all_movie_data_cursor = movies_col.find(
        {"title_clean": {"$regex": query_clean, "$options": "i"}},
        {"title_clean": 1, "original_title": "$title", "message_id": 1, "language": 1, "views_count": 1, "source_channel_id": 1}
    ).limit(200)

    all_movie_data = list(all_movie_data_cursor)

    corrected_suggestions = await asyncio.get_event_loop().run_in_executor(
        thread_pool_executor,
        find_corrected_matches,
        query_clean,
        all_movie_data,
        70,
        RESULTS_COUNT
    )

    try:
        await loading_message.delete()
    except Exception as e:
        print(f"Could not delete loading message: {e}")

    if corrected_suggestions:
        buttons = []
        for movie in corrected_suggestions:
            buttons.append([
                InlineKeyboardButton(
                    text=f"{movie['title'][:40]} ({movie.get('views_count', 0)} ভিউ)",
                    url=f"https://t.me/{app.me.username}?start=watch_{movie['message_id']}_{movie['source_channel_id']}"
                )
            ])
        
        lang_buttons = [
            InlineKeyboardButton("বেঙ্গলি", callback_data=f"lang_Bengali_{query_clean}"),
            InlineKeyboardButton("হিন্দি", callback_data=f"lang_Hindi_{query_clean}"),
            InlineKeyboardButton("ইংলিশ", callback_data=f"lang_English_{query_clean}")
        ]
        buttons.append(lang_buttons)

        m = await msg.reply("🔍 সরাসরি মিলে যায়নি, তবে কাছাকাছি কিছু পাওয়া গেছে:", reply_markup=InlineKeyboardMarkup(buttons), quote=True)
        asyncio.create_task(delete_message_later(m.chat.id, m.id))
    else:
        Google_Search_url = "https://www.google.com/search?q=" + urllib.parse.quote(query)
        
        request_button = InlineKeyboardButton("এই মুভির জন্য অনুরোধ করুন", callback_data=f"request_movie_{user_id}_{urllib.parse.quote_plus(query)}")
        google_button_row = [InlineKeyboardButton("গুগলে সার্চ করুন", url=Google_Search_url)]
        
        reply_markup_for_no_result = InlineKeyboardMarkup([
            google_button_row,
            [request_button]
        ])

        alert = await msg.reply_text( 
            """
❌ দুঃখিত! আপনার খোঁজা মুভিটি খুঁজে পাওয়া যায়নি।

যদি মুভির নামটি ভুল হয়ে থাকে, তাহলে আপনি নিচের বাটনে ক্লিক করে Google থেকে সঠিক নাম দেখে নিতে পারেন।

অথবা, আপনার পছন্দের মুভিটি আমাদের কাছে অনুরোধ করতে পারেন।
""",
            reply_markup=reply_markup_for_no_result,
            quote=True
        )
        asyncio.create_task(delete_message_later(alert.chat.id, alert.id))

        encoded_query = urllib.parse.quote_plus(query)
        admin_btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ ভুল নাম", callback_data=f"noresult_wrong_{user_id}_{encoded_query}"),
            InlineKeyboardButton("⏳ এখনো আসেনি", callback_data=f"noresult_notyet_{user_id}_{encoded_query}")
        ], [
            InlineKeyboardButton("📤 আপলোড আছে", callback_data=f"noresult_uploaded_{user_id}_{encoded_query}"),
            InlineKeyboardButton("🚀 শিগগির আসবে", callback_data=f"noresult_coming_{user_id}_{encoded_query}")
        ]])

        for admin_id in ADMIN_IDS:
            try:
                await app.send_message(
                    admin_id,
                    f"❗ *নতুন মুভি খোঁজা হয়েছে কিন্তু পাওয়া যায়নি!*\n\n"
                    f"🔍 অনুসন্ধান: `{query}`\n"
                    f"👤 ইউজার: [{msg.from_user.first_name}](tg://user?id={user_id}) (`{user_id}`)",
                    reply_markup=admin_btns,
                    disable_web_page_preview=True
                )
            except Exception as e:
                print(f"Could not notify admin {admin_id}: {e}")

@app.on_callback_query()
async def callback_handler(_, cq: CallbackQuery):
    data = cq.data

    if data == "confirm_delete_all_movies":
        movies_col.delete_many({})
        reply_msg = await cq.message.edit_text("✅ ডাটাবেস থেকে সব মুভি সফলভাবে ডিলিট করা হয়েছে।")
        asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))
        await cq.answer("সব মুভি ডিলিট করা হয়েছে।")
    elif data == "cancel_delete_all_movies":
        reply_msg = await cq.message.edit_text("❌ সব মুভি ডিলিট করার প্রক্রিয়া বাতিল করা হয়েছে।")
        asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))
        await cq.answer("বাতিল করা হয়েছে।")
    
    elif data.startswith("confirm_disconnect_"):
        channel_id = int(data.split("_")[2])
        result = connected_channels_col.delete_one({"channel_id": channel_id})
        if result.deleted_count > 0:
            success_msg = await cq.message.edit_text(f"চ্যানেল `{channel_id}` সফলভাবে ডিসকানেক্ট করা হয়েছে।")
            asyncio.create_task(delete_message_later(success_msg.chat.id, success_msg.id))
        else:
            error_msg = await cq.message.edit_text(f"চ্যানেল `{channel_id}` সংযুক্ত ছিল না।")
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        await cq.answer("ডিসকানেক্ট করা হয়েছে।")
    
    elif data.startswith("cancel_disconnect_"):
        channel_id = int(data.split("_")[2])
        cancel_msg = await cq.message.edit_text(f"চ্যানেল `{channel_id}` ডিসকানেক্ট করার প্রক্রিয়া বাতিল করা হয়েছে।")
        asyncio.create_task(delete_message_later(cancel_msg.chat.id, cancel_msg.id))
        await cq.answer("বাতিল করা হয়েছে।")

    elif data.startswith("lang_"):
        _, lang, query_clean = data.split("_", 2)
        
        potential_lang_matches_cursor = movies_col.find(
            {"language": lang, "title_clean": {"$regex": query_clean, "$options": "i"}},
            {"title": 1, "message_id": 1, "title_clean": 1, "views_count": 1, "source_channel_id": 1}
        ).limit(50)

        potential_lang_matches = list(potential_lang_matches_cursor)
        
        fuzzy_data_for_matching_lang = [
            {"title_clean": m["title_clean"], "original_title": m["title"], "message_id": m["message_id"],
             "language": lang, "views_count": m.get("views_count", 0), "source_channel_id": m["source_channel_id"]}
            for m in potential_lang_matches
        ]
        
        loop = asyncio.get_running_loop()
        matches_filtered_by_lang = await loop.run_in_executor(
            thread_pool_executor,
            find_corrected_matches,
            query_clean,
            fuzzy_data_for_matching_lang,
            70,
            RESULTS_COUNT
        )

        if matches_filtered_by_lang:
            buttons = []
            for m in matches_filtered_by_lang[:RESULTS_COUNT]:
                buttons.append([InlineKeyboardButton(f"{m['title'][:40]} ({m.get('views_count',0)} ভিউ)", url=f"https://t.me/{app.me.username}?start=watch_{m['message_id']}_{m['source_channel_id']}")])
            reply_msg = await cq.message.edit_text(
                f"ফলাফল ({lang}) - নিচের থেকে সিলেক্ট করুন:",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))
        else:
            await cq.answer("এই ভাষায় কিছু পাওয়া যায়নি।", show_alert=True)
        await cq.answer()

    elif data.startswith("request_movie_"):
        _, user_id_str, encoded_movie_name = data.split("_", 2)
        user_id = int(user_id_str)
        movie_name = urllib.parse.unquote_plus(encoded_movie_name)
        username = cq.from_user.username or cq.from_user.first_name

        requests_col.insert_one({
            "user_id": user_id,
            "username": username,
            "movie_name": movie_name,
            "request_time": datetime.now(UTC),
            "status": "pending"
        })
        
        await cq.answer(f"আপনার অনুরোধ '{movie_name}' সফলভাবে জমা দেওয়া হয়েছে।", show_alert=True)
        
        admin_request_btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ সম্পন্ন হয়েছে", callback_data=f"req_fulfilled_{user_id}_{encoded_movie_name}"),
            InlineKeyboardButton("❌ বাতিল করা হয়েছে", callback_data=f"req_rejected_{user_id}_{encoded_movie_name}")
        ]])

        for admin_id in ADMIN_IDS:
            try:
                await app.send_message(
                    admin_id,
                    f"❗ *নতুন মুভির অনুরোধ (ইনলাইন বাটন থেকে)!*\n\n"
                    f"🎬 মুভির নাম: `{movie_name}`\n"
                    f"👤 ইউজার: [{username}](tg://user?id={user_id}) (`{user_id}`)",
                    reply_markup=admin_request_btns,
                    disable_web_page_preview=True
                )
            except Exception as e:
                print(f"Could not notify admin {admin_id} about request from callback: {e}")
        
        try:
            edited_msg = await cq.message.edit_text(
                f"❌ দুঃখিত! আপনার খোঁজা মুভিটি খুঁজে পাওয়া যায়নি।\n\n"
                f"আপনার অনুরোধ **'{movie_name}'** জমা দেওয়া হয়েছে। এডমিনরা এটি পর্যালোচনা করবেন।",
                reply_markup=None
            )
            asyncio.create_task(delete_message_later(edited_msg.chat.id, edited_msg.id))
        except Exception as e:
            print(f"Error editing user message after request: {e}")

    elif data.startswith("like_") or data.startswith("dislike_"):
        parts = data.split("_", 3)
        if len(parts) < 4:
            await cq.answer("অকার্যকর রেটিং ডেটা।", show_alert=True)
            return

        action, message_id_str, source_channel_id_str, user_id_str = parts
        movie_message_id = int(message_id_str)
        source_channel_id = int(source_channel_id_str)
        user_id = int(user_id_str)

        movie = movies_col.find_one({"message_id": movie_message_id, "source_channel_id": source_channel_id})
        
        if not movie:
            await cq.answer("দুঃখিত, এই মুভিটি খুঁজে পাওয়া যায়নি।", show_alert=True)
            return

        if user_id in movie.get("rated_by", []):
            await cq.answer("আপনি ইতিমধ্যেই এই মুভিতে রেটিং দিয়েছেন!", show_alert=True)
            return

        update_query = {"$inc": {}, "$push": {"rated_by": user_id}}
        if action == "like":
            update_query["$inc"]["likes"] = 1
        elif action == "dislike":
            update_query["$inc"]["dislikes"] = 1
        
        movies_col.update_one({"message_id": movie_message_id, "source_channel_id": source_channel_id}, update_query)
        
        updated_movie = movies_col.find_one({"message_id": movie_message_id, "source_channel_id": source_channel_id})
        updated_likes = updated_movie.get('likes', 0)
        updated_dislikes = updated_movie.get('dislikes', 0)

        new_rating_buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"👍 লাইক ({updated_likes})", callback_data="noop"),
                InlineKeyboardButton(f"👎 ডিসলাইক ({updated_dislikes})", callback_data="noop")
            ]
        ])

        try:
            await cq.message.edit_reply_markup(reply_markup=new_rating_buttons)
            await cq.answer("আপনার রেটিং রেকর্ড করা হয়েছে! ধন্যবাদ।", show_alert=True)
        except Exception as e:
            print(f"Error editing message after rating: {e}")
            await cq.answer("রেটিং আপডেট করতে সমস্যা হয়েছে।", show_alert=True)

    elif "_" in data:
        parts = data.split("_", 3)
        if len(parts) == 4 and parts[0] in ["has", "no", "soon", "wrong"]: 
            action, uid, mid, raw_query = parts
            uid = int(uid)
            responses = {
                "has": f"✅ @{cq.from_user.username or cq.from_user.first_name} জানিয়েছেন যে **{raw_query}** মুভিটি ডাটাবেজে আছে। সঠিক নাম লিখে আবার চেষ্টা করুন।",
                "no": f"❌ @{cq.from_user.username or cq.from_user.first_name} জানিয়েছেন যে **{raw_query}** মুভিটি ডাটাবেজে নেই।",
                "soon": f"⏳ @{cq.from_user.username or cq.from_user.first_name} জানিয়েছেন যে **{raw_query}** মুভিটি শীঘ্রই আসবে।",
                "wrong": f"✏️ @{cq.from_user.username or cq.from_user.first_name} বলছেন যে আপনি ভুল নাম লিখেছেন: **{raw_query}**।"
            }
            if action in responses:
                try:
                    m = await app.send_message(uid, responses[action])
                    asyncio.create_task(delete_message_later(m.chat.id, m.id))
                    await cq.answer("অ্যাডমিনের পক্ষ থেকে উত্তর পাঠানো হয়েছে।")
                except Exception as e:
                    await cq.answer("ইউজারকে বার্তা পাঠাতে সমস্যা হয়েছে।", show_alert=True)
                    print(f"Error sending admin feedback message: {e}")
            else:
                await cq.answer("অকার্যকর কলব্যাক ডেটা।", show_alert=True)
    
    elif data == "noop":
        await cq.answer()


if __name__ == "__main__":
    print("বট শুরু হচ্ছে...")
    app.run()
