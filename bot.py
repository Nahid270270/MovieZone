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
# IMPORTANT: API_ID, API_HASH, BOT_TOKEN, DATABASE_URL MUST be set.
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
RESULTS_COUNT = int(os.getenv("RESULTS_COUNT", 10))
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(",")))
DATABASE_URL = os.getenv("DATABASE_URL")
UPDATE_CHANNEL = os.getenv("UPDATE_CHANNEL", "https://t.me/CTGMovieOfficial")
START_PIC = os.getenv("START_PIC", "https://i.ibb.co/prnGXMr3/photo-2025-05-16-05-15-45-7504908428624527364.jpg")

# Essential variable check before proceeding
if not all([API_ID, API_HASH, BOT_TOKEN, DATABASE_URL]):
    raise ValueError("One or more essential environment variables (API_ID, API_HASH, BOT_TOKEN, DATABASE_URL) are not set! Please check your Render environment variables.")

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
connected_channels_col = db["connected_channels"]

# Indexing - Optimized for faster search
try:
    movies_col.drop_index("message_id_1") # Attempt to drop old index if it exists
    print("Existing 'message_id_1' index dropped successfully (if it existed).")
except Exception as e:
    if "index not found" not in str(e):
        print(f"Error dropping existing index 'message_id_1': {e}")
    else:
        print("'message_id_1' index not found, proceeding with creation.")

try:
    movies_col.create_index([("message_id", ASCENDING), ("channel_id", ASCENDING)], unique=True, background=True)
    print("Composite index 'message_id_channel_id' (unique) ensured successfully.")
except DuplicateKeyError as e:
    print(f"Error: Cannot create unique index on 'message_id' and 'channel_id' due to duplicate entries. "
          f"Please clean your database manually if this persists. Error: {e}")
except OperationFailure as e:
    print(f"Error creating composite index: {e}")

movies_col.create_index("language", background=True)
movies_col.create_index([("title_clean", ASCENDING)], background=True)
movies_col.create_index([("language", ASCENDING), ("title_clean", ASCENDING)], background=True)
movies_col.create_index([("views_count", ASCENDING)], background=True)
print("All other necessary indexes ensured successfully.")

# Connected channels collection indexing
try:
    connected_channels_col.create_index("channel_id", unique=True, background=True)
    print("Index 'channel_id' for connected_channels ensured successfully.")
except DuplicateKeyError as e:
    print(f"Error: Cannot create unique index on 'channel_id' for connected_channels due to duplicate entries. Error: {e}")
except OperationFailure as e:
    print(f"Error creating index 'channel_id' for connected_channels: {e}")


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

async def delete_message_later(chat_id, message_id, delay=300): # Default delay 300 seconds (5 minutes)
    await asyncio.sleep(delay)
    try:
        await app.delete_messages(chat_id, message_id)
    except Exception as e:
        if "MESSAGE_ID_INVALID" not in str(e) and "MESSAGE_DELETE_FORBIDDEN" not in str(e):
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
                        "views_count": movie_data.get("views_count", 0)
                    })
                    break
    return corrected_suggestions

# Global dictionary to keep track of last start command time per user
user_last_start_time = {}

# save_movie_to_db helper function (for use in both save_post and connect)
async def save_movie_to_db(msg: Message):
    text = msg.text or msg.caption
    if not text:
        return None

    movie_to_save = {
        "message_id": msg.id,
        "channel_id": msg.chat.id,
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

    try:
        result = movies_col.update_one(
            {"message_id": msg.id, "channel_id": msg.chat.id},
            {"$set": movie_to_save},
            upsert=True
        )
        if result.upserted_id:
            return "inserted"
        elif result.modified_count:
            return "updated"
        else:
            return "unchanged"
    except DuplicateKeyError:
        print(f"Skipping duplicate: Message ID {msg.id} from channel {msg.chat.id} already exists.")
        return "duplicate"
    except Exception as e:
        print(f"Error saving message {msg.id} from channel {msg.chat.id} to DB: {e}")
        return "error"


# dynamic_chat_filter logic
async def get_connected_channel_ids():
    channels = connected_channels_col.find({})
    return [channel["channel_id"] for channel in channels]

# This is the corrected filter function
async def dynamic_channel_check(filter_instance, client_instance, msg: Message):
    # Only process messages from channels/supergroups
    if msg.chat.type not in ["channel", "supergroup"]:
        return False
    connected_ids = await get_connected_channel_ids()
    return msg.chat.id in connected_ids

# Register the dynamic filter
dynamic_chat_filter = filters.create("DynamicChatFilter", dynamic_channel_check)


@app.on_message(dynamic_chat_filter) # Use the dynamic filter here
async def save_post(_, msg: Message):
    # This handler specifically for saving posts from connected channels
    # No need to check msg.chat.type or ADMIN_IDS here as the filter already handles it
    status = await save_movie_to_db(msg)
    if status == "inserted":
        setting = settings_col.find_one({"key": "global_notify"})
        if setting and setting.get("value"):
            for user in users_col.find({"notify": {"$ne": False}}):
                try:
                    m = await app.send_message(
                        user["_id"],
                        f"নতুন মুভি আপলোড হয়েছে:\n**{msg.text.splitlines()[0][:100]}**\nএখনই সার্চ করে দেখুন!"
                    )
                    asyncio.create_task(delete_message_later(m.chat.id, m.id))
                    await asyncio.sleep(0.05)
                except Exception as e:
                    if "PEER_ID_INVALID" in str(e) or "USER_IS_BOT" in str(e) or "USER_DEACTIVATED_REQUIRED" in str(e) or "USER_BLOCKED_BOT" in str(e):
                        print(f"Skipping notification to invalid/blocked user {user['_id']}: {e}")
                    else:
                        print(f"Failed to send notification to user {user['_id']}: {e}")


@app.on_message(filters.command("start"))
async def start(_, msg: Message):
    user_id = msg.from_user.id
    current_time = datetime.now(UTC)

    # Cooldown for /start command to prevent spamming
    if user_id in user_last_start_time:
        time_since_last_start = current_time - user_last_start_time[user_id]
        if time_since_last_start < timedelta(seconds=3): # 3 seconds cooldown
            print(f"User {user_id} sent /start too quickly. Ignoring.")
            return

    user_last_start_time[user_id] = current_time

    if len(msg.command) > 1 and msg.command[1].startswith("watch_"):
        message_id = int(msg.command[1].replace("watch_", ""))
        movie_data = movies_col.find_one({"message_id": message_id})

        if not movie_data or "channel_id" not in movie_data:
            error_msg = await msg.reply_text("মুভিটি খুঁজে পাওয়া যায়নি বা লোড করা যায়নি। (ডেটা অসম্পূর্ণ)")
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
            print(f"Movie data for message_id {message_id} is incomplete or not found.")
            return

        target_channel_id = movie_data["channel_id"]
        try:
            copied_message = await app.copy_message(
                chat_id=msg.chat.id,
                from_chat_id=target_channel_id,
                message_id=message_id,
                protect_content=True
            )

            likes_count = movie_data.get('likes', 0)
            dislikes_count = movie_data.get('dislikes', 0)

            rating_buttons = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(f"👍 লাইক ({likes_count})", callback_data=f"like_{message_id}_{user_id}"),
                    InlineKeyboardButton(f"👎 ডিসলাইক ({dislikes_count})", callback_data=f"dislike_{message_id}_{user_id}")
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
                {"message_id": message_id, "channel_id": target_channel_id},
                {"$inc": {"views_count": 1}}
            )

        except Exception as e:
            error_msg = await msg.reply_text("মুভিটি খুঁজে পাওয়া যায়নি বা লোড করা যায়নি।")
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
            if "PEER_ID_INVALID" in str(e) or "USER_IS_BLOCKED" in str(e) or "USER_BOT" in str(e) or "USER_DEACTIVATED_REQUIRED" in str(e):
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

    # Search by exact title first
    movie_to_delete = movies_col.find_one({"title": {"$regex": re.escape(movie_title_to_delete), "$options": "i"}})

    # If not found by exact title, try cleaned title
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
            if "title" in movie and "message_id" in movie:
                buttons.append([
                    InlineKeyboardButton(
                        text=f"{movie['title'][:40]} ({movie.get('views_count', 0)} ভিউ)",
                        url=f"https://t.me/{app.me.username}?start=watch_{movie['message_id']}"
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

    # Skip search if message is from a connected channel (handled by save_post)
    connected_ids = await get_connected_channel_ids()
    if msg.chat.type in ["channel", "supergroup"] and msg.chat.id in connected_ids:
        return

    # Specific checks for group chats to reduce unnecessary processing
    if msg.chat.type == "group":
        if len(query) < 3: # Minimum 3 characters for group search
            return
        if msg.reply_to_message or msg.from_user.is_bot: # Ignore replies or bot messages
            return
        if not re.search(r'[a-zA-Z0-9]', query): # Ignore messages without alphanumeric characters
            return

    user_id = msg.from_user.id
    users_col.update_one(
        {"_id": user_id},
        {"$set": {"last_query": query}, "$setOnInsert": {"joined": datetime.now(UTC)}},
        upsert=True
    )

    loading_message = await msg.reply("🔎 লোড হচ্ছে, অনুগ্রহ করে অপেক্ষা করুন...", quote=True)
    asyncio.create_task(delete_message_later(loading_message.chat.id, loading_message.id, delay=15)) # Shorter delay for loading message

    query_clean = clean_text(query)

    # Search for all movies, regardless of channel_id for user search
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
            print(f"Error deleting loading message: {e}")

        buttons = []
        for movie in matched_movies_direct:
            buttons.append([
                InlineKeyboardButton(
                    text=f"{movie['title'][:40]} ({movie.get('views_count', 0)} ভিউ)",
                    url=f"https://t.me/{app.me.username}?start=watch_{movie['message_id']}"
                )
            ])

        m = await msg.reply("🎬 নিচের রেজাল্টগুলো পাওয়া গেছে:", reply_markup=InlineKeyboardMarkup(buttons), quote=True)
        asyncio.create_task(delete_message_later(m.chat.id, m.id))
        return

    all_movie_data_cursor = movies_col.find(
        {"title_clean": {"$regex": query_clean, "$options": "i"}},
        {"title_clean": 1, "original_title": "$title", "message_id": 1, "language": 1, "views_count": 1}
    ).limit(100) # Limit for fuzzy search candidates

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
        print(f"Error deleting loading message: {e}")

    if corrected_suggestions:
        buttons = []
        for movie in corrected_suggestions:
            buttons.append([
                InlineKeyboardButton(
                    text=f"{movie['title'][:40]} ({movie.get('views_count', 0)} ভিউ)",
                    url=f"https://t.me/{app.me.username}?start=watch_{movie['message_id']}"
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

    elif data.startswith("movie_"):
        await cq.answer("মুভিটি ফরওয়ার্ড করার জন্য আমাকে ব্যক্তিগতভাবে মেসেজ করুন।", show_alert=True)

    elif data.startswith("lang_"):
        _, lang, query_clean = data.split("_", 2)

        potential_lang_matches_cursor = movies_col.find(
            {"language": lang, "title_clean": {"$regex": query_clean, "$options": "i"}},
            {"title": 1, "message_id": 1, "title_clean": 1, "views_count": 1}
        ).limit(50)

        potential_lang_matches = list(potential_lang_matches_cursor)

        fuzzy_data_for_matching_lang = [
            {"title_clean": m["title_clean"], "original_title": m["title"], "message_id": m["message_id"],
             "language": lang, "views_count": m.get("views_count", 0)}
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
                buttons.append([InlineKeyboardButton(f"{m['title'][:40]} ({m.get('views_count',0)} ভিউ)", url=f"https://t.me/{app.me.username}?start=watch_{m['message_id']}")])
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
        action, message_id_str, user_id_str = data.split("_", 2)
        movie_message_id = int(message_id_str)
        user_id = int(user_id_str)

        movie = movies_col.find_one({"message_id": movie_message_id})

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

        movies_col.update_one({"message_id": movie_message_id, "channel_id": movie["channel_id"]}, update_query)

        updated_movie = movies_col.find_one({"message_id": movie_message_id, "channel_id": movie["channel_id"]})
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

    elif data.startswith("req_fulfilled_") or data.startswith("req_rejected_"):
        action, user_id_str, encoded_movie_name = data.split("_", 2)
        user_id = int(user_id_str)
        movie_name = urllib.parse.unquote_plus(encoded_movie_name)

        status_text = "সম্পন্ন" if action == "req_fulfilled" else "বাতিল"
        message_to_user = f"আপনার অনুরোধ **'{movie_name}'** {status_text} করা হয়েছে।"

        requests_col.update_one(
            {"user_id": user_id, "movie_name": movie_name, "status": "pending"},
            {"$set": {"status": status_text.lower(), "admin_action_time": datetime.now(UTC)}}
        )

        try:
            await app.send_message(user_id, message_to_user)
            await cq.answer(f"ব্যবহারকারীকে জানানো হয়েছে: {status_text}", show_alert=True)
            await cq.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"✅ {status_text} হয়েছে", callback_data="noop")
            ]]))
        except Exception as e:
            await cq.answer("ব্যবহারকারীকে বার্তা পাঠানো যায়নি ❌", show_alert=True)
            print(f"Error sending request fulfillment/rejection message to user {user_id}: {e}")

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
        else:
            await cq.answer("অকার্যকর কলব্যাক ডেটা।", show_alert=True)


# নতুন ফিচার: চ্যানেল কানেক্ট এবং ডিসকানেক্ট
@app.on_message(filters.command("connect") & filters.user(ADMIN_IDS) & filters.private)
async def connect_channel(_, msg: Message):
    if len(msg.command) < 2:
        help_msg = await msg.reply(
            "ব্যবহার: `/connect <channel_id>`\n"
            "উদাহরণ: `/connect -1001234567890`"
        )
        asyncio.create_task(delete_message_later(help_msg.chat.id, help_msg.id))
        return

    try:
        channel_id = int(msg.command[1])
    except ValueError:
        error_msg = await msg.reply("অনুগ্রহ করে একটি সঠিক চ্যানেল আইডি দিন।")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return

    # Check if bot is admin in the channel
    try:
        chat_member = await app.get_chat_member(channel_id, app.me.id)
        if not chat_member.can_read_messages:
            error_msg = await msg.reply(
                "বট এই চ্যানেলের মেসেজ পড়তে পারছে না। নিশ্চিত করুন বট এই চ্যানেলে অ্যাডমিন এবং তার 'Read Messages' পারমিশন আছে।"
            )
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
            return
    except Exception as e:
        error_msg = await msg.reply(
            f"চ্যানেলের তথ্য পেতে ব্যর্থ বা বট চ্যানেলের সদস্য নয়। নিশ্চিত করুন চ্যানেল আইডি সঠিক এবং বট চ্যানেলে আছে।\nএরর: {e}"
        )
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return

    # Add channel to connected_channels_col
    try:
        connected_channels_col.update_one(
            {"channel_id": channel_id},
            {"$set": {"connected_time": datetime.now(UTC)}},
            upsert=True
        )
        conn_msg = await msg.reply(f"✅ চ্যানেল `{channel_id}` সফলভাবে কানেক্ট করা হয়েছে।")
        asyncio.create_task(delete_message_later(conn_msg.chat.id, conn_msg.id))
    except DuplicateKeyError:
        already_conn_msg = await msg.reply(f"চ্যানেল `{channel_id}` ইতিমধ্যেই কানেক্ট করা আছে।")
        asyncio.create_task(delete_message_later(already_conn_msg.chat.id, already_conn_msg.id))
        return

    # Start importing existing messages
    importing_msg = await msg.reply(f"চ্যানেল `{channel_id}` থেকে বিদ্যমান মুভি ইম্পোর্ট শুরু হচ্ছে... এতে সময় লাগতে পারে।")
    asyncio.create_task(delete_message_later(importing_msg.chat.id, importing_msg.id))

    imported_count = 0
    try:
        # Iterate through messages in the channel
        async for message in app.get_chat_history(channel_id):
            if message.text or message.caption:
                status = await save_movie_to_db(message)
                if status == "inserted":
                    imported_count += 1
            await asyncio.sleep(0.01) # Small delay to avoid FloodWait

    except Exception as e: # pyrogram.errors.FloodWait সহ সব এরর এখানে ধরা হবে
        if "FloodWait" in str(e):
            wait_time = int(re.search(r'(\d+)', str(e)).group(1)) if re.search(r'(\d+)', str(e)) else 10 # Default to 10 seconds
            flood_wait_msg = await msg.reply(f"FloodWait এর কারণে ইম্পোর্ট থামানো হয়েছে: {wait_time} সেকেন্ড অপেক্ষা করুন।")
            asyncio.create_task(delete_message_later(flood_wait_msg.chat.id, flood_wait_msg.id))
            await asyncio.sleep(wait_time)
        else:
            error_import_msg = await msg.reply(f"ইম্পোর্ট করার সময় একটি এরর হয়েছে: {e}")
            asyncio.create_task(delete_message_later(error_import_msg.chat.id, error_import_msg.id))

    finally:
        final_import_msg = await msg.reply(f"চ্যানেল `{channel_id}` থেকে `{imported_count}` টি মুভি সফলভাবে ইম্পোর্ট/আপডেট করা হয়েছে।")
        asyncio.create_task(delete_message_later(final_import_msg.chat.id, final_import_msg.id))


@app.on_message(filters.command("disconnect") & filters.user(ADMIN_IDS) & filters.private)
async def disconnect_channel(_, msg: Message):
    if len(msg.command) < 2:
        help_msg = await msg.reply(
            "ব্যবহার: `/disconnect <channel_id>`\n"
            "উদাহরণ: `/disconnect -1001234567890`"
        )
        asyncio.create_task(delete_message_later(help_msg.chat.id, help_msg.id))
        return

    try:
        channel_id = int(msg.command[1])
    except ValueError:
        error_msg = await msg.reply("অনুগ্রহ করে একটি সঠিক চ্যানেল আইডি দিন।")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return

    result = connected_channels_col.delete_one({"channel_id": channel_id})

    if result.deleted_count > 0:
        disc_msg = await msg.reply(f"✅ চ্যানেল `{channel_id}` সফলভাবে ডিসকানেক্ট করা হয়েছে। এই চ্যানেলের নতুন পোস্ট আর সেভ হবে না।")
        asyncio.create_task(delete_message_later(disc_msg.chat.id, disc_msg.id))
    else:
        not_found_msg = await msg.reply(f"চ্যানেল `{channel_id}` কানেক্টেড অবস্থায় খুঁজে পাওয়া যায়নি।")
        asyncio.create_task(delete_message_later(not_found_msg.chat.id, not_found_msg.id))


@app.on_message(filters.command("list_connected") & filters.user(ADMIN_IDS) & filters.private)
async def list_connected_channels(_, msg: Message):
    connected_channels = list(connected_channels_col.find({}))
    if not connected_channels:
        m = await msg.reply("কোনো চ্যানেল কানেক্ট করা নেই।")
        asyncio.create_task(delete_message_later(m.chat.id, m.id))
        return

    response_text = "কানেক্টেড চ্যানেলসমূহ:\n"
    for channel in connected_channels:
        response_text += f"- `{channel['channel_id']}` (কানেক্টেড: {channel.get('connected_time', 'N/A').strftime('%Y-%m-%d %H:%M:%S')})\n"

    m = await msg.reply(response_text)
    asyncio.create_task(delete_message_later(m.chat.id, m.id))


@app.on_message(filters.command("savemovie") & filters.user(ADMIN_IDS) & filters.private)
async def save_movie_from_other_channel(_, msg: Message):
    if len(msg.command) < 3 and not msg.reply_to_message:
        help_msg = await msg.reply(
            "ব্যবহার: `/savemovie <channel_id> <message_id>`\n"
            "উদাহরণ: `/savemovie -1001234567890 123`\n\n"
            "অথবা, অন্য চ্যানেলের কোনো পোস্টকে রিপ্লাই করে `/savemovie` কমান্ডটি ব্যবহার করুন।"
        )
        asyncio.create_task(delete_message_later(help_msg.chat.id, help_msg.id))
        return

    target_channel_id = None
    target_message_id = None

    if msg.reply_to_message:
        target_channel_id = msg.reply_to_message.chat.id
        target_message_id = msg.reply_to_message.id
    else:
        try:
            target_channel_id = int(msg.command[1])
            target_message_id = int(msg.command[2])
        except ValueError:
            error_msg = await msg.reply("অনুগ্রহ করে সঠিক চ্যানেল আইডি এবং মেসেজ আইডি দিন।")
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
            return

    try:
        target_message = await app.get_messages(target_channel_id, target_message_id)
        if not target_message:
            error_msg = await msg.reply("নির্দিষ্ট চ্যানেল বা মেসেজ আইডি খুঁজে পাওয়া যায়নি।")
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
            return

        text = target_message.text or target_message.caption
        if not text:
            error_msg = await msg.reply("এই মেসেজটিতে কোনো টেক্সট বা ক্যাপশন নেই, তাই সেভ করা যাচ্ছে না।")
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
            return

        status = await save_movie_to_db(target_message)

        if status == "inserted":
            success_msg = await msg.reply(
                f"✅ মুভিটি সফলভাবে ডাটাবেসে সেভ করা হয়েছে!\n"
                f"টাইটেল: `{text.splitlines()[0][:100]}`\n"
                f"চ্যানেল আইডি: `{target_channel_id}`\n"
                f"মেসেজ আইডি: `{target_message_id}`"
            )
            asyncio.create_task(delete_message_later(success_msg.chat.id, success_msg.id))
        elif status == "updated":
            success_msg = await msg.reply(
                f"🔄 মুভিটি ইতিমধ্যেই ডাটাবেসে ছিল এবং সফলভাবে আপডেট করা হয়েছে!\n"
                f"টাইটেল: `{text.splitlines()[0][:100]}`"
            )
            asyncio.create_task(delete_message_later(success_msg.chat.id, success_msg.id))
        elif status == "unchanged":
            info_msg = await msg.reply("মুভিটি ইতিমধ্যেই ডাataবেসে বিদ্যমান এবং কোনো পরিবর্তনের প্রয়োজন হয়নি।")
            asyncio.create_task(delete_message_later(info_msg.chat.id, info_msg.id))
        elif status == "duplicate":
            info_msg = await msg.reply("মুভিটি ইতিমধ্যেই ডাটাবেসে বিদ্যমান ছিল (ডুপ্লিকেট)।")
            asyncio.create_task(delete_message_later(info_msg.chat.id, info_msg.id))
        else: # status == "error"
            error_msg = await msg.reply(f"মুভি সেভ করতে ব্যর্থ: কিছু একটা ভুল হয়েছে।")
            asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))

    except Exception as e:
        error_msg = await msg.reply(f"মুভি সেভ করতে ব্যর্থ: {e}")
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        print(f"Error saving movie from other channel: {e}")


if __name__ == "__main__":
    print("বট শুরু হচ্ছে...")
    app.run()
