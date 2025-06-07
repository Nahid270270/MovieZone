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
    movies_col.create_index("message_id", unique=True, background=True)
    print("Index 'message_id' (unique) ensured successfully.")
except DuplicateKeyError as e:
    print(f"Error: Cannot create unique index on 'message_id' due to duplicate entries. "
          f"Please clean your database manually if this persists. Error: {e}")
except OperationFailure as e:
    print(f"Error creating index 'message_id': {e}")

movies_col.create_index("language", background=True) 
movies_col.create_index([("title_clean", ASCENDING)], background=True)
movies_col.create_index([("language", ASCENDING), ("title_clean", ASCENDING)], background=True)
movies_col.create_index([("views_count", ASCENDING)], background=True)
print("All other necessary indexes ensured successfully.")

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
    # শুধু বর্ণমালা ও সংখ্যা রেখে অন্য সব ক্যারেক্টার বাদ দেওয়া এবং লোয়ারকেস করা
    return re.sub(r'[^a-zA-Z0-9]', '', text.lower())

def extract_language(text):
    # ভাষার নাম সনাক্তকরণের জন্য আরও শক্তিশালী লজিক
    text_lower = text.lower()
    if "bengali" in text_lower or "বাংলা" in text_lower or "বেঙ্গলি" in text_lower:
        return "Bengali"
    if "hindi" in text_lower or "হিন্দি" in text_lower:
        return "Hindi"
    if "english" in text_lower:
        return "English"
    # যদি কোনো ভাষা সনাক্ত না হয়, তাহলে "Others" হিসেবে সেভ করা
    return "Others"

def extract_year(text):
    match = re.search(r'\b(19|20)\d{2}\b', text)
    return int(match.group(0)) if match else None

async def delete_message_later(chat_id, message_id, delay=60): # ডিলে 60 সেকেন্ড (1 মিনিট) সেট করা হয়েছে
    await asyncio.sleep(delay)
    try:
        await app.delete_messages(chat_id, message_id)
    except Exception as e:
        if "MESSAGE_ID_INVALID" not in str(e) and "MESSAGE_DELETE_FORBIDDEN" not in str(e):
            print(f"Error deleting message {message_id} in chat {chat_id}: {e}")

def find_corrected_matches(query_clean, all_movie_titles_data, score_cutoff=55, limit=10): # স্কোর কাটঅফ 55 এ কমানো হয়েছে
    print(f"DEBUG: find_corrected_matches - Query: '{query_clean}', Total choices: {len(all_movie_titles_data)}")
    if not all_movie_titles_data:
        print("DEBUG: find_corrected_matches - No movie data provided.")
        return []
    
    # যদি query_clean খালি হয়, তাহলে fuzzywuzzy ব্যবহার করা সম্ভব নয়
    if not query_clean.strip():
        print("DEBUG: find_corrected_matches - query_clean is empty, cannot perform fuzzy matching.")
        return [] 

    choices = [item["title_clean"] for item in all_movie_titles_data]
    
    matches_raw = process.extract(query_clean, choices, limit=limit)
    print(f"DEBUG: find_corrected_matches - Raw matches: {matches_raw}")

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
    print(f"DEBUG: find_corrected_matches - Corrected suggestions ({len(corrected_suggestions)} found): {corrected_suggestions}")
    return corrected_suggestions

# Global dictionary to keep track of last start command time per user
user_last_start_time = {}

@app.on_message(filters.chat(CHANNEL_ID))
async def save_post(_, msg: Message):
    text = msg.text or msg.caption
    if not text:
        return

    movie_to_save = {
        "message_id": msg.id,
        "title": text,
        "date": msg.date,
        "year": extract_year(text),
        "language": extract_language(text), # এটি এখন শুধু 'Bengali', 'Hindi', 'English' সেভ করবে
        "title_clean": clean_text(text),
        "views_count": 0,
        "likes": 0,
        "dislikes": 0,
        "rated_by": []
    }
    
    result = movies_col.update_one({"message_id": msg.id}, {"$set": movie_to_save}, upsert=True)

    if result.upserted_id is not None:
        print(f"DEBUG: New movie saved: {movie_to_save['title']} (ID: {msg.id}, Lang: {movie_to_save['language']})")
        setting = settings_col.find_one({"key": "global_notify"})
        if setting and setting.get("value"):
            for user in users_col.find({"notify": {"$ne": False}}):
                try:
                    m = await app.send_message(
                        user["_id"],
                        f"নতুন মুভি আপলোড হয়েছে:\n**{text.splitlines()[0][:100]}**\nএখনই সার্চ করে দেখুন!"
                    )
                    asyncio.create_task(delete_message_later(m.chat.id, m.id))
                    await asyncio.sleep(0.05)
                except Exception as e:
                    if "PEER_ID_INVALID" in str(e) or "USER_IS_BOT" in str(e) or "USER_DEACTIVATED_REQUIRED" in str(e):
                        print(f"Skipping notification to invalid/blocked user {user['_id']}: {e}")
                    else:
                        print(f"Failed to send notification to user {user['_id']}: {e}")
    else:
        print(f"DEBUG: Movie with ID {msg.id} already exists or updated.")

@app.on_message(filters.command("start"))
async def start(_, msg: Message):
    user_id = msg.from_user.id
    current_time = datetime.now(UTC)

    if user_id in user_last_start_time:
        time_since_last_start = current_time - user_last_start_time[user_id]
        if time_since_last_start < timedelta(seconds=5):
            print(f"User {user_id} sent /start too quickly. Ignoring.")
            return

    user_last_start_time[user_id] = current_time

    if len(msg.command) > 1 and msg.command[1].startswith("watch_"):
        message_id = int(msg.command[1].replace("watch_", ""))
        try:
            copied_message = await app.copy_message(
                chat_id=msg.chat.id,
                from_chat_id=CHANNEL_ID,
                message_id=message_id,
                protect_content=True
            )
            
            movie_data = movies_col.find_one({"message_id": message_id})
            if movie_data:
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
                {"message_id": message_id},
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
    print(f"DEBUG: Search function triggered. Raw query: '{query}'")

    if not query:
        print("DEBUG: Search - Query is empty or only whitespace. Returning.")
        return 

    if msg.chat.type == "group":
        if len(query) < 3: # গ্রুপ চ্যাটে ছোট কোয়েরি ইগনোর করা
            print(f"DEBUG: Search - Group chat query '{query}' too short. Returning.")
            return
        if msg.reply_to_message or msg.from_user.is_bot:
            print(f"DEBUG: Search - Group chat: is reply or from bot. Returning.")
            return
        if not re.search(r'[a-zA-Z0-9]', query):
            print(f"DEBUG: Search - Group chat: query '{query}' contains no alphanumeric chars. Returning.")
            return

    user_id = msg.from_user.id
    users_col.update_one(
        {"_id": user_id},
        {"$set": {"last_query": query}, "$setOnInsert": {"joined": datetime.now(UTC)}},
        upsert=True
    )

    loading_message = await msg.reply("🔎 লোড হচ্ছে, অনুগ্রহ করে অপেক্ষা করুন...", quote=True)
    asyncio.create_task(delete_message_later(loading_message.chat.id, loading_message.id))

    query_clean = clean_text(query)
    print(f"DEBUG: Search - Original query: '{query}', Cleaned query: '{query_clean}'")

    # যদি clean_text করার পরেও query_clean খালি থাকে, তাহলে সার্চ করা অর্থহীন।
    if not query_clean:
        await loading_message.delete()
        print("DEBUG: Search - Cleaned query is empty. Not proceeding with search.")
        error_msg = await msg.reply("অনুগ্রহ করে একটি বৈধ মুভির নাম লিখুন।", quote=True)
        asyncio.create_task(delete_message_later(error_msg.chat.id, error_msg.id))
        return

    # সরাসরি মুভি ম্যাচিং
    matched_movies_direct = list(movies_col.find(
        {"$or": [
            {"title_clean": {"$regex": f"^{re.escape(query_clean)}", "$options": "i"}},
            {"title": {"$regex": re.escape(query), "$options": "i"}}
        ]}
    ).limit(RESULTS_COUNT))

    if matched_movies_direct:
        await loading_message.delete()
        buttons = []
        for movie in matched_movies_direct:
            buttons.append([
                InlineKeyboardButton(
                    text=f"{movie['title'][:40]} ({movie.get('views_count', 0)} ভিউ)",
                    url=f"https://t.me/{app.me.username}?start=watch_{movie['message_id']}"
                )
            ])
        
        encoded_query_clean_for_callback = urllib.parse.quote_plus(query_clean)
        lang_filter_buttons = [
            InlineKeyboardButton("বেঙ্গলি", callback_data=f"filter_lang_Bengali_{encoded_query_clean_for_callback}"),
            InlineKeyboardButton("হিন্দি", callback_data=f"filter_lang_Hindi_{encoded_query_clean_for_callback}"),
            InlineKeyboardButton("ইংলিশ", callback_data=f"filter_lang_English_{encoded_query_clean_for_callback}")
        ]
        buttons.append(lang_filter_buttons)

        m = await msg.reply("🎬 নিচের রেজাল্টগুলো পাওয়া গেছে:", reply_markup=InlineKeyboardMarkup(buttons), quote=True)
        asyncio.create_task(delete_message_later(m.chat.id, m.id))
        print(f"DEBUG: Direct match found for '{query_clean}'. Sent results and language filter buttons.")
        return

    # কাছাকাছি মিল খুঁজে বের করা
    # এখানে LIMIT বাড়ানো হয়েছে যাতে ফজিউইজি ভালোভাবে কাজ করতে পারে
    all_movie_data_cursor = movies_col.find(
        {}, 
        {"title_clean": 1, "original_title": "$title", "message_id": 1, "language": 1, "views_count": 1}
    ).limit(2000) # LIMIT বাড়িয়ে 2000 করা হলো, আরও বেশি ডেটা আনার জন্য

    all_movie_data = list(all_movie_data_cursor)
    print(f"DEBUG: Fetched {len(all_movie_data)} movies for fuzzy matching (initial search).")

    corrected_suggestions = await asyncio.get_event_loop().run_in_executor(
        thread_pool_executor,
        find_corrected_matches,
        query_clean,
        all_movie_data,
        55, # স্কোর কাটঅফ 55 এ কমানো হয়েছে
        RESULTS_COUNT
    )

    await loading_message.delete()

    if corrected_suggestions:
        buttons = []
        for movie in corrected_suggestions:
            buttons.append([
                InlineKeyboardButton(
                    text=f"{movie['title'][:40]} ({movie.get('views_count', 0)} ভিউ)",
                    url=f"https://t.me/{app.me.username}?start=watch_{movie['message_id']}"
                )
            ])
        
        encoded_query_clean_for_callback = urllib.parse.quote_plus(query_clean)
        lang_buttons = [
            InlineKeyboardButton("বেঙ্গলি", callback_data=f"filter_lang_Bengali_{encoded_query_clean_for_callback}"),
            InlineKeyboardButton("হিন্দি", callback_data=f"filter_lang_Hindi_{encoded_query_clean_for_callback}"),
            InlineKeyboardButton("ইংলিশ", callback_data=f"filter_lang_English_{encoded_query_clean_for_callback}")
        ]
        buttons.append(lang_buttons)

        m = await msg.reply("🔍 সরাসরি মিলে যায়নি, তবে কাছাকাছি কিছু পাওয়া গেছে:", reply_markup=InlineKeyboardMarkup(buttons), quote=True)
        asyncio.create_task(delete_message_later(m.chat.id, m.id))
        print(f"DEBUG: Fuzzy match found for '{query_clean}'. Sent results and language filter buttons.")
    else:
        # কোনো ফলাফল না পেলে
        Google_Search_url = "https://www.google.com/search?q=" + urllib.parse.quote(query)
        
        request_button = InlineKeyboardButton("এই মুভির জন্য অনুরোধ করুন", callback_data=f"request_movie_{user_id}_{urllib.parse.quote_plus(query)}")
        google_button_row = [InlineKeyboardButton("গুগলে সার্চ করুন", url=Google_Search_url)]
        
        # যদি query_clean খালি হয়, তাহলেও ভাষার বাটনগুলো যাতে কাজ করে তার জন্য query_clean এর জায়গায় একটি ফাঁকা স্ট্রিং পাঠানো হচ্ছে
        # কিন্তু, যেহেতু এই ক্ষেত্রে কোনো সার্চ ক্যোয়ারি নেই, তাই ক্যোয়ারি না থাকলে শুধু ভাষার মুভি দেখানোর লজিক কাজ করবে।
        encoded_query_clean_for_callback = urllib.parse.quote_plus(query_clean) if query_clean else urllib.parse.quote_plus("")

        lang_buttons_no_result = [
            InlineKeyboardButton("বেঙ্গলি মুভি দেখুন", callback_data=f"filter_lang_Bengali_{encoded_query_clean_for_callback}"),
            InlineKeyboardButton("হিন্দি মুভি দেখুন", callback_data=f"filter_lang_Hindi_{encoded_query_clean_for_callback}"),
            InlineKeyboardButton("ইংলিশ মুভি দেখুন", callback_data=f"filter_lang_English_{encoded_query_clean_for_callback}")
        ]

        reply_markup_for_no_result = InlineKeyboardMarkup([
            google_button_row,
            [request_button],
            lang_buttons_no_result
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
        print(f"DEBUG: No direct or fuzzy match found for '{query_clean}'. Sent no-result message.")

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
                print(f"ERROR: Could not notify admin {admin_id}: {e}")

@app.on_callback_query()
async def callback_handler(_, cq: CallbackQuery):
    data = cq.data
    print(f"DEBUG: Callback received: {data}")

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

    elif data.startswith("filter_lang_"): 
        parts = data.split("_", 2)
        if len(parts) < 3:
            await cq.answer("অকার্যকর কলব্যাক ডেটা ফরম্যাট।", show_alert=True)
            print(f"ERROR: Invalid callback data format for filter_lang_: {data}. Parts: {parts}") 
            return
        
        _, lang, encoded_query_clean = parts
        query_clean = urllib.parse.unquote_plus(encoded_query_clean) 

        print(f"DEBUG: Callback - filter_lang_ - lang: '{lang}', query_clean: '{query_clean}'") 
        
        # ভাষার উপর ভিত্তি করে মুভি খোঁজা
        lang_regex = re.compile(f"^{re.escape(lang)}$", re.IGNORECASE) # Exact match for language
        
        # যদি query_clean খালি থাকে, অথবা এটি 'Others' হয় (যেমন যখন কোনো সার্চ রেজাল্ট পাওয়া যায়নি)
        # তখন শুধুমাত্র ভাষার উপর ভিত্তি করে মুভি দেখানো হবে।
        # যদি query_clean থাকে, তাহলে ভাষা ও সার্চ ক্যোয়ারি উভয় দিয়েই ফিল্টার করা হবে।
        
        filter_criteria = {"language": {"$regex": lang_regex}}
        if query_clean.strip():
            filter_criteria["$or"] = [
                {"title_clean": {"$regex": f"^{re.escape(query_clean)}", "$options": "i"}},
                {"title_clean": {"$regex": re.escape(query_clean), "$options": "i"}} # for partial matches
            ]

        potential_lang_matches_cursor = movies_col.find(
            filter_criteria, 
            {"title": 1, "message_id": 1, "title_clean": 1, "views_count": 1, "language": 1}
        ).sort("views_count", -1).limit(RESULTS_COUNT) # জনপ্রিয়তার ভিত্তিতে সর্ট করা

        matches_filtered_by_lang = list(potential_lang_matches_cursor)
        
        print(f"DEBUG: Filtered {len(matches_filtered_by_lang)} movies by language '{lang}' with query '{query_clean}'.")
        
        if matches_filtered_by_lang:
            buttons = []
            for m in matches_filtered_by_lang: # ফলাফলের সংখ্যা RESULTS_COUNT দ্বারা সীমিত
                buttons.append([InlineKeyboardButton(f"{m['title'][:40]} ({m.get('views_count',0)} ভিউ)", url=f"https://t.me/{app.me.username}?start=watch_{m['message_id']}")])
            
            try:
                reply_msg = await cq.message.edit_text(
                    f"ফলাফল ({lang} মুভি):",
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
                asyncio.create_task(delete_message_later(reply_msg.chat.id, reply_msg.id))
                print(f"DEBUG: Sent {len(matches_filtered_by_lang)} language-filtered results for '{query_clean}' (Lang: {lang}).")
            except Exception as e:
                print(f"ERROR: Error editing message after language filter for '{query_clean}' (Lang: {lang}): {e}")
                await cq.answer("ফলাফল দেখানোর সময় সমস্যা হয়েছে।", show_alert=True)
        else:
            await cq.answer(f"দুঃখিত, '{lang}' ভাষায় আপনার অনুসন্ধানের জন্য কোনো মুভি পাওয়া যায়নি।", show_alert=True)
            print(f"DEBUG: No language-filtered results found for '{query_clean}' (Lang: {lang}).")
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
        print(f"DEBUG: Request created for '{movie_name}' by user {user_id}.")
        
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
                print(f"ERROR: Could not notify admin {admin_id} about request from callback: {e}")
        
        try:
            edited_msg = await cq.message.edit_text(
                f"❌ দুঃখিত! আপনার খোঁজা মুভিটি খুঁজে পাওয়া যায়নি।\n\n"
                f"আপনার অনুরোধ **'{movie_name}'** জমা দেওয়া হয়েছে। এডমিনরা এটি পর্যালোচনা করবেন।",
                reply_markup=None 
            )
            asyncio.create_task(delete_message_later(edited_msg.chat.id, edited_msg.id))
        except Exception as e:
            print(f"ERROR: Error editing user message after request: {e}")

    elif data.startswith("like_") or data.startswith("dislike_"):
        action, message_id_str, user_id_str = data.split("_", 2)
        movie_message_id = int(message_id_str)
        user_id = int(user_id_str)

        movie = movies_col.find_one({"message_id": movie_message_id})
        
        if not movie:
            await cq.answer("দুঃখিত, এই মুভিটি খুঁজে পাওয়া যায়নি।", show_alert=True)
            print(f"DEBUG: Movie {movie_message_id} not found for like/dislike.")
            return

        if user_id in movie.get("rated_by", []):
            await cq.answer("আপনি ইতিমধ্যেই এই মুভিতে রেটিং দিয়েছেন!", show_alert=True)
            print(f"DEBUG: User {user_id} already rated movie {movie_message_id}.")
            return

        update_query = {"$inc": {}, "$push": {"rated_by": user_id}}
        if action == "like":
            update_query["$inc"]["likes"] = 1
        elif action == "dislike":
            update_query["$inc"]["dislikes"] = 1
        
        movies_col.update_one({"message_id": movie_message_id}, update_query)
        
        updated_movie = movies_col.find_one({"message_id": movie_message_id})
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
            print(f"DEBUG: User {user_id} {action}d movie {movie_message_id}. Likes: {updated_likes}, Dislikes: {updated_dislikes}")
        except Exception as e:
            print(f"ERROR: Error editing message after rating: {e}")
            await cq.answer("রেটিং আপডেট করতে সমস্যা হয়েছে।", show_alert=True)

    elif data.startswith("req_fulfilled_") or data.startswith("req_rejected_"):
        action, user_id_str, encoded_movie_name = data.split("_", 2)
        user_id = int(user_id_str)
        movie_name = urllib.parse.unquote_plus(encoded_movie_name)
        
        status = "fulfilled" if action == "req_fulfilled" else "rejected"
        status_text = "পূরণ করা হয়েছে" if status == "fulfilled" else "বাতিল করা হয়েছে"

        requests_col.update_one(
            {"user_id": user_id, "movie_name": movie_name, "status": "pending"},
            {"$set": {"status": status, "admin_action_time": datetime.now(UTC)}}
        )

        try:
            if status == "fulfilled":
                await app.send_message(user_id, f"✅ আপনার অনুরোধ করা মুভি **'{movie_name}'** এখন উপলব্ধ। অনুগ্রহ করে সার্চ করে দেখুন!")
            else:
                await app.send_message(user_id, f"❌ দুঃখিত, আপনার অনুরোধ করা মুভি **'{movie_name}'** বাতিল করা হয়েছে।")
            
            await cq.answer(f"অনুরোধ {status_text} হিসেবে চিহ্নিত করা হয়েছে এবং ব্যবহারকারীকে জানানো হয়েছে।", show_alert=True)
            await cq.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"✅ সম্পন্ন হয়েছে: {status_text}", callback_data="noop")
            ]]))
            print(f"DEBUG: Request for '{movie_name}' by user {user_id} marked as {status}.")
        except Exception as e:
            await cq.answer("ব্যবহারকারীকে বার্তা পাঠানো যায়নি।", show_alert=True)
            print(f"ERROR: Error notifying user about request status: {e}")
            
    elif "_" in data:
        parts = data.split("_", 3)
        if len(parts) == 4 and parts[0] in ["noresult"]: 
            reason = parts[1]
            uid = int(parts[2])
            raw_query = urllib.parse.unquote_plus(parts[3]) 

            responses = {
                "wrong": f"❌ আপনি **'{raw_query}'** নামে ভুল সার্চ করেছেন। অনুগ্রহ করে সঠিক নাম লিখে আবার চেষ্টা করুন।",
                "notyet": f"⏳ **'{raw_query}'** মুভিটি এখনো আমাদের কাছে আসেনি। অনুগ্রহ করে কিছু সময় পর আবার চেষ্টা করুন।",
                "uploaded": f"📤 **'{raw_query}'** মুভিটি ইতিমধ্যে আপলোড করা হয়েছে। সঠিক নামে আবার সার্চ করুন।",
                "coming": f"🚀 **'{raw_query}'** মুভিটি খুব শিগগিরই আমাদের চ্যানেলে আসবে। অনুগ্রহ করে অপেক্ষা করুন."
            }
            if reason in responses:
                try:
                    m = await app.send_message(uid, responses[reason])
                    asyncio.create_task(delete_message_later(m.chat.id, m.id))
                    await cq.answer("অ্যাডমিনের পক্ষ থেকে উত্তর পাঠানো হয়েছে।")
                    await cq.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(f"✅ উত্তর দেওয়া হয়েছে: {responses[reason].split(' ')[0]}", callback_data="noop")
                    ]]))
                    print(f"DEBUG: Admin replied to no-result query '{raw_query}' for user {uid} with reason '{reason}'.")
                except Exception as e:
                    await cq.answer("ইউজারকে বার্তা পাঠাতে সমস্যা হয়েছে।", show_alert=True)
                    print(f"ERROR: Error sending admin feedback message: {e}")
            else:
                await cq.answer("অকার্যকর কলব্যাক ডেটা।", show_alert=True)
                print(f"ERROR: Unknown noresult reason: {data}")
        else:
            await cq.answer("অকার্যকর কলব্যাক ডেটা।", show_alert=True)
            print(f"ERROR: Unhandled callback data: {data}")
            
    else: 
        await cq.answer("আপনার অনুরোধ প্রক্রিয়া করা হচ্ছে।", show_alert=False)
        print(f"DEBUG: Default callback answer for: {data}")


if __name__ == "__main__":
    print("বট শুরু হচ্ছে...")
    app.run()

