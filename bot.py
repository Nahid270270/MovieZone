from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient, ASCENDING
from flask import Flask
from threading import Thread
import os
import re
from datetime import datetime
import asyncio
import urllib.parse
from fuzzywuzzy import process # Added for spell correction

# Configs
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

# Index
movies_col.create_index([("title", ASCENDING)])
movies_col.create_index("message_id")
movies_col.create_index("language")
movies_col.create_index([("title_clean", ASCENDING)]) # Ensure title_clean is also indexed

# Flask App for health check
flask_app = Flask(__name__)
@flask_app.route("/")
def home():
    return "Bot is running!"
Thread(target=lambda: flask_app.run(host="0.0.0.0", port=8080)).start()

# Helpers
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9]', '', text.lower())

def extract_year(text):
    match = re.search(r"(19|20)\d{2}", text)
    return match.group() if match else None

def extract_language(text):
    langs = ["Bengali", "Hindi", "English"]
    return next((lang for lang in langs if lang.lower() in text.lower()), "Unknown")

async def delete_message_later(chat_id, message_id, delay=600):
    await asyncio.sleep(delay)
    try:
        await app.delete_messages(chat_id, message_id)
    except Exception as e:
        # Suppress "Message not found" errors which are common if user deletes first
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
                        "language": movie_data["language"]
                    })
                    break
    return corrected_suggestions

# Main message processing for new posts in channel
@app.on_message(filters.chat(CHANNEL_ID))
async def save_post(_, msg: Message):
    text = msg.text or msg.caption
    if not text:
        return

    # Check if movie already exists to prevent duplicate processing if bot restarts/re-processes
    # This specifically targets notification to prevent sending multiple times for same post
    existing_movie = movies_col.find_one({"message_id": msg.id})
    
    movie = {
        "message_id": msg.id,
        "title": text,
        "date": msg.date,
        "year": extract_year(text),
        "language": extract_language(text),
        "title_clean": clean_text(text)
    }
    
    # Upsert the movie, if it exists, it will update; if not, it will insert
    movies_col.update_one({"message_id": msg.id}, {"$set": movie}, upsert=True)

    # Only send notification if this is a truly new post or processed for the first time
    if not existing_movie: # This is the main fix for duplicate notifications
        setting = settings_col.find_one({"key": "global_notify"})
        if setting and setting.get("value"):
            for user in users_col.find({"notify": {"$ne": False}}):
                try:
                    await app.send_message(
                        user["_id"],
                        f"নতুন মুভি আপলোড হয়েছে:\n**{text.splitlines()[0][:100]}**\nএখনই সার্চ করে দেখুন!"
                    )
                    await asyncio.sleep(0.05) # Small delay to avoid flood limits when sending many notifications
                except Exception as e:
                    # Catch and log specific errors for sending messages (e.g., user blocked bot)
                    if "PEER_ID_INVALID" in str(e) or "USER_IS_BOT" in str(e) or "USER_BOT" in str(e) or "USER_DEACTIVATED_REQUIRED" in str(e):
                        print(f"Skipping notification to invalid/blocked user {user['_id']}: {e}")
                        # Optionally, remove user from database if they blocked bot
                        # users_col.delete_one({"_id": user["_id"]})
                    else:
                        print(f"Failed to send notification to user {user['_id']}: {e}")


@app.on_message(filters.command("start"))
async def start(_, msg: Message):
    # Ensure 'notify' field is set when a user starts the bot for the first time
    users_col.update_one(
        {"_id": msg.from_user.id},
        {"$set": {"joined": datetime.utcnow(), "notify": True}}, # Set notify to True by default on start
        upsert=True
    )
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("Update Channel", url=UPDATE_CHANNEL)],
        [InlineKeyboardButton("Contact Admin", url="https://t.me/ctgmovies23")]
    ])
    await msg.reply_photo(photo=START_PIC, caption="Send me a movie name to search.", reply_markup=btns)

@app.on_message(filters.command("feedback") & filters.private)
async def feedback(_, msg: Message):
    if len(msg.command) < 2:
        return await msg.reply("Please write something after /feedback.")
    feedback_col.insert_one({
        "user": msg.from_user.id,
        "text": msg.text.split(None, 1)[1],
        "time": datetime.utcnow()
    })
    m = await msg.reply("Thanks for your feedback!")
    asyncio.create_task(delete_message_later(m.chat.id, m.id, delay=30))

@app.on_message(filters.command("broadcast") & filters.user(ADMIN_IDS))
async def broadcast(_, msg: Message):
    if len(msg.command) < 2:
        return await msg.reply("Usage: /broadcast Your message here")
    count = 0
    message_to_send = msg.text.split(None, 1)[1]
    for user in users_col.find():
        try:
            await app.send_message(user["_id"], message_to_send)
            count += 1
            await asyncio.sleep(0.05) # Small delay to avoid flood limits
        except Exception as e:
            # Improved error handling for broadcast failures
            if "PEER_ID_INVALID" in str(e) or "USER_IS_BLOCKED" in str(e) or "USER_BOT" in str(e) or "USER_DEACTIVATED_REQUIRED" in str(e):
                print(f"Skipping broadcast to invalid/blocked user {user['_id']}: {e}")
                # Optional: Remove user from database if they have blocked the bot
                # users_col.delete_one({"_id": user["_id"]})
            else:
                print(f"Failed to broadcast to user {user['_id']}: {e}")
    await msg.reply(f"Broadcast sent to {count} users.")

@app.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def stats(_, msg: Message):
    await msg.reply(
        f"Users: {users_col.count_documents({})}\n"
        f"Movies: {movies_col.count_documents({})}\n"
        f"Feedbacks: {feedback_col.count_documents({})}"
    )

@app.on_message(filters.command("notify") & filters.user(ADMIN_IDS))
async def notify_command(_, msg: Message):
    if len(msg.command) != 2 or msg.command[1] not in ["on", "off"]:
        return await msg.reply("ব্যবহার: /notify on  অথবা  /notify off")
    new_value = True if msg.command[1] == "on" else False
    settings_col.update_one(
        {"key": "global_notify"},
        {"$set": {"value": new_value}},
        upsert=True
    )
    status = "enabled" if new_value else "disabled"
    await msg.reply(f"✅ Global notifications {status}!")

@app.on_message(filters.text)
async def search(_, msg: Message):
    raw_query = msg.text.strip()
    query_clean = clean_text(raw_query)
    user_id = msg.from_user.id

    users_col.update_one(
        {"_id": user_id},
        {"$set": {"last_search": datetime.utcnow()}},
        upsert=True
    )

    loading = await msg.reply("🔎 লোড হচ্ছে, অনুগ্রহ করে অপেক্ষা করুন...")

    # First, try to find direct matches
    direct_suggestions = list(movies_col.find(
        {"title_clean": {"$regex": query_clean, "$options": "i"}},
        {"title": 1, "message_id": 1, "language": 1}
    ).limit(RESULTS_COUNT))

    if direct_suggestions:
        await loading.delete()
        buttons = []
        for m in direct_suggestions:
            buttons.append([InlineKeyboardButton(m["title"][:40], callback_data=f"movie_{m['message_id']}")])
        
        lang_buttons = [
            InlineKeyboardButton("Bengali", callback_data=f"lang_Bengali_{query_clean}"),
            InlineKeyboardButton("Hindi", callback_data=f"lang_Hindi_{query_clean}"),
            InlineKeyboardButton("English", callback_data=f"lang_English_{query_clean}")
        ]
        buttons.append(lang_buttons)
        
        m = await msg.reply("আপনার মুভির নাম মিলতে পারে, নিচের থেকে সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(buttons))
        asyncio.create_task(delete_message_later(m.chat.id, m.id))
        return

    # If no direct matches, attempt spell correction
    all_movie_titles_from_db = list(movies_col.find({}, {"title": 1, "message_id": 1, "language": 1}))
    
    fuzzy_data_for_matching = [
        {"title_clean": clean_text(m["title"]), "original_title": m["title"], "message_id": m["message_id"], "language": m["language"]}
        for m in all_movie_titles_from_db
    ]

    corrected_suggestions = find_corrected_matches(query_clean, fuzzy_data_for_matching)

    if corrected_suggestions:
        await loading.delete()
        buttons = []
        for m in corrected_suggestions[:RESULTS_COUNT]:
            buttons.append([InlineKeyboardButton(m["title"][:40], callback_data=f"movie_{m['message_id']}")])

        lang_buttons = [
            InlineKeyboardButton("Bengali", callback_data=f"lang_Bengali_{query_clean}"),
            InlineKeyboardButton("Hindi", callback_data=f"lang_Hindi_{query_clean}"),
            InlineKeyboardButton("English", callback_data=f"lang_English_{query_clean}")
        ]
        buttons.append(lang_buttons)

        m = await msg.reply(
            "আপনার সার্চের সাথে সরাসরি কোনো ফলাফল মেলেনি। আপনি কি এটি বোঝাতে চেয়েছিলেন?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        asyncio.create_task(delete_message_later(m.chat.id, m.id))
        return

    # If no direct matches and no spell correction suggestions
    await loading.delete()
    
    Google_Search_url = "https://www.google.com/search?q=" + urllib.parse.quote(raw_query)
    google_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("Search on Google", url=Google_Search_url)]
    ])
    
    alert = await msg.reply(
        "দুঃখিত! আপনার খোঁজা মুভিটি খুঁজে পাওয়া যায়নি। অ্যাডমিনকে জানানো হয়েছে। নিচের বাটনে ক্লিক করে গুগলে সার্চ করতে পারেন।",
        reply_markup=google_button
    )
    asyncio.create_task(delete_message_later(alert.chat.id, alert.id))

    btn_admin_request = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ মুভি আছে", callback_data=f"has_{msg.chat.id}_{msg.id}_{raw_query}"),
            InlineKeyboardButton("❌ নেই", callback_data=f"no_{msg.chat.id}_{msg.id}_{raw_query}")
        ],
        [
            InlineKeyboardButton("⏳ শীঘ্রই আসবে", callback_data=f"soon_{msg.chat.id}_{msg.id}_{raw_query}"),
            InlineKeyboardButton("✏️ ভুল নাম", callback_data=f"wrong_{msg.chat.id}_{msg.id}_{raw_query}")
        ]
    ])
    
    for admin_id in ADMIN_IDS:
        try:
            await app.send_message(
                admin_id,
                f"❗ ইউজার `{msg.from_user.id}` (`{msg.from_user.first_name}`) খুঁজেছে: **{raw_query}**\nফলাফল পাওয়া যায়নি। নিচে বাটন থেকে উত্তর দিন।",
                reply_markup=btn_admin_request
            )
        except Exception as e:
            print(f"Failed to send admin message to {admin_id}: {e}")

@app.on_callback_query()
async def callback_handler(_, cq: CallbackQuery):
    data = cq.data

    if data.startswith("movie_"):
        mid = int(data.split("_")[1])
        try:
            fwd = await app.forward_messages(cq.message.chat.id, CHANNEL_ID, mid)
            asyncio.create_task(delete_message_later(cq.message.chat.id, fwd.id))
            await cq.answer("মুভি পাঠানো হয়েছে।")
        except Exception as e:
            await cq.answer("মুভিটি ফরওয়ার্ড করা যায়নি।", show_alert=True)
            print(f"Error forwarding message: {e}")

    elif data.startswith("lang_"):
        _, lang, query_clean = data.split("_", 2)
        
        all_movies_in_lang = list(movies_col.find(
            {"language": lang},
            {"title": 1, "message_id": 1}
        ))
        
        fuzzy_data_for_matching_lang = [
            {"title_clean": clean_text(m["title"]), "original_title": m["title"], "message_id": m["message_id"], "language": lang}
            for m in all_movies_in_lang
        ]
        
        matches_filtered_by_lang = find_corrected_matches(query_clean, fuzzy_data_for_matching_lang)

        if matches_filtered_by_lang:
            buttons = [
                [InlineKeyboardButton(m["title"][:40], callback_data=f"movie_{m['message_id']}")]
                for m in matches_filtered_by_lang[:RESULTS_COUNT]
            ]
            await cq.message.edit_text(
                f"ফলাফল ({lang}) - নিচের থেকে সিলেক্ট করুন:",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        else:
            await cq.answer("এই ভাষায় কিছু পাওয়া যায়নি।", show_alert=True)
        await cq.answer()

    elif "_" in data:
        parts = data.split("_", 3)
        if len(parts) == 4:
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
                    asyncio.create_task(delete_message_later(m.chat.id, m.id, delay=30))
                    await cq.answer("অ্যাডমিনের পক্ষ থেকে উত্তর পাঠানো হয়েছে।")
                except Exception as e:
                    await cq.answer("ইউজারকে বার্তা পাঠাতে সমস্যা হয়েছে।", show_alert=True)
                    print(f"Error sending admin feedback message: {e}")
            else:
                await cq.answer()
        else:
            await cq.answer("অকার্যকর কলব্যাক ডেটা।", show_alert=True)

if __name__ == "__main__":
    print("Bot is starting...")
    app.run()
