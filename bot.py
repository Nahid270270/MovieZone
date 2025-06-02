from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient, ASCENDING
from flask import Flask
from threading import Thread
import os
import re
from datetime import datetime, timedelta # timedelta ইম্পোর্ট করা হয়েছে
import asyncio
import urllib.parse

# --- Configs ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
RESULTS_COUNT = int(os.getenv("RESULTS_COUNT", 10))
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(",")))
DATABASE_URL = os.getenv("DATABASE_URL")
UPDATE_CHANNEL = os.getenv("UPDATE_CHANNEL", "https://t.me/CTGMovieOfficial")
START_PIC = os.getenv("START_PIC", "https://i.ibb.co/prnGXMr3/photo-2025-05-16-05-15-45-7504908428624527364.jpg")

# Bitly API Token (যদি আপনি Bitly ব্যবহার করতে চান, আপনার .env ফাইলে BITLY_API_TOKEN যোগ করুন)
BITLY_API_TOKEN = os.getenv("BITLY_API_TOKEN")

app = Client("movie_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- MongoDB setup ---
mongo = MongoClient(DATABASE_URL)
db = mongo["movie_bot"]
movies_col = db["movies"]
feedback_col = db["feedback"]
stats_col = db["stats"]
users_col = db["users"] # ইউজার কালেকশন
settings_col = db["settings"]

# Index
movies_col.create_index([("title", ASCENDING)])
movies_col.create_index("message_id")
movies_col.create_index("language")

# --- Flask ---
flask_app = Flask(__name__)
@flask_app.route("/")
def home():
    return "Bot is running!"
Thread(target=lambda: flask_app.run(host="0.0.0.0", port=8080)).start()

# --- Helpers ---
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9]', '', text.lower())

def extract_year(text):
    match = re.search(r"(19|20)\d{2}", text)
    return match.group() if match else None

def extract_language(text):
    langs = ["Bengali", "Hindi", "English"]
    return next((lang for lang in langs if lang.lower() in text.lower()), "Unknown")

async def delete_message_later(chat_id, message_id, delay=600): # ডিফল্ট ডিলে 10 মিনিট
    await asyncio.sleep(delay)
    try:
        await app.delete_messages(chat_id, message_id)
    except Exception as e:
        print(f"Error deleting message {message_id} in chat {chat_id}: {e}")

# --- লিঙ্ক শর্টনার ফাংশন ---
# এটি একটি ডামি ফাংশন। আপনাকে আপনার পছন্দের লিঙ্ক শর্টনার সার্ভিসের API ব্যবহার করে এটি তৈরি করতে হবে।
# Bitly ব্যবহার করতে চাইলে, BITLY_API_TOKEN সেট করুন এবং Bitly API কল লজিক যোগ করুন।
def shorten_link(long_url):
    """
    এই ফাংশনটি একটি লম্বা URL কে শর্ট করে।
    আপনার পছন্দের লিঙ্ক শর্টনার সার্ভিসের API ব্যবহার করে এটি ইমপ্লিমেন্ট করুন।
    উদাহরণস্বরূপ, Bitly API ব্যবহার করা যেতে পারে।
    """
    if BITLY_API_TOKEN:
        # Bitly API ইন্টিগ্রেশন (উদাহরণ)
        import requests
        headers = {
            "Authorization": f"Bearer {BITLY_API_TOKEN}",
            "Content-Type": "application/json"
        }
        payload = {
            "long_url": long_url
        }
        try:
            response = requests.post("https://api-ssl.bitly.com/v4/shorten", headers=headers, json=payload)
            response.raise_for_status()
            short_url = response.json().get("link")
            if short_url:
                print(f"Shortened {long_url} to {short_url}")
                return short_url
        except requests.exceptions.RequestException as e:
            print(f"Error shortening link with Bitly: {e}")
            # ত্রুটি হলেও মূল লিঙ্কটি ফেরত দিন
            return long_url
    
    # যদি কোনো লিঙ্ক শর্টনার API সেট না থাকে বা ব্যর্থ হয়, তাহলে মূল লিঙ্কটি ফেরত দিন
    print(f"No link shortener configured or failed for {long_url}. Returning original URL.")
    return long_url

# --- প্রিমিয়াম ইউজার চেক ফাংশন ---
def is_premium_user(user_id):
    user = users_col.find_one({"_id": user_id})
    if user and user.get("is_premium") and user.get("premium_until") and user["premium_until"] > datetime.utcnow():
        return True
    return False

# --- Handlers ---

@app.on_message(filters.chat(CHANNEL_ID))
async def save_post(_, msg: Message):
    text = msg.text or msg.caption
    if not text:
        return
    movie = {
        "message_id": msg.id,
        "title": text,
        "date": msg.date,
        "year": extract_year(text),
        "language": extract_language(text),
        "title_clean": clean_text(text)
    }
    movies_col.update_one({"message_id": msg.id}, {"$set": movie}, upsert=True)

    setting = settings_col.find_one({"key": "global_notify"})
    if setting and setting.get("value"):
        # প্রিমিয়াম ব্যবহারকারীদের কাছে নোটিফিকেশন পাঠাতে পারেন, অথবা নতুন মুভি আপলোড হলে জানাতে পারেন
        # এখন সব ব্যবহারকারীর কাছেই নোটিফিকেশন যাবে, যদি 'notify' False না হয়
        for user in users_col.find({"notify": {"$ne": False}}):
            try:
                # সংক্ষিপ্ত নোটিফিকেশন
                first_line = text.splitlines()[0]
                notification_text = f"নতুন মুভি আপলোড হয়েছে: **{first_line[:100]}**\nএখনই সার্চ করে দেখুন!"
                await app.send_message(user["_id"], notification_text)
            except Exception as e:
                print(f"Could not send notification to user {user.get('_id', 'N/A')}: {e}")

@app.on_message(filters.command("start"))
async def start(_, msg: Message):
    users_col.update_one(
        {"_id": msg.from_user.id},
        {"$set": {"joined": datetime.utcnow()}},
        upsert=True
    )
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("আপডেট চ্যানেল", url=UPDATE_CHANNEL)],
        [InlineKeyboardButton("অ্যাডমিনের সাথে যোগাযোগ", url="https://t.me/ctgmovies23")],
        [InlineKeyboardButton("আমাদের প্রিমিয়াম নিন!", callback_data="show_premium_info")] # প্রিমিয়াম ইনফো বাটন
    ])
    m = await msg.reply_photo(photo=START_PIC, caption="আমাকে মুভির নাম পাঠান সার্চ করার জন্য।", reply_markup=btns)
    asyncio.create_task(delete_message_later(m.chat.id, m.id, delay=1200)) # 20 মিনিট পর ডিলিট হবে

@app.on_message(filters.command("feedback") & filters.private)
async def feedback(_, msg):
    if len(msg.command) < 2:
        return await msg.reply("অনুগ্রহ করে `/feedback` এর পর আপনার মতামত লিখুন।")
    feedback_col.insert_one({
        "user_id": msg.from_user.id,
        "username": msg.from_user.username,
        "first_name": msg.from_user.first_name,
        "text": msg.text.split(None, 1)[1],
        "time": datetime.utcnow()
    })
    m = await msg.reply("আপনার মূল্যবান মতামতের জন্য ধন্যবাদ! 💖")
    asyncio.create_task(delete_message_later(m.chat.id, m.id, delay=120)) # 2 মিনিট পর ডিলিট হবে

@app.on_message(filters.command("broadcast") & filters.user(ADMIN_IDS))
async def broadcast(_, msg):
    if len(msg.command) < 2:
        return await msg.reply("ব্যবহার: `/broadcast আপনার মেসেজ`")
    broadcast_message = msg.text.split(None, 1)[1]
    count = 0
    failed_count = 0
    for user in users_col.find():
        try:
            await app.send_message(user["_id"], broadcast_message)
            count += 1
            await asyncio.sleep(0.1) # Flood wait এড়ানোর জন্য ছোট ডিলে
        except Exception as e:
            failed_count += 1
            print(f"Failed to send broadcast to user {user.get('_id', 'N/A')}: {e}")
    await msg.reply(f"ব্রডকাস্ট পাঠানো হয়েছে **{count}** জন ব্যবহারকারীকে। **{failed_count}** জন ব্যবহারকারীর কাছে পৌঁছানো যায়নি।")

@app.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def stats(_, msg):
    total_users = users_col.count_documents({})
    premium_users = users_col.count_documents({"is_premium": True, "premium_until": {"$gt": datetime.utcnow()}})
    await msg.reply(
        f"**বট পরিসংখ্যান:**\n"
        f"মোট ব্যবহারকারী: `{total_users}`\n"
        f"প্রিমিয়াম ব্যবহারকারী: `{premium_users}`\n"
        f"মোট মুভি: `{movies_col.count_documents({})}`\n"
        f"মোট ফিডব্যাক: `{feedback_col.count_documents({})}`"
    )

@app.on_message(filters.command("notify") & filters.user(ADMIN_IDS))
async def notify_command(_, msg: Message):
    if len(msg.command) != 2 or msg.command[1] not in ["on", "off"]:
        return await msg.reply("ব্যবহার: `/notify on` অথবা `/notify off`")
    new_value = True if msg.command[1] == "on" else False
    settings_col.update_one(
        {"key": "global_notify"},
        {"$set": {"value": new_value}},
        upsert=True
    )
    status = "সক্রিয়" if new_value else "নিষ্ক্রিয়"
    await msg.reply(f"✅ গ্লোবাল নোটিফিকেশন **{status}** করা হয়েছে!")

# --- প্রিমিয়াম সাবস্ক্রিপশনের জন্য কমান্ড ---
@app.on_message(filters.command("premium") & filters.private)
async def premium_info(_, msg: Message):
    user_id = msg.from_user.id
    if is_premium_user(user_id):
        user = users_col.find_one({"_id": user_id})
        expiry_date_str = user['premium_until'].strftime('%Y-%m-%d %H:%M:%S')
        await msg.reply(f"আপনি একজন **প্রিমিয়াম সদস্য**! 🎉\nআপনার সাবস্ক্রিপশনের মেয়াদ শেষ হবে: **{expiry_date_str} (UTC)**।")
    else:
        payment_details = f"""
আমাদের **প্রিমিয়াম সাবস্ক্রিপশন** নিন এবং উপভোগ করুন **বিজ্ঞাপন-মুক্ত মুভি লিঙ্ক**! 🎬✨

**ফি:** ৳100 (মাসিক)

**পেমেন্ট পদ্ধতি:**
1.  **বিকাশ (Bkash) পার্সোনাল:** `01XXXXXXXXX` (সেন্ড মানি করুন)
2.  **নগদ (Nagad) পার্সোনাল:** `01XXXXXXXXX` (সেন্ড মানি করুন)

**পেমেন্ট করার পর:**
টাকা পাঠানোর পর, অনুগ্রহ করে পেমেন্টের **ট্রান্সেকশন আইডি (Transaction ID)** সহ `/paid <ট্রান্সেকশন_আইডি>` এই কমান্ডটি ব্যবহার করে আমাদের জানান।
উদাহরণ: `/paid 8P6P5X2A`

আমরা দ্রুত আপনার পেমেন্ট যাচাই করে প্রিমিয়াম স্ট্যাটাস সক্রিয় করে দেব। কোনো সমস্যা হলে অ্যাডমিনের সাথে যোগাযোগ করুন।
"""
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("অ্যাডমিনকে মেসেজ করুন", url="https://t.me/ctgmovies23")]
        ])
        m = await msg.reply(payment_details, reply_markup=btns)
        asyncio.create_task(delete_message_later(m.chat.id, m.id, delay=1200)) # 20 মিনিট পর মেসেজটি ডিলিট হবে

# --- পেমেন্ট নোটিফিকেশন হ্যান্ডেলিং (ব্যবহারকারীদের জন্য) ---
@app.on_message(filters.command("paid") & filters.private)
async def handle_payment_notification(_, msg: Message):
    if len(msg.command) < 2:
        return await msg.reply("অনুগ্রহ করে আপনার ট্রান্সেকশন আইডি দিন। উদাহরণ: `/paid 8P6P5X2A`")

    transaction_id = msg.text.split(None, 1)[1].strip()
    user_info = f"ইউজার `{msg.from_user.id}` `{msg.from_user.first_name}` (`@{msg.from_user.username or 'N/A'}`)"

    notification_message = (
        f"**🚨 নতুন পেমেন্ট নোটিফিকেশন!**\n"
        f"{user_info} পেমেন্ট করেছেন বলে দাবি করছেন।\n"
        f"**ট্রান্সেকশন আইডি:** `{transaction_id}`\n\n"
        f"যাচাই করে প্রিমিয়াম অ্যাক্সেস দিতে নিচের বাটনটি ব্যবহার করুন:"
    )

    for admin_id in ADMIN_IDS:
        try:
            await app.send_message(
                admin_id,
                notification_message,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ প্রিমিয়াম দিন", callback_data=f"grantpremium_{msg.from_user.id}_{transaction_id}")],
                    [InlineKeyboardButton("❌ প্রত্যাখ্যান করুন", callback_data=f"rejectpremium_{msg.from_user.id}_{transaction_id}")]
                ])
            )
        except Exception as e:
            print(f"Error sending payment notification to admin {admin_id}: {e}")

    m = await msg.reply("আপনার পেমেন্ট নোটিফিকেশন অ্যাডমিনদের কাছে পাঠানো হয়েছে। তারা দ্রুত এটি যাচাই করে আপনাকে জানাবে। ধন্যবাদ! 🙏")
    asyncio.create_task(delete_message_later(m.chat.id, m.id, delay=600)) # 10 মিনিট পর ডিলিট হবে

@app.on_message(filters.text)
async def search(_, msg):
    raw_query = msg.text.strip()
    query = clean_text(raw_query)
    users_col.update_one(
        {"_id": msg.from_user.id},
        {"$set": {"last_search": datetime.utcnow()}},
        upsert=True
    )

    loading = await msg.reply("🔎 লোড হচ্ছে, অনুগ্রহ করে অপেক্ষা করুন...")

    suggestions = list(movies_col.find(
        {"title_clean": {"$regex": query}},
        {"title": 1, "message_id": 1, "language": 1}
    ).limit(RESULTS_COUNT))

    if suggestions:
        await loading.delete()
        lang_buttons = [
            InlineKeyboardButton("বাংলা 🇧🇩", callback_data=f"lang_Bengali_{query}"),
            InlineKeyboardButton("হিন্দি 🇮🇳", callback_data=f"lang_Hindi_{query}"),
            InlineKeyboardButton("ইংলিশ 🇬🇧", callback_data=f"lang_English_{query}")
        ]
        buttons = [[InlineKeyboardButton(m["title"][:40], callback_data=f"movie_{m['message_id']}")] for m in suggestions]
        buttons.append(lang_buttons)
        m = await msg.reply("আপনার মুভির নাম মিলতে পারে, নিচের থেকে সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(buttons))
        asyncio.create_task(delete_message_later(m.chat.id, m.id, delay=600)) # 10 মিনিট পর ডিলিট হবে
        return

    await loading.delete()
    Google Search_url = "https://www.google.com/search?q=" + urllib.parse.quote(raw_query)
    google_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("গুগলে সার্চ করুন 🔍", url=Google Search_url)]
    ])
    alert = await msg.reply(
        "কোনও ফলাফল পাওয়া যায়নি। অ্যাডমিনকে জানানো হয়েছে। নিচের বাটনে ক্লিক করে গুগলে সার্চ করুন।",
        reply_markup=google_button
    )
    asyncio.create_task(delete_message_later(alert.chat.id, alert.id, delay=600)) # 10 মিনিট পর ডিলিট হবে

    btn = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ মুভি আছে", callback_data=f"has_{msg.chat.id}_{msg.id}_{raw_query}"),
            InlineKeyboardButton("❌ নেই", callback_data=f"no_{msg.chat.id}_{msg.id}_{raw_query}")
        ],
        [
            InlineKeyboardButton("⏳ আসবে", callback_data=f"soon_{msg.chat.id}_{msg.id}_{raw_query}"),
            InlineKeyboardButton("✏️ ভুল নাম", callback_data=f"wrong_{msg.chat.id}_{msg.id}_{raw_query}")
        ]
    ])
    for admin_id in ADMIN_IDS:
        try:
            await app.send_message(
                admin_id,
                f"❗ ইউজার `{msg.from_user.id}` `{msg.from_user.first_name}` খুঁজেছে: **{raw_query}**\nফলাফল পাওয়া যায়নি। নিচে বাটন থেকে উত্তর দিন।",
                reply_markup=btn
            )
        except Exception as e:
            print(f"Error sending admin notification to {admin_id}: {e}")

@app.on_callback_query()
async def callback_handler(_, cq: CallbackQuery):
    data = cq.data
    user_id_initiator = cq.from_user.id # যে ইউজার কলব্যাক করছেন

    # --- প্রিমিয়াম ইনফো দেখানোর জন্য ---
    if data == "show_premium_info":
        await cq.answer("আমাদের প্রিমিয়াম সাবস্ক্রিপশন সম্পর্কে তথ্য লোড হচ্ছে...", show_alert=True)
        # /premium কমান্ডের লজিককে কল করুন, কিন্তু এটি সরাসরি Message অবজেক্ট পায় না
        # তাই, একটি ডামি Message অবজেক্ট তৈরি করে পাঠাই
        dummy_msg = Message(id=0, chat=cq.message.chat, from_user=cq.from_user, date=datetime.utcnow(), text="/premium")
        await premium_info(_, dummy_msg)
        return # প্রিমিয়াম ইনফো দেখানোর পর আর কিছু করার দরকার নেই

    # --- অ্যাডমিন কলব্যাক হ্যান্ডলার (শুধুমাত্র অ্যাডমিনদের জন্য) ---
    if user_id_initiator in ADMIN_IDS:
        if data.startswith("grantpremium_"):
            _, target_user_id_str, transaction_id = data.split("_", 2)
            target_user_id = int(target_user_id_str)

            premium_expiry = datetime.utcnow() + timedelta(days=30) # 30 দিনের জন্য প্রিমিয়াম
            users_col.update_one(
                {"_id": target_user_id},
                {"$set": {"is_premium": True, "premium_until": premium_expiry}},
                upsert=True
            )

            await cq.message.edit_text(f"✅ ইউজার `{target_user_id}` কে প্রিমিয়াম অ্যাক্সেস দেওয়া হয়েছে। (ট্রান্সেকশন: `{transaction_id}`) - অ্যাডমিন: @{cq.from_user.username or cq.from_user.first_name}")
            try:
                await app.send_message(target_user_id, "অভিনন্দন! 🎉 আপনার প্রিমিয়াম সাবস্ক্রিপশন সক্রিয় করা হয়েছে। এখন থেকে আপনি সরাসরি মুভি লিঙ্ক পাবেন এবং কোনো অ্যাড দেখাবে না। উপভোগ করুন!")
            except Exception as e:
                print(f"Could not send premium activation message to user {target_user_id}: {e}")

            await cq.answer("প্রিমিয়াম অ্যাক্সেস দেওয়া হয়েছে।")
            return # হ্যান্ডেল হয়ে গেলে আর কিছু করার দরকার নেই

        elif data.startswith("rejectpremium_"):
            _, target_user_id_str, transaction_id = data.split("_", 2)
            target_user_id = int(target_user_id_str)

            await cq.message.edit_text(f"❌ ইউজার `{target_user_id}` এর পেমেন্ট প্রত্যাখ্যান করা হয়েছে। (ট্রান্সেকশন: `{transaction_id}`) - অ্যাডমিন: @{cq.from_user.username or cq.from_user.first_name}")
            try:
                await app.send_message(target_user_id, "দুঃখিত! 😟 আপনার পেমেন্ট যাচাই করা যায়নি। অনুগ্রহ করে সঠিক ট্রান্সেকশন আইডি দিয়ে আবার চেষ্টা করুন অথবা অ্যাডমিনের সাথে যোগাযোগ করুন।")
            except Exception as e:
                print(f"Could not send premium rejection message to user {target_user_id}: {e}")

            await cq.answer("পেমেন্ট প্রত্যাখ্যান করা হয়েছে।")
            return # হ্যান্ডেল হয়ে গেলে আর কিছু করার দরকার নেই

        elif "_" in data and len(data.split("_")) == 4: # ইউজার সার্চ রিকোয়েস্টের অ্যাডমিন রেসপন্স
            parts = data.split("_", 3)
            action, uid, mid, raw_query = parts
            uid = int(uid)
            responses = {
                "has": f"✅ @{cq.from_user.username or cq.from_user.first_name} জানিয়েছেন যে **{raw_query}** মুভিটি ডাটাবেজে আছে। সঠিক নাম লিখে আবার চেষ্টা করুন।",
                "no": f"❌ @{cq.from_user.username or cq.from_user.first_name} জানিয়েছেন যে **{raw_query}** মুভিটি ডাটাবেজে নেই।",
                "soon": f"⏳ @{cq.from_user.username or cq.from_user.first_name} জানিয়েছেন যে **{raw_query}** মুভিটি শীঘ্রই আসবে।",
                "wrong": f"✏️ @{cq.from_user.username or cq.from_user.first_name} বলছেন যে আপনি ভুল নাম লিখেছেন: **{raw_query}**।"
            }
            if action in responses:
                m = await app.send_message(uid, responses[action])
                asyncio.create_task(delete_message_later(m.chat.id, m.id))
                await cq.answer("অ্যাডমিনের পক্ষ থেকে উত্তর পাঠানো হয়েছে।")
            else:
                await cq.answer()
            return # হ্যান্ডেল হয়ে গেলে আর কিছু করার দরকার নেই

    # --- মুভি ডেলিভারি লজিক (সব ব্যবহারকারীর জন্য) ---
    if data.startswith("movie_"):
        mid = int(data.split("_")[1])

        if is_premium_user(user_id_initiator):
            # প্রিমিয়াম ব্যবহারকারী হলে সরাসরি মুভি ফরওয়ার্ড করুন, লিঙ্ক শর্টনার ছাড়াই
            fwd = await app.forward_messages(cq.message.chat.id, CHANNEL_ID, mid)
            asyncio.create_task(delete_message_later(cq.message.chat.id, fwd.id, delay=1800)) # 30 মিনিট পর ডিলিট হবে
            await cq.answer("আপনার প্রিমিয়াম মুভি পাঠানো হয়েছে।", show_alert=True)
        else:
            # সাধারণ ব্যবহারকারী হলে লিঙ্ক শর্টনার ব্যবহার করুন
            try:
                movie_message = await app.get_messages(CHANNEL_ID, mid)
                original_text = movie_message.text or movie_message.caption
                movie_link_pattern = r"https?://\S+" # লিঙ্কের প্যাটার্ন
                found_links = re.findall(movie_link_pattern, original_text)

                modified_text = original_text
                # লিঙ্ক শর্টনার প্রয়োগ করুন
                for link in found_links:
                    short_link = shorten_link(link)
                    modified_text = modified_text.replace(link, short_link)

                # লিঙ্ক শর্ট করার পর ব্যবহারকারীকে নতুন মেসেজ পাঠান
                m = await cq.message.reply_text(
                    f"আপনার মুভি লিঙ্ক:\n\n{modified_text}\n\n**টিপস:** বিজ্ঞাপন ছাড়া সরাসরি মুভি পেতে আমাদের **প্রিমিয়াম সাবস্ক্রিপশন** নিন! 👇",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("প্রিমিয়াম নিন", callback_data="show_premium_info")]])
                )
                asyncio.create_task(delete_message_later(m.chat.id, m.id, delay=900)) # 15 মিনিট পর ডিলিট হবে

                await cq.answer("মুভি লিঙ্ক পাঠানো হয়েছে।")

            except Exception as e:
                await cq.answer("মুভি লোড করতে সমস্যা হয়েছে। অ্যাডমিনকে জানানো হয়েছে।", show_alert=True)
                print(f"Error processing movie callback for non-premium user: {e}")

    elif data.startswith("lang_"):
        _, lang, query = data.split("_", 2)
        lang_movies = list(movies_col.find({"language": lang}))
        matches = [
            m for m in lang_movies
            if re.search(re.escape(query), clean_text(m.get("title", "")), re.IGNORECASE)
        ]
        if matches:
            buttons = [
                [InlineKeyboardButton(m["title"][:40], callback_data=f"movie_{m['message_id']}")]
                for m in matches[:RESULTS_COUNT]
            ]
            await cq.message.edit_text(
                f"ফলাফল ({lang}) - নিচের থেকে সিলেক্ট করুন:",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        else:
            await cq.answer(f"এই ভাষায় '{query}' এর জন্য কিছু পাওয়া যায়নি।", show_alert=True)
        await cq.answer() # কলব্যাক শেষ করা


if __name__ == "__main__":
    print("বট শুরু হচ্ছে...")
    app.run()

