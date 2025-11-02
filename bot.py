# --- FILE 1: bot.py (Part 1 of 3) ---
# --- Copy this part first. ---

import logging
import asyncio
import json
import os
import aiohttp
import aiofiles
from flask import Flask, jsonify, request, abort
from threading import Thread
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    Message, CallbackQuery
)

# -----------------------------------------------------------------
# --- 1. CONFIGURATION (FILL THIS OUT) ---
# -----------------------------------------------------------------

# --- Pyrogram API Config (Get from my.telegram.org) ---
API_ID = 1234567               # PASTE YOUR API_ID (e.g., 1234567)
API_HASH = "your_api_hash"     # PASTE YOUR API_HASH (e.g., "0123456789abcdef0123456789abcdef")
SESSION_NAME = "user_session"  # This will be the name of your session file

# --- Bot Config ---
BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"     # PASTE YOUR BOT TOKEN
ADMIN_CHAT_ID = "123456789"        # PASTE YOUR CHAT ID (Get this by sending /myid to the bot)

# --- Web & Ad Config ---
YOUR_BLOGGER_AD_PAGE_URL = "https://YOUR_BLOG.blogspot.com/p/ad-page.html" # PASTE YOUR BLOGGER URL
DASHBOARD_URL = "https://YOUR-NETLIFY-SITE.netlify.app/admin_dashboard.html" # PASTE YOUR DASHBOARD URL
API_SECRET_KEY = "your-very-secret-password-123" # CHANGE THIS to a strong password
SERVER_HOST = "0.0.0.0"            # Run on all IPs
SERVER_PORT = 3000                 # Port for your API server

# --- Gemini AI Config ---
GEMINI_API_KEY = "" # Leave as ""
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"
GEMINI_API_URL_WITH_SEARCH = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"

# --- Bot Behavior Config ---
BATCH_PROCESS_DELAY = 5000 # 5 seconds
IGNORE_WORDS = [
    'movie', 'dado', 'do', 'de', 'le', 'hai', 'bhai', 'pls', 'please', 'bro', 'send', 
    'me', 'full', 'download', 'link', 'a', 'the', 'in', 'is', 'it', 'ho', 'to', 
    'ka', 'ki', 'fullhd', 'dedo', 'new'
]
LANGUAGE_MAP = {
    'hindi': 'Hindi', 'eng': 'English', 'english': 'English', 'tamil': 'Tamil', 
    'bengali': 'Bengali', 'malayalam': 'Malayalam', 'punjabi': 'Punjabi', 
    'telugu': 'Telugu', 'marathi': 'Marathi', 'kannada': 'Kannada'
}
QUALITY_MAP = {
    '4k': '4K', '2k': '2K', '1080': '1080p', '1080p': '1080p', '720': '720p', 
    '720p': '720p', '480': '480p', '480p': '480p', 'hd': '720p', 'sd': 'SD'
}

# --- Database Paths ---
DB_PATH_FILES = './file_groups_ai.json'
DB_PATH_REQUESTS = './requests_v2.json'
DB_PATH_POPULARITY = './popularity.json'

# -----------------------------------------------------------------
# --- 2. INITIALIZE LOGGING, BOTS, & DATABASES ---
# -----------------------------------------------------------------

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Pyrogram Clients
# Bot Account (for users)
bot_app = Client("bot_session", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)
# User Account (for scraping)
user_app = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH)

# Batch processing globals
admin_batch_queues = {}
admin_batch_timers = {}
db_lock = asyncio.Lock() # Lock for thread-safe DB writes
scrape_lock = asyncio.Lock() # Lock to prevent multiple /index commands at once

# --- Database Functions (with AsyncFile for safety) ---
async def load_db(path):
    async with db_lock:
        if os.path.exists(path):
            try:
                async with aiofiles.open(path, 'r', encoding='utf-8') as f:
                    return json.loads(await f.read())
            except Exception as e:
                logger.error(f"Error loading DB {path}: {e}")
                return {}
        return {}
async def save_db(path, db):
    async with db_lock:
        try:
            async with aiofiles.open(path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(db, indent=4))
        except Exception as e:
            logger.error(f"CRITICAL: Error saving DB {path}: {e}")

# --- 3. GEMINI AI HELPER ---
async def call_gemini(session, prompt, schema=None, use_search=False):
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": "You are a helpful assistant."}]}
    }
    url = GEMINI_API_URL_WITH_SEARCH if use_search else GEMINI_API_URL

    if schema:
        payload["generationConfig"] = {
            "responseMimeType": "application/json",
            "responseSchema": schema
        }
    if use_search:
        payload["tools"] = [{"google_search": {}}]

    delay = 1000
    for i in range(3): # Retry up to 3 times
        try:
            async with session.post(url, json=payload, timeout=60) as response:
                if response.status != 200:
                    raise Exception(f"API call failed with status {response.status}")
                
                result = await response.json()
                text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text")
                
                if not text:
                    raise Exception("Invalid AI response structure.")
                
                if schema:
                    return json.loads(text) # Return JSON object
                else:
                    return text # Return plain text
        
        except Exception as e:
            logger.error(f"Gemini call attempt {i + 1} failed: {e}")
            if i < 2:
                await asyncio.sleep(delay / 1000.0)
                delay *= 2 # Exponential backoff
            else:
                return None # Failed all retries

# --- 4. POPULARITY TRACKER ---
async def track_popularity(file_data):
    try:
        group_name = file_data.get("groupName")
        lang = file_data.get("lang")
        quality = file_data.get("quality")
        
        db_key = f"{group_name}_{lang}_{quality}".lower().replace(r'[^a-z0-9_]', '').replace('__', '_')
        
        db = await load_db(DB_PATH_POPULARITY)
        
        if db_key not in db:
            db[db_key] = {"groupName": group_name, "lang": lang, "quality": quality, "count": 0}
        
        db[db_key]["count"] += 1
        await save_db(DB_PATH_POPULARITY, db)
        logger.info(f"Popularity +1 for: {group_name} ({lang} / {quality})")
    
    except Exception as e:
        logger.error(f"Error tracking popularity: {e}")

# --- 5. CENTRALIZED AI-POWERED INDEXER ---
async def process_file_for_indexing(file_message: Message):
    if not file_message or not (file_message.document or file_message.video):
        return {"status": "failure", "error": "No file object"}
    
    file = file_message.document or file_message.video
    file_name = file.file_name if file.file_name else "Untitled"
    
    prompt = f'Analyze: "{file_name}". Extract: "groupName" (canonical title), "lang" (full language name or "Unknown"), "quality" (e.g., "720p" or "SD").'
    schema = {
        "type": "OBJECT",
        "properties": {
            "groupName": {"type": "STRING"},
            "lang": {"type": "STRING"},
            "quality": {"type": "STRING"}
        },
        "required": ["groupName", "lang", "quality"]
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            ai_response = await call_gemini(session, prompt, schema, False)

        if not ai_response:
            logger.error(f"AI Indexing failed for: {file_name}")
            return {"status": "failure", "error": "AI analysis failed", "fileName": file_name}
        
        group_name = ai_response.get("groupName")
        lang = ai_response.get("lang")
        quality = ai_response.get("quality")
        file_id = file.file_id
        file_type = "video" if file_message.video else "document"
        group_id = group_name.lower().replace(r'[^a-z0-9]', '_').replace('__', '_')
        
        db = await load_db(DB_PATH_FILES)
        is_new_group = group_id not in db
        
        if is_new_group:
            db[group_id] = {"groupName": group_name, "searchAll": f"{group_name.lower()}", "languages": {}}
        
        db[group_id]["searchAll"] += f" {file_name.lower().replace(r'[^a-z0-9]', ' ')}"
        if lang not in db[group_id]["languages"]:
            db[group_id]["languages"][lang] = {}
        
        db[group_id]["languages"][lang][quality] = {
            "fileId": file_id,
            "fileName": file_name,
            "fileType": file_type
        }
        
        await save_db(DB_PATH_FILES, db)
        
        return {"status": "success", "groupName": group_name, "lang": lang, "quality": quality, "isNewGroup": is_new_group, "groupId": group_id}

    except Exception as e:
        logger.error(f'Error saving AI-indexed file to DB: {e}')
        return {"status": "failure", "error": "Database save error", "fileName": file_name}
# --- FILE 1: bot.py (Part 2 of 3) ---
# --- Copy this part after Part 1 ---

# -----------------------------------------------------------------
# --- 6. BOT LOGIC (HANDLERS) ---
# -----------------------------------------------------------------

# --- 6a. AI INDEXER (NEW FILES IN CHANNEL) ---
@bot_app.on_message(filters.chat(ADMIN_CHAT_ID) & (filters.document | filters.video) & filters.channel)
async def handle_channel_post(client: Client, message: Message):
    logger.info(f"AI Indexer: Detected new file in channel: {message.chat.id}")
    result = await process_file_for_indexing(message)
    if result["status"] == "success":
        await bot_app.send_message(message.chat.id, f'AI Indexed: {result["groupName"]} ({result["lang"]} / {result["quality"]})')
    else:
        await bot_app.send_message(message.chat.id, f'AI Indexing failed for "{result.get("fileName", "Unknown")}". Error: {result["error"]}')

# --- 6b. AI SCRAPER (OLD FILES /INDEX COMMAND) ---
@bot_app.on_message(filters.command("index") & filters.user(int(ADMIN_CHAT_ID)) & filters.private)
async def handle_index_command(client: Client, message: Message):
    if scrape_lock.locked():
        await message.reply("A scraping task is already in progress. Please wait.")
        return

    try:
        chat_id = message.text.split(" ", 1)[1] # Get channel link/ID
    except IndexError:
        await message.reply("Usage: `/index [channel_link_or_id]`")
        return

    async with scrape_lock:
        status_msg = await message.reply(f"Starting to scrape channel: {chat_id}. This may take a long time...")
        logger.info(f"Scraper: Admin triggered /index for {chat_id}")
        
        total_files = 0
        successes = 0
        failures = 0
        new_groups = set()
        db_before = await load_db(DB_PATH_FILES)

        try:
            async for file_msg in user_app.get_chat_history(chat_id):
                if file_msg.document or file_msg.video:
                    total_files += 1
                    result = await process_file_for_indexing(file_msg)
                    
                    if result["status"] == "success":
                        successes += 1
                        if result["isNewGroup"]:
                            new_groups.add(result["groupName"])
                    else:
                        failures += 1

                    if total_files % 50 == 0: # Update every 50 files
                        await status_msg.edit(f"Scraping...\nFiles Found: {total_files}\nSuccessfully Indexed: {successes}\nFailed: {failures}")
                        await asyncio.sleep(1) # Small pause

            # Send final report
            report = f"✅ **Scraping Complete!**\n\n"
            report += f"Total Messages Found: **{total_files}**\n"
            report += f"Successfully Indexed: **{successes}** files\n"
            report += f"Failed: **{failures}** files\n"
            if new_groups:
                report += f"\nNew Groups Added: **{len(new_groups)}**\n- " + "\n- ".join(list(new_groups)[:20])
                if len(new_groups) > 20:
                    report += f"\n...and {len(new_groups) - 20} more."
            
            await status_msg.edit(report)

        except FloodWait as e:
            logger.warning(f"FloodWait: Sleeping for {e.value} seconds.")
            await status_msg.edit(f"FloodWait: Sleeping for {e.value} seconds... Task will resume.")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            await status_msg.edit(f"An error occurred during scraping: {e}")

# --- 6c. AI-POWERED USER SEARCH ---
@bot_app.on_message(filters.text & filters.private & ~filters.user(int(ADMIN_CHAT_ID)))
async def handle_user_search(client: Client, message: Message):
    chat_id = message.chat.id
    original_query = message.text.lower()
    
    if original_query.startswith('/'): return

    detected_lang = None
    detected_qual = None
    final_search_words = []
    
    for word in original_query.split(' '):
        if word in IGNORE_WORDS: continue
        mapped_lang = LANGUAGE_MAP.get(word)
        if mapped_lang:
            detected_lang = mapped_lang
            continue
        mapped_qual = QUALITY_MAP.get(word)
        if mapped_qual:
            detected_qual = mapped_qual
            continue
        final_search_words.append(word)
    
    cleaned_query = ' '.join(final_search_words).replace(r'[^a-z0-9 ]', '')
    
    if len(cleaned_query) < 3:
        await message.reply("Search term must be 3+ chars.")
        return

    results = await run_search(cleaned_query)
    query_used = cleaned_query

    if not results:
        status_msg = await message.reply(f"No results for '{cleaned_query}'. Trying AI search...")
        ai_prompt = f"A user's search for '{original_query}' failed. What movie title were they likely looking for? Respond with *only* the movie title."
        
        async with aiohttp.ClientSession() as session:
            suggested_title = await call_gemini(session, ai_prompt, None, True)
        
        if suggested_title:
            clean_suggested_title = suggested_title.replace(r'["\'.]', '').lower()
            results = await run_search(clean_suggested_title)
            query_used = clean_suggested_title
            if results:
                await status_msg.edit(f"Did you mean '{clean_suggested_title}'? Showing results:")
            else:
                await status_msg.delete()
        else:
            await status_msg.delete()

    if results:
        await handle_search_results(chat_id, results, detected_lang, detected_qual)
    else:
        # Final failure: Show Request button
        final_query_to_request = query_used
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(f'Yes, request "{final_query_to_request}"', callback_data=f"request_{final_query_to_request}")]])
        await message.reply(f"Sorry, I couldn't find any files for '{final_query_to_request}'.\n\nWould you like me to add it to my request list?", reply_markup=keyboard)

async def run_search(query):
    db = await load_db(DB_PATH_FILES)
    results = []
    if len(query) < 3: return []
    for group_id, group in db.items():
        if query in group.get("searchAll", ""):
            results.append(group)
    return results

async def handle_search_results(chat_id, results, detected_lang, detected_qual):
    for group in results:
        group_name = group["groupName"]
        group_id = group_name.lower().replace(r'[^a-z0-9]', '_').replace('__', '_')
        available_languages = list(group["languages"].keys())
        
        if not available_languages: continue

        # Case 1: "jawan hindi 4k" - Perfect match
        if detected_lang and detected_qual and detected_lang in group["languages"] and detected_qual in group["languages"][detected_lang]:
            file = group["languages"][detected_lang][detected_qual]
            msg_text = f"Found '{group_name} ({detected_lang} - {detected_qual})'! Generating your link..."
            await bot_app.send_message(chat_id, msg_text)
            await send_ad_link(chat_id, {"fileId": file["fileId"], "fileName": file["fileName"], "groupName": group_name, "lang": detected_lang, "quality": detected_qual})
            continue

        # Case 2: "jawan 4k" - Lang missing
        if not detected_lang and detected_qual:
            langs_with_quality = [lang for lang in available_languages if detected_qual in group["languages"][lang]]
            if len(langs_with_quality) == 1:
                lang = langs_with_quality[0]
                file = group["languages"][lang][detected_qual]
                msg_text = f"Found '{group_name} ({lang} - {detected_qual})'! Generating your link..."
                await bot_app.send_message(chat_id, msg_text)
                await send_ad_link(chat_id, {"fileId": file["fileId"], "fileName": file["fileName"], "groupName": group_name, "lang": lang, "quality": detected_qual})
                continue
            elif len(langs_with_quality) > 1:
                buttons = [InlineKeyboardButton(lang, callback_data=f"lang_{group_id}_{lang}") for lang in langs_with_quality]
                await bot_app.send_message(chat_id, f"I found '{group_name}' in {detected_qual}. Which language?", reply_markup=InlineKeyboardMarkup([buttons]))
                continue

        # Case 3: "jawan hindi" - Quality missing
        if detected_lang and not detected_qual and detected_lang in group["languages"]:
            await ask_for_quality(chat_id, None, group, detected_lang)
            continue
            
        # Case 4: "jawan" - Both missing
        if len(available_languages) == 1:
            await ask_for_quality(chat_id, None, group, available_languages[0])
        else:
            buttons = [InlineKeyboardButton(lang, callback_data=f"lang_{group_id}_{lang}") for lang in available_languages]
            keyboard_rows = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
# --- FILE 1: bot.py (Part 3 of 3) ---
# --- Copy this part last. ---

# --- 6d. BUTTON CLICK HANDLER ---
@bot_app.on_callback_query()
async def handle_callback_query(client: Client, query: CallbackQuery):
    chat_id = query.message.chat.id
    user_id = query.from_user.id
    data = query.data

    try:
        if data.startswith("lang_") or data.startswith("qual_"):
            await query.answer()
            db = await load_db(DB_PATH_FILES)
            parts = data.split('_')
            type, group_id = parts[0], parts[1]
            group = db.get(group_id)
            if not group: raise Exception(f"Group not found: {group_id}")

            if type == "lang":
                lang = parts[2]
                await ask_for_quality(chat_id, query.message, group, lang)
            
            elif type == "qual":
                lang, quality = parts[2], parts[3]
                file = group["languages"][lang][quality]
                await query.message.reply_text(f"Generating link for '{group['groupName']} ({lang} - {quality})'...")
                await send_ad_link(chat_id, {"fileId": file["fileId"], "fileName": file["fileName"], "groupName": group["groupName"], "lang": lang, "quality": quality})
                await query.message.edit_reply_markup(None) # Remove buttons

        elif data.startswith("request_"):
            query_to_request = data.split("_", 1)[1]
            requests_db = await load_db(DB_PATH_REQUESTS)
            
            if query_to_request not in requests_db:
                requests_db[query_to_request] = []
            
            if user_id not in requests_db[query_to_request]:
                requests_db[query_to_request].append(user_id)
                await save_db(DB_PATH_REQUESTS, requests_db)
                await query.answer("Request added!", show_alert=False)
                await query.message.edit_text(f"Great! I've added '{query_to_request}' to the admin's request list. **You will be notified when it's available.**", parse_mode=enums.ParseMode.MARKDOWN)
                logger.info(f"New Request: '{query_to_request}' from user {user_id}")
            else:
                await query.answer("You already requested this!", show_alert=False)
                await query.message.edit_text(f"You have already requested '{query_to_request}'. I'll let you know!", parse_mode=enums.ParseMode.MARKDOWN)

    except Exception as e:
        logger.error(f"Error in callback query: {e}")
        await query.message.reply_text("Sorry, something went wrong.")

# --- 6e. "ASK FOR QUALITY" HELPER ---
async def ask_for_quality(chat_id, original_message: Message, group, lang):
    if lang not in group["languages"]:
        await bot_app.send_message(chat_id, f"Sorry, I don't have '{group['groupName']}' in {lang}.")
        return
        
    qualities = list(group["languages"][lang].keys())
    group_id = group["groupName"].lower().replace(r'[^a-z0-9]', '_').replace('__', '_')
    
    if len(qualities) == 1:
        quality = qualities[0]
        file = group["languages"][lang][quality]
        msg_text = f"Found '{group['groupName']} ({lang} - {quality})'! Generating your link..."
        
        if original_message:
            await original_message.edit_text(msg_text)
        else:
            await bot_app.send_message(chat_id, msg_text)
        await send_ad_link(chat_id, {"fileId": file["fileId"], "fileName": file["fileName"], "groupName": group["groupName"], "lang": lang, "quality": quality})
    
    else:
        buttons = [InlineKeyboardButton(qual, callback_data=f"qual_{group_id}_{lang}_{qual}") for qual in qualities]
        keyboard = InlineKeyboardMarkup([buttons])
        msg_text = f"You selected {lang}. Now, which quality do you need?"
        
        if original_message:
            await original_message.edit_text(msg_text, reply_markup=keyboard)
        else:
            await bot_app.send_message(chat_id, f"I found '{group['groupName']} ({lang})'. Which quality do you need?", reply_markup=keyboard)

# --- 6f. "SEND AD LINK" HELPER ---
async def send_ad_link(chat_id, file_data):
    if "YOUR_BLOG" in YOUR_BLOGGER_AD_PAGE_URL:
        await bot_app.send_message(chat_id, "Bot is not configured. Admin needs to set the Blogger URL.")
        logger.error("CRITICAL: YOUR_BLOGGER_AD_PAGE_URL is not set.")
        return
    
    try:
        # Use the User Account to generate the file link, as it's more reliable
        long_link = await user_app.get_download_link(file_data["fileId"])
        encoded_link = aiohttp.helpers.quote(long_link)
        ad_page_link = f"{YOUR_BLOGGER_AD_PAGE_URL}?dest={encoded_link}"
        reply_message = f"File: {file_data['fileName']}\nLink: {ad_page_link}"
        
        await bot_app.send_message(chat_id, reply_message, disable_web_page_preview=True)
        await track_popularity(file_data) # Track this click
        
    except Exception as e:
        logger.error(f"Error getting file link: {e}")
        await bot_app.send_message(chat_id, "Sorry, I couldn't generate the download link. This might be a temporary Telegram issue.")

# -----------------------------------------------------------------
# --- 7. ADMIN COMMANDS (BOT HANDLERS) ---
# -----------------------------------------------------------------

@bot_app.on_message(filters.command("myid") & filters.private)
async def handle_myid(client: Client, message: Message):
    await message.reply(f"Your Chat ID is: `{message.chat.id}`\n\nPaste this ID into the `ADMIN_CHAT_ID` variable.", parse_mode=enums.ParseMode.MARKDOWN)

@bot_app.on_message(filters.command("requests") & filters.user(int(ADMIN_CHAT_ID)) & filters.private)
async def handle_requests(client: Client, message: Message):
    requests_db = await load_db(DB_PATH_REQUESTS)
    if not requests_db:
        await message.reply("The request list is currently empty.")
        return
    
    request_counts = {title: len(user_ids) for title, user_ids in requests_db.items()}
    sorted_requests = sorted(request_counts.items(), key=lambda item: item[1], reverse=True)
    
    reply_message = "🏆 Top 20 Movie Requests:\n\n"
    for i, (title, count) in enumerate(sorted_requests[:20]):
        reply_message += f"{i + 1}. `{title}` ({count} requests)\n"
    
    await message.reply(reply_message, parse_mode=enums.ParseMode.MARKDOWN)

@bot_app.on_message(filters.command("clearrequests") & filters.user(int(ADMIN_CHAT_ID)) & filters.private)
async def handle_clear_requests(client: Client, message: Message):
    await save_db(DB_PATH_REQUESTS, {})
    await message.reply("✅ The movie request list has been cleared.")

@bot_app.on_message(filters.command("popularity") & filters.user(int(ADMIN_CHAT_ID)) & filters.private)
async def handle_popularity(client: Client, message: Message):
    pop_db = await load_db(DB_PATH_POPULARITY)
    if not pop_db:
        await message.reply("No popularity data recorded yet.")
        return
        
    sorted_list = sorted(pop_db.values(), key=lambda item: item["count"], reverse=True)
    
    reply_message = "🔥 Top 20 Most Popular Files:\n\n"
    for i, item in enumerate(sorted_list[:20]):
        reply_message += f"{i + 1}. `{item['groupName']} ({item['lang']} / {item['quality']})` - {item['count']} Clicks\n"
    
    await message.reply(reply_message, parse_mode=enums.ParseMode.MARKDOWN)

@bot_app.on_message(filters.command("broadcast") & filters.user(int(ADMIN_CHAT_ID)) & filters.private)
async def handle_broadcast(client: Client, message: Message):
    try:
        admin_query = message.text.split(" ", 1)[1].lower()
    except IndexError:
        await message.reply("Usage: `/broadcast [Movie Title]`")
        return

    requests_db = await load_db(DB_PATH_REQUESTS)
    target_title = None
    target_user_ids = []
    
    for title, user_ids in requests_db.items():
        if title.lower() == admin_query:
            target_title = title
            target_user_ids = user_ids
            break
            
    if not target_title:
        await message.reply(f"Error: Could not find '{admin_query}' in the request list.")
        return

    await message.reply(f"Starting broadcast for '{target_title}' to {len(target_user_ids)} users...")
    success_count = 0
    
    for user_id in target_user_ids:
        try:
            await bot_app.send_message(user_id, f"Good news! The movie you requested, '{target_title}', is now available.\n\nSend '{target_title}' to the bot to get your link!")
            success_count += 1
            await asyncio.sleep(0.1) # 100ms delay for rate limit
        except Exception as e:
            logger.warning(f"Failed to send broadcast to user {user_id}: {e}")
            
    del requests_db[target_title]
    await save_db(DB_PATH_REQUESTS, requests_db)
    
    await message.reply(f"Broadcast complete!\nMessage sent to {success_count} / {len(target_user_ids)} users.\nRequest for '{target_title}' has been cleared.")

@bot_app.on_message(filters.command("admin") & filters.user(int(ADMIN_CHAT_ID)) & filters.private)
async def handle_admin(client: Client, message: Message):
    if "YOUR-NETLIFY-SITE" in DASHBOARD_URL:
        await message.reply("Admin Error: `DASHBOARD_URL` is not set in the bot script.")
        return
        
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Open Mission Control", web_app={"url": DASHBOARD_URL})]])
    await message.reply("Welcome, Admin. Here is your dashboard link:", reply_markup=keyboard)

# -----------------------------------------------------------------
# --- 8. FLASK WEB SERVER (For Admin Dashboard API) ---
# -----------------------------------------------------------------

flask_app = Flask(__name__)

@flask_app.route('/api/dashboard_data', methods=['GET'])
async def get_dashboard_data():
    if request.args.get('secret') != API_SECRET_KEY:
        abort(403, description="Forbidden: Invalid API Secret")
    
    try:
        requests_db = await load_db(DB_PATH_REQUESTS)
        popularity_db = await load_db(DB_PATH_POPULARITY)
        files_db = await load_db(DB_PATH_FILES)
        
        request_counts = {title: len(user_ids) for title, user_ids in requests_db.items()}
        sorted_requests = sorted(request_counts.items(), key=lambda item: item[1], reverse=True)
        
        pop_list = sorted(popularity_db.values(), key=lambda item: item["count"], reverse=True)
        
        return jsonify({
            "topRequests": [{"title": title, "count": count} for title, count in sorted_requests[:20]],
            "topPopular": pop_list[:20],
            "totalFiles": len(files_db),
            "totalRequests": len(requests_db),
            "totalClicks": sum(item["count"] for item in pop_list)
        })
    except Exception as e:
        logger.error(f"Error in /api/dashboard_data: {e}")
        abort(500, description="Internal
            await bot_app.send_message(chat_id, f"I found '{group_name}'. Which language do you need?", reply_markup=InlineKeyboardMarkup(keyboard_rows))