#!/usr/bin/env python3

import os
import logging
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest

# =========================
# Configuration
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # e.g. https://your-app.onrender.com

PORT = int(os.getenv("PORT", "10000"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

if not WEBHOOK_URL:
    raise RuntimeError("WEBHOOK_URL not set")


# =========================
# Logging Setup
# =========================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

# =========================
# Helper Functions
# =========================

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

# =========================
# Keyboard Builders
# =========================

from telegram import ReplyKeyboardMarkup

# 👤 User Main Menu
def user_main_menu():
    keyboard = [
        ["💰 Balance", "💳 Deposit"],
        ["🏧 Withdraw", "📜 History"],
        ["⚙ Settings"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


# 👑 Admin Main Menu
def admin_main_menu():
    keyboard = [
        ["📥 Pending Deposits", "📤 Pending Withdrawals"],
        ["👥 Users", "📊 Reports"],
        ["🧾 Audit Logs", "⚙ Admin Settings"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# =========================
# Handlers
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    # Admin welcome
    if user.id == ADMIN_ID:
        await update.message.reply_text(
            "👑 Admin Panel Initialized.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Normal user welcome
    await update.message.reply_text(
        "👋 Welcome to Pillar Digital Bank!\n\n"
        "Secure, simple, and smart banking starts here.",
        parse_mode=ParseMode.HTML,
    )
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if is_admin(user_id):
        await update.message.reply_text(
            "👑 Admin Panel Initialized.",
            reply_markup=admin_main_menu()
        )
    else:
        await update.message.reply_text(
            "🏦 Welcome to Pillar Digital Bank.",
            reply_markup=user_main_menu()
        )

async def health_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        await update.message.reply_text("✅ Bot is running.")


# =========================
# Main App Setup
# =========================

def main():

    request = HTTPXRequest(connect_timeout=10.0, read_timeout=10.0)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .build()
    )

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("health", health_check))

    # Webhook Setup
    logger.info("Starting webhook server...")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        url_path=BOT_TOKEN,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()