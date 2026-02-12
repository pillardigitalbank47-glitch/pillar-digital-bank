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
# Feature Handlers (User)
# =========================

async def handle_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💰 Balance feature coming soon.")


async def handle_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💳 Deposit system coming soon.")


async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏧 Withdraw system coming soon.")


async def handle_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📜 Transaction history coming soon.")


async def handle_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚙ Settings panel coming soon.")

    # =========================
# Feature Handlers (Admin)
# =========================

async def handle_pending_deposits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📥 Pending Deposits Panel (Coming Soon)")


async def handle_pending_withdrawals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📤 Pending Withdrawals Panel (Coming Soon)")


async def handle_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👥 User Management System (Coming Soon)")


async def handle_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 Reports Dashboard (Coming Soon)")


async def handle_audit_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🧾 Audit Logs System (Coming Soon)")


async def handle_admin_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚙ Admin Settings Panel (Coming Soon)")

# =========================
# Handlers
# =========================

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


async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id

    # 👑 Admin
    if is_admin(user_id):

        if text == "📥 Pending Deposits":
            await handle_pending_deposits(update, context)

        elif text == "📤 Pending Withdrawals":
            await handle_pending_withdrawals(update, context)

        elif text == "👥 Users":
            await handle_users(update, context)

        elif text == "📊 Reports":
            await handle_reports(update, context)

        elif text == "🧾 Audit Logs":
            await handle_audit_logs(update, context)

        elif text == "⚙ Admin Settings":
            await handle_admin_settings(update, context)

    # 👤 User
    else:

        if text == "💰 Balance":
            await handle_balance(update, context)

        elif text == "💳 Deposit":
            await handle_deposit(update, context)

        elif text == "🏧 Withdraw":
            await handle_withdraw(update, context)

        elif text == "📜 History":
            await handle_history(update, context)

        elif text == "⚙ Settings":
            await handle_settings(update, context)

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

    # =========================
    # Handlers
    # =========================

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("health", health_check))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))

    # =========================
    # Webhook Setup
    # =========================

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