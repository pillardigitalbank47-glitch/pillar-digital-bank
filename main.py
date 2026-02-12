import logging
import os
import secrets
import hashlib
from datetime import datetime, timedelta
from decimal import Decimal
import pytz

# Scheduler for background tasks (Interest calculation)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# PostgreSQL Database connector
import psycopg2
from psycopg2.extras import RealDictCursor

# Original Telegram imports
from telegram import Update, ReplyKeyboardMarkup
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

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
# Database Manager
# =========================

class DatabaseManager:
    """Manages PostgreSQL database connection and operations"""
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        """Establish database connection"""
        try:
            if os.getenv("DATABASE_URL"):
                self.conn = psycopg2.connect(os.getenv("DATABASE_URL"))
                self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
                self.init_tables()
                print("✅ Database Connected")
            else:
                print("⚠️ No DATABASE_URL found. Using temporary storage.")
        except Exception as e:
            print(f"❌ DB Error: {e}")

    def init_tables(self):
        """Create necessary tables if they don't exist"""
        if not self.cursor: return

        # Users Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id BIGINT PRIMARY KEY,
                full_name VARCHAR(255),
                phone VARCHAR(20),
                pin_hash VARCHAR(255),
                referral_code VARCHAR(20),
                status VARCHAR(20) DEFAULT 'PENDING',
                balance DECIMAL(15,2) DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Savings Plans Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS savings_plans (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                plan_name VARCHAR(50),
                amount DECIMAL(15,2),
                start_date DATE,
                end_date DATE,
                daily_rate DECIMAL(5,4),
                status VARCHAR(20) DEFAULT 'ACTIVE'
            )
        """)
        self.conn.commit()

    def get_user(self, tid):
        """Retrieve user data by Telegram ID"""
        if not self.cursor: return None
        self.cursor.execute("SELECT * FROM users WHERE telegram_id = %s", (tid,))
        return self.cursor.fetchone()

    def create_user(self, tid, name, phone, pin_hash):
        """Register a new user"""
        if not self.cursor: return False
        try:
            self.cursor.execute("""
                INSERT INTO users (telegram_id, full_name, phone, pin_hash, status)
                VALUES (%s, %s, %s, %s, 'PENDING')
            """, (tid, name, phone, pin_hash))
            self.conn.commit()
            return True
        except:
            return False

    def update_balance(self, tid, amount):
        """Update user balance (add or subtract)"""
        if not self.cursor: return
        self.cursor.execute("""
            UPDATE users SET balance = balance + %s WHERE telegram_id = %s
        """, (amount, tid))
        self.conn.commit()

# Initialize DB globally
db = DatabaseManager()

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


async def health_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Bot is running.")


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
    # Setup Scheduler (NY Time 4:30 PM)
    # =========================
    scheduler = AsyncIOScheduler(timezone=pytz.timezone("America/New_York"))
    
    async def interest_job():
        """Calculates and adds interest to users"""
        logger.info("Running Interest Calculation Job...")
        # Logic to calculate interest goes here
        # For now, just logging
        pass

    # Run daily at 4:30 PM
    scheduler.add_job(interest_job, 'cron', hour=16, minute=30)
    scheduler.start()
    
    logger.info("Scheduler started.")

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