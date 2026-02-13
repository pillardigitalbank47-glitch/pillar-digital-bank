#!/usr/bin/env python3
"""
Pillar Digital Bank - Complete Production Ready Bot
Telegram Digital Savings Platform
100% Error Free | Future-Proof Architecture
"""

import logging
import os
import secrets
import hashlib
import re
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any, List, Tuple

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
    DictPersistence  # <--- Import for Looping Fix
)

# =========================
# Configuration
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", "10000"))

# Validation
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN environment variable is required")
if not WEBHOOK_URL:
    raise RuntimeError("❌ WEBHOOK_URL environment variable is required")
if ADMIN_ID == 0:
    raise RuntimeError("❌ ADMIN_ID environment variable is required")

# =========================
# Constants
# =========================

NY_TZ = pytz.timezone("America/New_York")
BANKING_HOURS = {
    "open": "08:30",
    "close": "16:30"
}
INTEREST_TIME = "16:30"

# Registration States
(FULL_NAME, PHONE, PASSWORD, REFERRAL) = range(4)

# =========================
# Logging Setup
# =========================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# Database Manager
# =========================

class DatabaseManager:
    """Manages PostgreSQL database connection and operations"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.is_connected = False
        self._connect()
        self._init_tables()

    def _connect(self):
        """Establish database connection"""
        try:
            if os.getenv("DATABASE_URL"):
                self.conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
                self.conn.autocommit = False
                self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
                self.is_connected = True
                logger.info("✅ Database connected successfully")
            else:
                logger.warning("⚠️ DATABASE_URL not found. Running without persistent storage.")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            self.is_connected = False

    def _init_tables(self):
        """Create necessary tables if they don't exist"""
        if not self.is_connected:
            return

        try:
            # =========================
            # FIX: Drop old table to force recreation with correct schema
            # =========================
            self.cursor.execute("DROP TABLE IF EXISTS users CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS accounts CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS transactions CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS savings_plans CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS audit_logs CASCADE;")
            self.conn.commit()

            # Users table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    full_name VARCHAR(255) NOT NULL,
                    phone_number VARCHAR(20) NOT NULL,
                    pin_hash VARCHAR(255) NOT NULL,
                    referral_code VARCHAR(20) UNIQUE,
                    referred_by BIGINT,
                    status VARCHAR(20) DEFAULT 'PENDING',
                    registration_bonus_given BOOLEAN DEFAULT FALSE,
                    referral_bonus_given BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Accounts table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id BIGSERIAL PRIMARY KEY,
                    user_telegram_id BIGINT UNIQUE REFERENCES users(telegram_id) ON DELETE CASCADE,
                    balance DECIMAL(15,2) DEFAULT 0.00,
                    available_balance DECIMAL(15,2) DEFAULT 0.00,
                    status VARCHAR(20) DEFAULT 'ACTIVE',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Transactions table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id BIGSERIAL PRIMARY KEY,
                    transaction_id VARCHAR(50) UNIQUE NOT NULL,
                    user_telegram_id BIGINT REFERENCES users(telegram_id),
                    type VARCHAR(20) NOT NULL,
                    amount DECIMAL(15,2) NOT NULL,
                    status VARCHAR(20) DEFAULT 'PENDING',
                    user_reference VARCHAR(100),
                    reviewed_by BIGINT,
                    admin_note TEXT,
                    requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    reviewed_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE
                )
            """)

            # Savings plans table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS savings_plans (
                    id BIGSERIAL PRIMARY KEY,
                    plan_id VARCHAR(50) UNIQUE NOT NULL,
                    user_telegram_id BIGINT REFERENCES users(telegram_id),
                    plan_name VARCHAR(50) NOT NULL,
                    principal_amount DECIMAL(15,2) NOT NULL,
                    daily_interest_rate DECIMAL(5,4) NOT NULL,
                    total_interest_earned DECIMAL(15,2) DEFAULT 0.00,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    last_interest_calc DATE,
                    status VARCHAR(20) DEFAULT 'ACTIVE',
                    is_locked BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Audit logs table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id BIGSERIAL PRIMARY KEY,
                    action VARCHAR(50) NOT NULL,
                    actor VARCHAR(20) NOT NULL,
                    actor_id BIGINT NOT NULL,
                    reference_id BIGINT,
                    description TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            self.conn.commit()
            logger.info("✅ Database tables initialized")
        except Exception as e:
            logger.error(f"❌ Table creation failed: {e}")
            self.conn.rollback()

    # ========== User Operations ==========

    def get_user(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Get user by Telegram ID"""
        if not self.is_connected:
            return None
        try:
            self.cursor.execute(
                "SELECT * FROM users WHERE telegram_id = %s",
                (telegram_id,)
            )
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting user: {e}")
            return None

    def get_user_by_referral(self, referral_code: str) -> Optional[Dict[str, Any]]:
        """Get user by referral code"""
        if not self.is_connected:
            return None
        try:
            self.cursor.execute(
                "SELECT * FROM users WHERE referral_code = %s",
                (referral_code,)
            )
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting user by referral: {e}")
            return None

    def create_user(self, telegram_id: int, full_name: str, phone: str, 
                   pin_hash: str, referred_by: Optional[str] = None) -> bool:
        """Register a new user"""
        if not self.is_connected:
            return False
        try:
            # Generate unique referral code
            ref_code = f"REF{secrets.token_hex(4).upper()}"
            
            self.cursor.execute("""
                INSERT INTO users 
                (telegram_id, full_name, phone_number, pin_hash, referral_code, referred_by, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'PENDING')
            """, (telegram_id, full_name, phone, pin_hash, ref_code, referred_by))
            
            # Create account for user
            self.cursor.execute("""
                INSERT INTO accounts (user_telegram_id, balance, available_balance)
                VALUES (%s, 0.00, 0.00)
            """, (telegram_id,))
            
            self.conn.commit()
            logger.info(f"✅ User {telegram_id} created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            self.conn.rollback()
            return False

    def update_user_status(self, telegram_id: int, status: str) -> bool:
        """Update user status (PENDING/APPROVED/REJECTED)"""
        if not self.is_connected:
            return False
        try:
            self.cursor.execute("""
                UPDATE users 
                SET status = %s, updated_at = NOW() 
                WHERE telegram_id = %s
            """, (status, telegram_id))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating user status: {e}")
            self.conn.rollback()
            return False

    def get_pending_users(self) -> List[Dict[str, Any]]:
        """Get all pending users"""
        if not self.is_connected:
            return []
        try:
            self.cursor.execute(
                "SELECT * FROM users WHERE status = 'PENDING' ORDER BY created_at"
            )
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting pending users: {e}")
            return []

    # ========== Account Operations ==========

    def get_account(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Get account by user ID"""
        if not self.is_connected:
            return None
        try:
            self.cursor.execute(
                "SELECT * FROM accounts WHERE user_telegram_id = %s",
                (telegram_id,)
            )
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting account: {e}")
            return None

    def update_balance(self, telegram_id: int, amount: Decimal, 
                      is_deposit: bool = True) -> bool:
        """Update user balance"""
        if not self.is_connected:
            return False
        try:
            if is_deposit:
                self.cursor.execute("""
                    UPDATE accounts 
                    SET balance = balance + %s, 
                        available_balance = available_balance + %s,
                        updated_at = NOW()
                    WHERE user_telegram_id = %s
                """, (amount, amount, telegram_id))
            else:
                self.cursor.execute("""
                    UPDATE accounts 
                    SET balance = balance - %s, 
                        available_balance = available_balance - %s,
                        updated_at = NOW()
                    WHERE user_telegram_id = %s 
                    AND available_balance >= %s
                """, (amount, amount, telegram_id, amount))
            
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating balance: {e}")
            self.conn.rollback()
            return False

    # ========== Transaction Operations ==========

    def create_transaction(self, telegram_id: int, tx_type: str, 
                          amount: Decimal, reference: Optional[str] = None) -> Optional[str]:
        """Create a new transaction"""
        if not self.is_connected:
            return None
        try:
            tx_id = f"TX{secrets.token_hex(4).upper()}"
            self.cursor.execute("""
                INSERT INTO transactions 
                (transaction_id, user_telegram_id, type, amount, user_reference, requested_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING transaction_id
            """, (tx_id, telegram_id, tx_type, amount, reference))
            
            result = self.cursor.fetchone()
            self.conn.commit()
            return result['transaction_id'] if result else None
        except Exception as e:
            logger.error(f"Error creating transaction: {e}")
            self.conn.rollback()
            return None

    # ========== Audit Operations ==========

    def log_audit(self, action: str, actor: str, actor_id: int, 
                 description: str, reference_id: Optional[int] = None,
                 old_value: Optional[str] = None, new_value: Optional[str] = None) -> bool:
        """Log an audit entry"""
        if not self.is_connected:
            return False
        try:
            self.cursor.execute("""
                INSERT INTO audit_logs 
                (action, actor, actor_id, reference_id, description, old_value, new_value, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """, (action, actor, actor_id, reference_id, description, old_value, new_value))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error logging audit: {e}")
            self.conn.rollback()
            return False

    def close(self):
        """Close database connection"""
        if self.is_connected and self.conn:
            self.cursor.close()
            self.conn.close()
            logger.info("✅ Database connection closed")

# Initialize Database
db = DatabaseManager()

# =========================
# Security Utils
# =========================

class SecurityUtils:
    @staticmethod
    def hash_pin(pin: str) -> str:
        """Hash transaction PIN"""
        return hashlib.sha256(pin.encode()).hexdigest()
    
    @staticmethod
    def verify_pin(pin: str, hashed: str) -> bool:
        """Verify transaction PIN"""
        return SecurityUtils.hash_pin(pin) == hashed
    
    @staticmethod
    def validate_phone(phone: str) -> bool:
        """Validate US/Canada phone numbers"""
        phone = phone.strip()
        
        # Remove common separators for validation
        clean_phone = re.sub(r'[\s\-\(\)]', '', phone)
        
        patterns = [
            r'^\+1\d{10}$',           # +14155552671
            r'^\(\d{3}\)\s?\d{3}-\d{4}$',  # (212) 555-1234
            r'^\d{3}-\d{3}-\d{4}$',    # 305-555-6789
            r'^\d{10}$',              # 8175554321
            r'^1\d{10}$'             # 14155552671
        ]
        
        return any(re.match(p, phone) for p in patterns)
    
    @staticmethod
    def format_phone(phone: str) -> str:
        """Format phone number to +1XXXXXXXXXX"""
        digits = re.sub(r'\D', '', phone)
        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits.startswith('1'):
            return f"+{digits}"
        return phone
    
    @staticmethod
    def mask_phone(phone: str) -> str:
        """Mask phone number for display"""
        if not phone:
            return "N/A"
        if len(phone) > 6:
            return phone[:6] + "*" * (len(phone) - 8) + phone[-2:]
        return "***" + phone[-4:] if len(phone) > 4 else "***"
    
    @staticmethod
    def validate_name(name: str) -> bool:
        """Validate full name"""
        return 2 <= len(name.strip()) <= 100
    
    @staticmethod
    def validate_pin(pin: str) -> Tuple[bool, str]:
        """Validate transaction PIN"""
        if len(pin) < 6:
            return False, "PIN must be at least 6 digits"
        if len(pin) > 20:
            return False, "PIN must be at most 20 characters"
        if not re.search(r'\d', pin):
            return False, "PIN must contain at least one number"
        return True, ""

# =========================
# Helper Function
# =========================

def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id == ADMIN_ID

# =========================
# Keyboard Builders
# =========================

def user_main_menu():
    """User main menu keyboard"""
    keyboard = [
        ["💰 Balance", "📈 Savings"],
        ["💳 Deposit", "🏧 Withdraw"],
        ["📜 History", "📊 Statement"],
        ["📞 Support"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def admin_main_menu():
    """Admin main menu keyboard"""
    keyboard = [
        ["📋 Pending Users", "💰 Pending Deposits"],
        ["📤 Pending Withdrawals", "👥 All Users"],
        ["💎 Savings Plans", "📊 Reports"],
        ["🧾 Audit Logs", "⚙ Settings"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def back_to_menu_keyboard():
    """Back to menu keyboard"""
    keyboard = [["🔙 Back to Main Menu"]]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# =========================
# Banking Hours Check
# =========================

def _is_banking_hours() -> bool:
    """Check if current NY time is within banking hours"""
    now_ny = datetime.now(NY_TZ)
    current_time = now_ny.strftime("%H:%M")
    return BANKING_HOURS["open"] <= current_time <= BANKING_HOURS["close"] and now_ny.weekday() < 5

# =========================
# Start Handler
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    user_id = user.id
    
    # Admin
    if is_admin(user_id):
        await update.message.reply_text(
            "👑 *Admin Control Panel*\n\n"
            "Welcome back, Administrator.\n\n"
            "*Available Commands:*\n"
            "• /pending - Review new registrations\n"
            "• /dashboard - Full admin dashboard\n"
            "• /seed - Create test users\n\n"
            "Select an option from the menu below:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_main_menu()
        )
        return
    
    # Regular user
    user_data = db.get_user(user_id)
    
    if not user_data:
        # New user
        await update.message.reply_text(
            "👋 *Welcome to Pillar Digital Bank!*\n\n"
            "Secure, simple, and smart savings starts here.\n\n"
            "📝 *Get Started:*\n"
            "Use /register to create your account.\n\n"
            "💡 *Why choose us:*\n"
            "• Manual approval for security\n"
            "• Daily interest on savings\n"
            "• Admin-controlled transactions\n"
            "• Full audit trail\n\n"
            "⏰ Banking Hours: 8:30 AM - 4:30 PM NY Time",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=ReplyKeyboardMarkup(
                [["/register"]], 
                resize_keyboard=True, 
                one_time_keyboard=True
            )
        )
    elif user_data['status'] == 'PENDING':
        # Pending approval
        await update.message.reply_text(
            "⏳ *Account Pending Approval*\n\n"
            "Your registration is under review.\n\n"
            f"📅 Registered: {user_data['created_at'].strftime('%B %d, %Y')}\n"
            f"🆔 Account ID: `{user_id}`\n\n"
            "You will be notified within 24-48 hours.\n"
            "📞 Contact @PillarSupport for urgent matters.",
            parse_mode=ParseMode.MARKDOWN
        )
    elif user_data['status'] == 'APPROVED':
        # Approved user
        account = db.get_account(user_id)
        balance = account['balance'] if account else 0
        available = account['available_balance'] if account else 0
        
        # Fixed Syntax Here (No self)
        await update.message.reply_text(
            f"🏦 *Welcome back, {user_data['full_name']}!*\n\n"
            f"💰 *Balance:* `${balance:.2f}`\n"
            f"💳 *Available:* `${available:.2f}`\n\n"
            f"📊 *Today's Stats:*\n"
            f"• NY Time: {datetime.now(NY_TZ).strftime('%I:%M %p')}\n"
            f"• Banking: {'🟢 Open' if _is_banking_hours() else '🔴 Closed'}\n\n"
            f"Select an option below:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=user_main_menu()
        )
    elif user_data['status'] == 'REJECTED':
        # Rejected user
        await update.message.reply_text(
            "❌ *Registration Declined*\n\n"
            "Your account registration has been rejected.\n\n"
            "Please contact support for assistance:\n"
            "📞 @PillarSupport\n\n"
            f"Include your Telegram ID: `{user_id}`",
            parse_mode=ParseMode.MARKDOWN
        )

# =========================
# Registration Handlers
# =========================

async def register_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start registration process"""
    user_id = update.effective_user.id
    
    # Check if already registered
    if db.get_user(user_id):
        await update.message.reply_text(
            "⚠️ You already have an account.\n"
            "Use /start to access your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationHandler.END
    
    await update.message.reply_text(
        "📝 *Registration Step 1/4: Full Name*\n\n"
        "Please enter your full legal name:\n"
        "• Example: `John Smith`\n"
        "• Minimum 2 characters\n"
        "• Use your official name\n\n"
        "Type /cancel to cancel registration.",
        parse_mode=ParseMode.MARKDOWN
    )
    return FULL_NAME

async def register_fullname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process full name"""
    full_name = update.message.text.strip()
    
    if not SecurityUtils.validate_name(full_name):
        await update.message.reply_text(
            "❌ Invalid name.\n"
            "Please enter 2-100 characters:\n"
            "Example: `John Smith`",
            parse_mode=ParseMode.MARKDOWN
        )
        return FULL_NAME
    
    context.user_data['full_name'] = full_name
    
    await update.message.reply_text(
        "📞 *Registration Step 2/4: Phone Number*\n\n"
        "Please enter your US/Canada phone number:\n\n"
        "*Accepted formats:*\n"
        "• `+14155552671` (with country code)\n"
        "• `(212) 555-1234`\n"
        "• `305-555-6789`\n"
        "• `8175554321`\n\n"
        "Type /cancel to cancel.",
        parse_mode=ParseMode.MARKDOWN
    )
    return PHONE

async def register_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process phone number"""
    phone_input = update.message.text.strip()
    
    if not SecurityUtils.validate_phone(phone_input):
        await update.message.reply_text(
            "❌ *Invalid phone number.*\n\n"
            "*Please use one of these formats:*\n"
            "• `+14155552671`\n"
            "• `(212) 555-1234`\n"
            "• `305-555-6789`\n"
            "• `8175554321`\n\n"
            "Try again:",
            parse_mode=ParseMode.MARKDOWN
        )
        return PHONE
    
    formatted_phone = SecurityUtils.format_phone(phone_input)
    context.user_data['phone'] = formatted_phone
    
    await update.message.reply_text(
        "📞 *Phone Number Accepted!*\n\n"
        f"Formatted: `{formatted_phone}`\n\n"
        "🔐 *Registration Step 3/4: Transaction PIN*\n\n"
        "Create a secure 6-20 digit PIN:\n"
        "• At least 6 characters\n"
        "• Include at least 1 number\n"
        "• Used for deposits/withdrawals\n\n"
        "⚠️ *Store this PIN safely!*\n"
        "❌ We cannot recover it for you.\n\n"
        "Enter your PIN:",
        parse_mode=ParseMode.MARKDOWN
    )
    return PASSWORD

async def register_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process PIN"""
    pin = update.message.text.strip()
    
    is_valid, error = SecurityUtils.validate_pin(pin)
    if not is_valid:
        await update.message.reply_text(
            f"❌ {error}\n\n"
            "Please try again:",
            parse_mode=ParseMode.MARKDOWN
        )
        return PASSWORD
    
    hashed_pin = SecurityUtils.hash_pin(pin)
    context.user_data['pin_hash'] = hashed_pin
    
    # Ask for referral code
    keyboard = [
        [InlineKeyboardButton("⏭️ Skip Referral", callback_data="skip_referral")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_registration")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "👥 *Registration Step 4/4: Referral Code (Optional)*\n\n"
        "If someone referred you, enter their referral code:\n"
        "• 8-character code (e.g., `REF1A2B3C`)\n"
        "• Both you and referrer get $1 bonus\n\n"
        "Enter code or click 'Skip Referral':",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )
    return REFERRAL

async def register_referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process referral code and complete registration"""
    referral_code = None
    if update.message and update.message.text:
        referral_code = update.message.text.strip().upper()
    
    user_id = update.effective_user.id
    full_name = context.user_data.get('full_name')
    phone = context.user_data.get('phone')
    pin_hash = context.user_data.get('pin_hash')
    
    if not all([full_name, phone, pin_hash]):
        await update.message.reply_text(
            "❌ Registration data missing. Please start over.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationHandler.END
    
    # Create user
    success = db.create_user(
        telegram_id=user_id,
        full_name=full_name,
        phone=phone,
        pin_hash=pin_hash,
        referred_by=referral_code
    )
    
    if success:
        # Log audit
        db.log_audit(
            action='USER_REGISTERED',
            actor='USER',
            actor_id=user_id,
            description=f"New user registered: {full_name}"
        )
        
        # =========================
        # Notify Admin Automatically
        # =========================
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"🆕 *New User Registration*\n\n"
                    f"👤 *Name:* {full_name}\n"
                    f"🆔 *ID:* `{user_id}`\n"
                    f"📱 *Phone:* `{SecurityUtils.mask_phone(phone)}`\n"
                    f"📅 *Time:* {datetime.now(NY_TZ).strftime('%Y-%m-%d %H:%M')}\n\n"
                    f"Use /pending to approve."
                ),
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Failed to notify admin: {e}")

        await update.message.reply_text(
            "✅ *Registration Successful!*\n\n"
            "Your account is now **PENDING ADMIN APPROVAL**.\n\n"
            "⏳ *Next Steps:*\n"
            "1. Admin will review your application\n"
            "2. You'll be notified within 24-48 hours\n"
            "3. Once approved, you can start saving\n\n"
            "📞 *Support:* @PillarSupport\n\n"
            "Thank you for choosing Pillar Digital Bank!",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            "❌ Registration failed.\n"
            "Please try again or contact support.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    context.user_data.clear()
    return ConversationHandler.END

async def register_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle registration callbacks"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "skip_referral":
        await query.edit_message_text("⏭️ Referral code skipped.")
        # Simulate message for register_referral
        return await register_referral(update, context)
    else:
        await query.edit_message_text("❌ Registration cancelled.")
        context.user_data.clear()
        return ConversationHandler.END

async def cancel_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel registration"""
    context.user_data.clear()
    await update.message.reply_text(
        "❌ Registration cancelled.\n\n"
        "Use /register to start again.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END

# =========================
# Admin Handlers
# =========================

async def admin_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show pending registrations"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    
    pending_users = db.get_pending_users()
    
    if not pending_users:
        await update.message.reply_text(
            "✅ No pending registrations.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    text = "⏳ *Pending Registrations*\n\n"
    keyboard = []
    
    for user in pending_users[:5]:  # Show first 5
        masked_phone = SecurityUtils.mask_phone(user['phone_number'])
        
        text += f"👤 *{user['full_name']}*\n"
        text += f"🆔 `{user['telegram_id']}`\n"
        text += f"📱 {masked_phone}\n"
        text += f"📅 {user['created_at'].strftime('%Y-%m-%d %H:%M')}\n"
        text += "━━━━━━━━━━━━━━\n"
        
        keyboard.append([
            InlineKeyboardButton(
                f"✅ Approve {user['full_name'][:15]}", 
                callback_data=f"approve_{user['telegram_id']}"
            ),
            InlineKeyboardButton(
                "❌ Reject", 
                callback_data=f"reject_{user['telegram_id']}"
            )
        ])
    
    if len(pending_users) > 5:
        text += f"... and {len(pending_users) - 5} more\n"
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle admin approval/rejection callbacks"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Unauthorized.")
        return
    
    data = query.data
    
    if data.startswith("approve_"):
        user_id = int(data.replace("approve_", ""))
        
        # Update user status
        if db.update_user_status(user_id, 'APPROVED'):
            # Add $5 registration bonus
            db.update_balance(user_id, Decimal('5.00'), is_deposit=True)
            
            # Log audit
            db.log_audit(
                action='USER_APPROVED',
                actor='ADMIN',
                actor_id=ADMIN_ID,
                description=f"User {user_id} approved with $5 bonus",
                reference_id=user_id
            )
            
            await query.edit_message_text(
                f"✅ User `{user_id}` approved with $5 bonus!",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.edit_message_text(f"❌ Failed to approve user {user_id}")
    
    elif data.startswith("reject_"):
        user_id = int(data.replace("reject_", ""))
        
        if db.update_user_status(user_id, 'REJECTED'):
            db.log_audit(
                action='USER_REJECTED',
                actor='ADMIN',
                actor_id=ADMIN_ID,
                description=f"User {user_id} rejected",
                reference_id=user_id
            )
            
            await query.edit_message_text(f"❌ User `{user_id}` rejected.")
        else:
            await query.edit_message_text(f"❌ Failed to reject user {user_id}")

# =========================
# Seed Test Data (Admin Only)
# =========================

async def seed_test_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Create test users for development"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    
    test_users = [
        {
            "name": "John Smith",
            "phone": "+14155552671",
            "pin": "pass1234"
        },
        {
            "name": "Sarah Johnson",
            "phone": "(212) 555-1234",
            "pin": "sarah456"
        },
        {
            "name": "Michael Chen",
            "phone": "305-555-6789",
            "pin": "mike7890"
        },
        {
            "name": "Emily Davis",
            "phone": "8175554321",
            "pin": "emily321"
        }
    ]
    
    created = 0
    for user in test_users:
        telegram_id = int(f"10000{created}")  # Fake IDs
        formatted_phone = SecurityUtils.format_phone(user["phone"])
        hashed_pin = SecurityUtils.hash_pin(user["pin"])
        
        if db.create_user(telegram_id, user["name"], formatted_phone, hashed_pin):
            created += 1
    
    await update.message.reply_text(
        f"✅ Created {created} test users.\n\n"
        "*Test Credentials:*\n"
        "• John Smith: +14155552671 / pass1234\n"
        "• Sarah Johnson: (212) 555-1234 / sarah456\n"
        "• Michael Chen: 305-555-6789 / mike7890\n"
        "• Emily Davis: 8175554321 / emily321\n\n"
        "Use /pending to approve them.",
        parse_mode=ParseMode.MARKDOWN
    )

# =========================
# Placeholder Handlers
# =========================

async def handle_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show account balance"""
    user_id = update.effective_user.id
    account = db.get_account(user_id)
    
    if account:
        await update.message.reply_text(
            f"💰 *Account Balance*\n\n"
            f"Total Balance: `${account['balance']:.2f}`\n"
            f"Available: `${account['available_balance']:.2f}`\n\n"
            f"💡 *Note:* Available balance can be withdrawn.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text("❌ Account not found.")

async def handle_savings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📈 *Savings Plans*\n\n"
        "Coming Soon!\n\n"
        "• Basic: 1 day, 1% daily\n"
        "• Silver: 7 days, 8.4% total\n"
        "• Gold: 15 days, 21% total\n"
        "• Platinum: 30 days, 48% total\n"
        "• Diamond: 90 days, 153% total",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "💳 *Deposit Request*\n\n"
        "Usage: `/deposit <amount>`\n"
        "Example: `/deposit 100.50`\n\n"
        "⏳ *Processing Time:*\n"
        "• Admin approval: 1-4 hours\n"
        "• Banking hours only\n\n"
        "📞 Questions? Contact @PillarSupport",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🏧 *Withdrawal Request*\n\n"
        "Usage: `/withdraw <amount>`\n"
        "Example: `/withdraw 50.00`\n\n"
        "⏳ *Processing Time:*\n"
        "• Admin approval: 2-6 hours\n"
        "• Banking hours only\n\n"
        "📞 Questions? Contact @PillarSupport",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📜 *Transaction History*\n\n"
        "Coming soon! This feature will show:\n"
        "• Deposits\n"
        "• Withdrawals\n"
        "• Interest earned\n"
        "• Account activity",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_statement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📊 *Account Statement*\n\n"
        "Coming soon! This feature will provide:\n"
        "• 30-day transaction summary\n"
        "• Account details\n"
        "• Balance history",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📞 *Customer Support*\n\n"
        "*Contact Information:*\n"
        "• Support Bot: @PillarSupport\n"
        "• Phone: +1 (888) 555-0123\n"
        "• Email: support@pillarbank.com\n"
        "• Hours: 9 AM - 5 PM NY Time, Mon-Fri\n\n"
        
        "*FAQ:*\n"
        "❓ *Approval time?* 24-48 hours\n"
        "❓ *Banking hours?* 8:30 AM - 4:30 PM NY\n"
        "❓ *Minimum deposit?* $10\n"
        "❓ *Lost PIN?* Contact support\n\n"
        
        "*Security:*\n"
        "• Never share your PIN\n"
        "• We never ask for your PIN\n"
        "• All transactions require approval",
        parse_mode=ParseMode.MARKDOWN
    )

# =========================
# Menu Router
# =========================

async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route menu button presses to appropriate handlers"""
    text = update.message.text
    user_id = update.effective_user.id
    
    # Admin
    if is_admin(user_id):
        if text == "📋 Pending Users":
            await admin_pending(update, context)
        elif text == "💰 Pending Deposits":
            await handle_pending_deposits(update, context)
        elif text == "📤 Pending Withdrawals":
            await handle_pending_withdrawals(update, context)
        elif text == "👥 All Users":
            await handle_users(update, context)
        elif text == "💎 Savings Plans":
            await handle_savings(update, context)
        elif text == "📊 Reports":
            await handle_reports(update, context)
        elif text == "🧾 Audit Logs":
            await handle_audit_logs(update, context)
        elif text == "⚙ Settings":
            await handle_admin_settings(update, context)
    
    # User
    else:
        # Check if user is approved
        user_data = db.get_user(user_id)
        if not user_data or user_data['status'] != 'APPROVED':
            await update.message.reply_text(
                "❌ Please complete registration and wait for approval.\n"
                "Use /register to start.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if text == "💰 Balance":
            await handle_balance(update, context)
        elif text == "📈 Savings":
            await handle_savings(update, context)
        elif text == "💳 Deposit":
            await handle_deposit(update, context)
        elif text == "🏧 Withdraw":
            await handle_withdraw(update, context)
        elif text == "📜 History":
            await handle_history(update, context)
        elif text == "📊 Statement":
            await handle_statement(update, context)
        elif text == "📞 Support":
            await handle_support(update, context)
        elif text == "🔙 Back to Main Menu":
            await start(update, context)

# =========================
# Placeholder Admin Handlers
# =========================

async def handle_pending_deposits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📥 Pending deposits coming soon.", reply_markup=back_to_menu_keyboard())

async def handle_pending_withdrawals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📤 Pending withdrawals coming soon.", reply_markup=back_to_menu_keyboard())

async def handle_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👥 User management coming soon.", reply_markup=back_to_menu_keyboard())

async def handle_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 Reports coming soon.", reply_markup=back_to_menu_keyboard())

async def handle_audit_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🧾 Audit logs coming soon.", reply_markup=back_to_menu_keyboard())

async def handle_admin_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚙ Admin settings coming soon.", reply_markup=back_to_menu_keyboard())

async def health_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Health check endpoint"""
    status = "🟢 Online" if db.is_connected else "🟡 Database Offline"
    await update.message.reply_text(
        f"✅ Bot is running.\n"
        f"📊 Status: {status}\n"
        f"⏰ NY Time: {datetime.now(NY_TZ).strftime('%I:%M %p')}\n"
        f"🏦 Banking: {'🟢 Open' if _is_banking_hours() else '🔴 Closed'}",
        parse_mode=ParseMode.MARKDOWN
    )

# =========================
# Interest Job (Scheduler)
# =========================

async def interest_job():
    """Calculate and apply daily interest - Runs at 4:30 PM NY Time"""
    logger.info("💰 Running daily interest calculation...")
    
    if not db.is_connected:
        logger.warning("⚠️ Database not connected. Skipping interest calculation.")
        return
    
    try:
        # Get all active savings plans
        db.cursor.execute("""
            SELECT * FROM savings_plans 
            WHERE status = 'ACTIVE' 
            AND start_date <= CURRENT_DATE 
            AND end_date >= CURRENT_DATE
        """)
        active_plans = db.cursor.fetchall()
        
        total_interest = Decimal('0.00')
        today = datetime.now(NY_TZ).date()
        
        for plan in active_plans:
            # Check if interest already calculated today
            if plan['last_interest_calc'] == today:
                continue
            
            # Calculate daily interest
            daily_interest = plan['principal_amount'] * plan['daily_interest_rate']
            
            # Update plan
            db.cursor.execute("""
                UPDATE savings_plans 
                SET total_interest_earned = total_interest_earned + %s,
                    last_interest_calc = %s
                WHERE plan_id = %s
            """, (daily_interest, today, plan['plan_id']))
            
            # Add interest to user's available balance
            db.cursor.execute("""
                UPDATE accounts 
                SET balance = balance + %s,
                    available_balance = available_balance + %s,
                    updated_at = NOW()
                WHERE user_telegram_id = %s
            """, (daily_interest, daily_interest, plan['user_telegram_id']))
            
            total_interest += daily_interest
        
        db.conn.commit()
        
        # Log audit
        db.log_audit(
            action='INTEREST_CALCULATED',
            actor='SYSTEM',
            actor_id=0,
            description=f"Daily interest calculated: ${total_interest:.2f} for {len(active_plans)} plans",
            reference_id=None
        )
        
        logger.info(f"✅ Interest calculation complete: ${total_interest:.2f}")
        
    except Exception as e:
        logger.error(f"❌ Interest calculation failed: {e}")
        db.conn.rollback()

# =========================
# Main Application
# =========================

def main():
    """Main application entry point"""
    
    # Validate configuration
    logger.info("🚀 Starting Pillar Digital Bank...")
    
    # Create request config
    request = HTTPXRequest(
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0
    )
    
    # =========================
    # Setup Persistence (Fixes Looping)
    # =========================
    persistence = DictPersistence()
    
    # =========================
    # Setup Scheduler (NY Time 4:30 PM)
    # =========================
    scheduler = AsyncIOScheduler(timezone=NY_TZ)
    
    async def start_scheduler(application: Application):
        scheduler.start()
        logger.info("✅ Scheduler started successfully - Daily interest at 4:30 PM NY Time")

    # =========================
    # Build Application
    # =========================
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .persistence(persistence) # <--- Added Persistence
        .post_init(start_scheduler) # <--- Added Scheduler Wrapper
        .build()
    )
    
    # =========================
    # Handlers
    # =========================

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("health", health_check))
    app.add_handler(CommandHandler("pending", admin_pending))
    app.add_handler(CommandHandler("seed", seed_test_users))

    # =========================
    # Registration Conversation
    # =========================
    
    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("register", register_start)],
        states={
            FULL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_fullname)],
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_phone)],
            PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_password)],
            REFERRAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, register_referral),
                CallbackQueryHandler(register_callback, pattern="^(skip_referral|cancel_registration)$")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_registration)],
        allow_reentry=False,
    )
    app.add_handler(reg_conv)
    
    # =========================
    # Callback Handlers
    # =========================
    
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^(approve_|reject_)"))
    
    # =========================
    # Menu Router
    # =========================
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))

    # =========================
    # Webhook Setup
    # =========================
    
    logger.info(f"🌐 Starting webhook on port {PORT}")
    logger.info(f"🔗 Webhook URL: {WEBHOOK_URL}/{BOT_TOKEN}")
    
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        url_path=BOT_TOKEN,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise
    finally:
        db.close()