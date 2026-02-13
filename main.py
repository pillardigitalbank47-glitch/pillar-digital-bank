#!/usr/bin/env python3
"""
Pillar Digital Bank - Complete Production Ready Bot
Telegram Digital Savings Platform
Email Verification + Admin Approval System
"""

import logging
import os
import secrets
import hashlib
import re
import random
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any, List, Tuple

import pytz
import aiosmtplib
from email.message import EmailMessage
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
    DictPersistence
)

# =========================
# Configuration
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", "10000"))

# =========================
# Email Configuration (Gmail SMTP)
# =========================
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Validation
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN environment variable is required")
if not WEBHOOK_URL:
    raise RuntimeError("❌ WEBHOOK_URL environment variable is required")
if ADMIN_ID == 0:
    raise RuntimeError("❌ ADMIN_ID environment variable is required")
if not SENDER_EMAIL or not EMAIL_APP_PASSWORD:
    raise RuntimeError("❌ Email credentials (SENDER_EMAIL, EMAIL_APP_PASSWORD) are required")

# =========================
# Constants
# =========================

NY_TZ = pytz.timezone("America/New_York")
BANKING_HOURS = {
    "open": "08:30",
    "close": "16:30"
}
INTEREST_TIME = "16:30"

# Registration States (Updated with Email + OTP)
(FULL_NAME, PHONE, PIN, EMAIL, OTP, REFERRAL) = range(6)

# OTP Settings
OTP_EXPIRY_MINUTES = 10
OTP_LENGTH = 6

# =========================
# Logging Setup
# =========================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# Database Manager (WITHOUT DROP TABLE)
# =========================

class DatabaseManager:
    """Manages PostgreSQL database connection and operations"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.is_connected = False
        self._connect()
        self._init_tables()
        self._add_email_columns()  # Add email columns safely

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
        """Create necessary tables if they don't exist (NO DROP)"""
        if not self.is_connected:
            return

        try:
            # Users table - Create if not exists
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    full_name VARCHAR(255) NOT NULL,
                    phone_number VARCHAR(20) NOT NULL,
                    pin_hash VARCHAR(255) NOT NULL,
                    email VARCHAR(255),
                    otp_code VARCHAR(6),
                    otp_expiry TIMESTAMP WITH TIME ZONE,
                    is_email_verified BOOLEAN DEFAULT FALSE,
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
            logger.info("✅ Database tables initialized (preserved existing data)")
        except Exception as e:
            logger.error(f"❌ Table creation failed: {e}")
            self.conn.rollback()

    def _add_email_columns(self):
        """Safely add email-related columns if they don't exist"""
        if not self.is_connected:
            return

        try:
            # Check and add email column
            self.cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='users' AND column_name='email'
                    ) THEN
                        ALTER TABLE users ADD COLUMN email VARCHAR(255);
                    END IF;
                END $$;
            """)

            # Add otp_code column
            self.cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='users' AND column_name='otp_code'
                    ) THEN
                        ALTER TABLE users ADD COLUMN otp_code VARCHAR(6);
                    END IF;
                END $$;
            """)

            # Add otp_expiry column
            self.cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='users' AND column_name='otp_expiry'
                    ) THEN
                        ALTER TABLE users ADD COLUMN otp_expiry TIMESTAMP WITH TIME ZONE;
                    END IF;
                END $$;
            """)

            # Add is_email_verified column
            self.cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='users' AND column_name='is_email_verified'
                    ) THEN
                        ALTER TABLE users ADD COLUMN is_email_verified BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
            """)

            self.conn.commit()
            logger.info("✅ Email columns added successfully (existing data preserved)")
        except Exception as e:
            logger.error(f"❌ Failed to add email columns: {e}")
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

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        if not self.is_connected:
            return None
        try:
            self.cursor.execute(
                "SELECT * FROM users WHERE email = %s",
                (email,)
            )
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting user by email: {e}")
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
                   pin_hash: str, email: str, referred_by: Optional[str] = None) -> bool:
        """Register a new user (without OTP yet)"""
        if not self.is_connected:
            return False
        try:
            # Generate unique referral code
            ref_code = f"REF{secrets.token_hex(4).upper()}"
            
            self.cursor.execute("""
                INSERT INTO users 
                (telegram_id, full_name, phone_number, pin_hash, email, 
                 referral_code, referred_by, status, is_email_verified)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING', FALSE)
            """, (telegram_id, full_name, phone, pin_hash, email, ref_code, referred_by))
            
            # Create account for user
            self.cursor.execute("""
                INSERT INTO accounts (user_telegram_id, balance, available_balance)
                VALUES (%s, 0.00, 0.00)
            """, (telegram_id,))
            
            self.conn.commit()
            logger.info(f"✅ User {telegram_id} created successfully (pending email verification)")
            return True
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            self.conn.rollback()
            return False

    def save_otp(self, telegram_id: int, otp_code: str) -> bool:
        """Save OTP code for user"""
        if not self.is_connected:
            return False
        try:
            expiry = datetime.now() + timedelta(minutes=OTP_EXPIRY_MINUTES)
            self.cursor.execute("""
                UPDATE users 
                SET otp_code = %s, otp_expiry = %s, updated_at = NOW()
                WHERE telegram_id = %s
            """, (otp_code, expiry, telegram_id))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error saving OTP: {e}")
            self.conn.rollback()
            return False

    def verify_otp(self, telegram_id: int, otp_code: str) -> Tuple[bool, str]:
        """Verify OTP code"""
        if not self.is_connected:
            return False, "Database not connected"
        try:
            self.cursor.execute("""
                SELECT otp_code, otp_expiry FROM users 
                WHERE telegram_id = %s
            """, (telegram_id,))
            user = self.cursor.fetchone()
            
            if not user:
                return False, "User not found"
            
            if not user['otp_code'] or not user['otp_expiry']:
                return False, "No OTP found. Please request a new one."
            
            if user['otp_code'] != otp_code:
                return False, "Invalid OTP code"
            
            if datetime.now() > user['otp_expiry']:
                return False, "OTP has expired. Please request a new one."
            
            # Mark email as verified
            self.cursor.execute("""
                UPDATE users 
                SET is_email_verified = TRUE, 
                    otp_code = NULL, 
                    otp_expiry = NULL,
                    updated_at = NOW()
                WHERE telegram_id = %s
            """, (telegram_id,))
            self.conn.commit()
            
            return True, "Email verified successfully"
            
        except Exception as e:
            logger.error(f"Error verifying OTP: {e}")
            self.conn.rollback()
            return False, f"Error: {str(e)}"

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
        """Get all pending users (email verified)"""
        if not self.is_connected:
            return []
        try:
            self.cursor.execute("""
                SELECT * FROM users 
                WHERE status = 'PENDING' 
                AND is_email_verified = TRUE 
                ORDER BY created_at DESC
            """)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting pending users: {e}")
            return []

    def get_unverified_users(self) -> List[Dict[str, Any]]:
        """Get users who haven't verified email"""
        if not self.is_connected:
            return []
        try:
            self.cursor.execute("""
                SELECT * FROM users 
                WHERE is_email_verified = FALSE 
                ORDER BY created_at DESC
            """)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting unverified users: {e}")
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

    def add_registration_bonus(self, telegram_id: int) -> bool:
        """Add $5 registration bonus"""
        return self.update_balance(telegram_id, Decimal('5.00'), is_deposit=True)

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
# Email Service (Async)
# =========================

class EmailService:
    """Async email service for OTP verification"""
    
    @staticmethod
    async def send_otp_email(recipient_email: str, otp_code: str, full_name: str) -> Tuple[bool, str]:
        """Send OTP verification email asynchronously"""
        try:
            message = EmailMessage()
            message["From"] = SENDER_EMAIL
            message["To"] = recipient_email
            message["Subject"] = "🔐 Pillar Digital Bank - Email Verification"
            
            html_content = f"""
            <html>
                <body style="font-family: Arial, sans-serif; padding: 20px; color: #333;">
                    <div style="max-width: 600px; margin: 0 auto; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px; color: white;">
                        <h1 style="text-align: center; margin-bottom: 20px;">🏦 Pillar Digital Bank</h1>
                        <h2 style="text-align: center;">Email Verification</h2>
                        <div style="background: white; padding: 30px; border-radius: 10px; color: #333;">
                            <p style="font-size: 16px;">Hello <strong>{full_name}</strong>,</p>
                            <p style="font-size: 16px;">Thank you for registering with Pillar Digital Bank. Please use the following verification code to complete your registration:</p>
                            <div style="text-align: center; margin: 30px 0;">
                                <span style="font-size: 36px; font-weight: bold; letter-spacing: 10px; color: #667eea;">{otp_code}</span>
                            </div>
                            <p style="font-size: 14px; color: #666;">This code will expire in <strong>{OTP_EXPIRY_MINUTES} minutes</strong>.</p>
                            <p style="font-size: 14px; color: #666;">If you didn't request this, please ignore this email.</p>
                            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                            <p style="font-size: 12px; color: #999; text-align: center;">
                                © 2026 Pillar Digital Bank. All rights reserved.<br>
                                This is an automated message, please do not reply.
                            </p>
                        </div>
                    </div>
                </body>
            </html>
            """
            
            message.set_content(f"Your OTP code is: {otp_code}. Valid for {OTP_EXPIRY_MINUTES} minutes.")
            message.add_alternative(html_content, subtype="html")
            
            await aiosmtplib.send(
                message,
                hostname=SMTP_SERVER,
                port=SMTP_PORT,
                start_tls=True,
                username=SENDER_EMAIL,
                password=EMAIL_APP_PASSWORD
            )
            
            logger.info(f"✅ OTP email sent to {recipient_email}")
            return True, "Email sent successfully"
            
        except Exception as e:
            logger.error(f"❌ Failed to send email: {e}")
            return False, f"Failed to send email: {str(e)}"

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
        patterns = [
            r'^\+1\d{10}$',
            r'^\(\d{3}\)\s?\d{3}-\d{4}$',
            r'^\d{3}-\d{3}-\d{4}$',
            r'^\d{10}$',
            r'^1\d{10}$'
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
    def mask_email(email: str) -> str:
        """Mask email for display"""
        if not email:
            return "N/A"
        parts = email.split('@')
        if len(parts) != 2:
            return email
        name = parts[0]
        domain = parts[1]
        if len(name) <= 2:
            masked_name = name[0] + '*' * len(name[1:])
        else:
            masked_name = name[:2] + '*' * (len(name) - 2)
        return f"{masked_name}@{domain}"
    
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
        if not re.match(r'^[a-zA-Z0-9]+$', pin):
            return False, "PIN can only contain letters and numbers"
        return True, ""
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def generate_otp() -> str:
        """Generate 6-digit numeric OTP"""
        return ''.join([str(random.randint(0, 9)) for _ in range(OTP_LENGTH)])

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
# Admin Notification (with Inline Keyboard)
# =========================

async def notify_admin_new_registration(bot, user_id: int, full_name: str, 
                                       phone: str, email: str, username: str):
    """Send new registration notification to admin with inline buttons"""
    
    masked_phone = SecurityUtils.mask_phone(phone)
    masked_email = SecurityUtils.mask_email(email)
    
    # Create inline keyboard for quick actions
    keyboard = [
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"approve_{user_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"reject_{user_id}")
        ],
        [
            InlineKeyboardButton("👤 View Details", callback_data=f"view_{user_id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    reg_time = datetime.now(NY_TZ).strftime("%Y-%m-%d %I:%M %p")
    
    message = (
        "🆕 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     NEW REGISTRATION\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        f"👤 *Name:* `{full_name}`\n"
        f"🆔 *ID:* `{user_id}`\n"
        f"📱 *Phone:* `{masked_phone}`\n"
        f"📧 *Email:* `{masked_email}`\n"
        f"👤 *Username:* @{username}\n"
        f"⏰ *Time:* {reg_time} NY\n"
        f"📊 *Status:* ⏳ Pending Email Verified\n\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n"
        "Select action below:"
    )
    
    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=message,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        logger.info(f"✅ Admin notification sent for user {user_id}")
    except Exception as e:
        logger.error(f"❌ Failed to send admin notification: {e}")

# =========================
# Start Handler
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    user_id = user.id
    
    # Admin
    if is_admin(user_id):
        pending_users = db.get_pending_users()
        pending_count = len(pending_users)
        
        await update.message.reply_text(
            f"👑 *━━━━━━━━━━━━━━━━━━━━*\n"
            f"     ADMIN CONTROL PANEL\n"
            f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
            f"Welcome back, *Administrator*.\n\n"
            f"📊 *Quick Stats:*\n"
            f"• ⏳ Pending: `{pending_count}` users\n"
            f"• 🕐 NY Time: `{datetime.now(NY_TZ).strftime('%I:%M %p')}`\n"
            f"• 🏦 Banking: `{'🟢 Open' if _is_banking_hours() else '🔴 Closed'}`\n\n"
            f"*Available Commands:*\n"
            f"• /pending - Review new registrations\n"
            f"• /unverified - View unverified emails\n"
            f"• /dashboard - Full admin dashboard\n\n"
            f"{'🆕 ' + str(pending_count) + ' new registration' + ('s' if pending_count != 1 else '') + ' waiting!' if pending_count > 0 else ''}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_main_menu()
        )
        return
    
    # Regular user
    user_data = db.get_user(user_id)
    
    if not user_data:
        # New user
        await update.message.reply_text(
            "👋 *━━━━━━━━━━━━━━━━━━━━*\n"
            "  WELCOME TO PILLAR BANK\n"
            "*━━━━━━━━━━━━━━━━━━━━*\n\n"
            "Secure, simple, and smart savings starts here.\n\n"
            "📝 *Registration Steps:*\n"
            "1️⃣ Full Name\n"
            "2️⃣ Phone Number\n"
            "3️⃣ Transaction PIN\n"
            "4️⃣ Email Address\n"
            "5️⃣ OTP Verification\n"
            "6️⃣ Admin Approval\n\n"
            "✅ Use /register to begin.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=ReplyKeyboardMarkup(
                [["/register"]], 
                resize_keyboard=True, 
                one_time_keyboard=True
            )
        )
    elif not user_data.get('is_email_verified', False):
        # Email not verified
        await update.message.reply_text(
            "📧 *Email Verification Required*\n\n"
            "Please check your email for the OTP code.\n\n"
            "• Use `/verify <OTP>` to verify your email\n"
            "• Use `/resend` to request a new OTP\n\n"
            "⏳ OTP expires in 10 minutes.",
            parse_mode=ParseMode.MARKDOWN
        )
    elif user_data['status'] == 'PENDING':
        # Pending approval
        await update.message.reply_text(
            "⏳ *━━━━━━━━━━━━━━━━━━━━*\n"
            "     PENDING APPROVAL\n"
            "*━━━━━━━━━━━━━━━━━━━━*\n\n"
            f"✅ Email Verified\n"
            f"📅 Registered: `{user_data['created_at'].strftime('%B %d, %Y')}`\n"
            f"🆔 Account ID: `{user_id}`\n\n"
            "Admin will review your application within 24-48 hours.\n"
            "📞 Contact @PillarSupport for urgent matters.",
            parse_mode=ParseMode.MARKDOWN
        )
    elif user_data['status'] == 'APPROVED':
        # Approved user
        account = db.get_account(user_id)
        balance = account['balance'] if account else 0
        available = account['available_balance'] if account else 0
        
        await update.message.reply_text(
            f"🏦 *━━━━━━━━━━━━━━━━━━━━*\n"
            f"  WELCOME BACK!\n"
            f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
            f"👤 *{user_data['full_name']}*\n\n"
            f"💰 *Balance:* `${balance:.2f}`\n"
            f"💳 *Available:* `${available:.2f}`\n\n"
            f"📊 *Today:* {datetime.now(NY_TZ).strftime('%B %d, %Y')}\n"
            f"• NY Time: `{datetime.now(NY_TZ).strftime('%I:%M %p')}`\n"
            f"• Banking: `{'🟢 Open' if _is_banking_hours() else '🔴 Closed'}`\n\n"
            f"Select an option below:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=user_main_menu()
        )
    elif user_data['status'] == 'REJECTED':
        # Rejected user
        await update.message.reply_text(
            "❌ *━━━━━━━━━━━━━━━━━━━━*\n"
            "     REGISTRATION DECLINED\n"
            "*━━━━━━━━━━━━━━━━━━━━*\n\n"
            "Your account registration has been rejected.\n\n"
            "Please contact support for assistance:\n"
            "📞 @PillarSupport\n\n"
            f"Include your Telegram ID: `{user_id}`",
            parse_mode=ParseMode.MARKDOWN
        )

# =========================
# Registration Handlers (Updated with Email + OTP)
# =========================

async def register_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start registration process"""
    user_id = update.effective_user.id
    
    # Check if already registered
    user = db.get_user(user_id)
    if user:
        if user.get('is_email_verified', False):
            await update.message.reply_text(
                "⚠️ You already have an account.\n"
                "Use /start to access your account.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                "⚠️ You have an incomplete registration.\n"
                "Please verify your email using /verify <OTP> or /resend.",
                parse_mode=ParseMode.MARKDOWN
            )
        return ConversationHandler.END
    
    await update.message.reply_text(
        "📝 *━━━━━━━━━━━━━━━━━━━━*\n"
        "  REGISTRATION STEP 1/5\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "Please enter your *full legal name*:\n\n"
        "• Example: `John Smith`\n"
        "• Minimum 2 characters\n"
        "• Use your official name\n\n"
        "Type /cancel to cancel.",
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
        "📞 *━━━━━━━━━━━━━━━━━━━━*\n"
        "  REGISTRATION STEP 2/5\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "Please enter your *phone number*:\n\n"
        "*Accepted formats:*\n"
        "• `+14155552671`\n"
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
        "🔐 *━━━━━━━━━━━━━━━━━━━━*\n"
        "  REGISTRATION STEP 3/5\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "Create a *secure transaction PIN*:\n"
        "• 6-20 characters\n"
        "• Include at least 1 number\n"
        "• Letters and numbers only\n"
        "• Used for deposits/withdrawals\n\n"
        "⚠️ *Store this PIN safely!*\n"
        "❌ We cannot recover it for you.\n\n"
        "Enter your PIN:",
        parse_mode=ParseMode.MARKDOWN
    )
    return PIN

async def register_pin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process PIN"""
    pin = update.message.text.strip()
    
    is_valid, error = SecurityUtils.validate_pin(pin)
    if not is_valid:
        await update.message.reply_text(
            f"❌ {error}\n\n"
            "Please try again:",
            parse_mode=ParseMode.MARKDOWN
        )
        return PIN
    
    hashed_pin = SecurityUtils.hash_pin(pin)
    context.user_data['pin_hash'] = hashed_pin
    
    await update.message.reply_text(
        "📧 *━━━━━━━━━━━━━━━━━━━━*\n"
        "  REGISTRATION STEP 4/5\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "Please enter your *email address*:\n\n"
        "• Example: `john.smith@gmail.com`\n"
        "• You'll receive a 6-digit OTP code\n"
        "• Valid for 10 minutes\n\n"
        "Type /cancel to cancel.",
        parse_mode=ParseMode.MARKDOWN
    )
    return EMAIL

async def register_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process email and send OTP"""
    email = update.message.text.strip().lower()
    user_id = update.effective_user.id
    
    if not SecurityUtils.validate_email(email):
        await update.message.reply_text(
            "❌ *Invalid email format.*\n\n"
            "Please enter a valid email address:\n"
            "Example: `john.smith@gmail.com`",
            parse_mode=ParseMode.MARKDOWN
        )
        return EMAIL
    
    # Check if email already exists
    existing_user = db.get_user_by_email(email)
    if existing_user:
        await update.message.reply_text(
            "❌ This email is already registered.\n"
            "Please use a different email address.",
            parse_mode=ParseMode.MARKDOWN
        )
        return EMAIL
    
    # Generate OTP
    otp = SecurityUtils.generate_otp()
    
    # Create user first (without OTP verification)
    full_name = context.user_data.get('full_name')
    phone = context.user_data.get('phone')
    pin_hash = context.user_data.get('pin_hash')
    
    success = db.create_user(
        telegram_id=user_id,
        full_name=full_name,
        phone=phone,
        pin_hash=pin_hash,
        email=email
    )
    
    if not success:
        await update.message.reply_text(
            "❌ Failed to create account. Please try again.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationHandler.END
    
    # Save OTP to database
    db.save_otp(user_id, otp)
    context.user_data['email'] = email
    
    # Send OTP email
    await update.message.reply_text(
        "📧 *Sending verification email...*",
        parse_mode=ParseMode.MARKDOWN
    )
    
    success, message = await EmailService.send_otp_email(email, otp, full_name)
    
    if success:
        await update.message.reply_text(
            "✅ *━━━━━━━━━━━━━━━━━━━━*\n"
            "  REGISTRATION STEP 5/5\n"
            "*━━━━━━━━━━━━━━━━━━━━*\n\n"
            f"📧 Email: `{SecurityUtils.mask_email(email)}`\n\n"
            "A 6-digit OTP code has been sent to your email.\n\n"
            "Please enter the code below:\n\n"
            "• `/verify <code>` - Verify your email\n"
            "• `/resend` - Request new code\n\n"
            "⏳ Code expires in 10 minutes.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            f"❌ Failed to send email: {message}\n\n"
            "Please try again with a different email address.",
            parse_mode=ParseMode.MARKDOWN
        )
        return EMAIL
    
    return ConversationHandler.END

async def verify_otp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /verify command for OTP"""
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text(
            "❌ Please provide OTP code.\n"
            "Usage: `/verify 123456`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    otp = context.args[0].strip()
    
    if not otp.isdigit() or len(otp) != OTP_LENGTH:
        await update.message.reply_text(
            f"❌ Invalid OTP format.\n"
            f"Please enter {OTP_LENGTH}-digit numeric code.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    success, message = db.verify_otp(user_id, otp)
    
    if success:
        # Get user data for admin notification
        user = db.get_user(user_id)
        
        # Notify admin with inline keyboard
        await notify_admin_new_registration(
            context.bot,
            user_id,
            user['full_name'],
            user['phone_number'],
            user['email'],
            update.effective_user.username or "No username"
        )
        
        await update.message.reply_text(
            "✅ *━━━━━━━━━━━━━━━━━━━━*\n"
            "  EMAIL VERIFIED!\n"
            "*━━━━━━━━━━━━━━━━━━━━*\n\n"
            "Your email has been successfully verified.\n\n"
            "📋 *Next Steps:*\n"
            "1️⃣ Admin will review your application\n"
            "2️⃣ You'll be notified within 24-48 hours\n"
            "3️⃣ Once approved, you can start saving\n\n"
            "📞 *Support:* @PillarSupport\n\n"
            "Thank you for choosing Pillar Digital Bank!",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            f"❌ {message}",
            parse_mode=ParseMode.MARKDOWN
        )

async def resend_otp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /resend command for OTP"""
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    if not user:
        await update.message.reply_text(
            "❌ No registration found. Please use /register.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    if user.get('is_email_verified', False):
        await update.message.reply_text(
            "✅ Your email is already verified.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Generate new OTP
    otp = SecurityUtils.generate_otp()
    db.save_otp(user_id, otp)
    
    # Send new OTP email
    success, message = await EmailService.send_otp_email(
        user['email'], 
        otp, 
        user['full_name']
    )
    
    if success:
        await update.message.reply_text(
            "✅ New OTP code has been sent to your email.\n"
            f"Valid for {OTP_EXPIRY_MINUTES} minutes.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            f"❌ Failed to send email: {message}",
            parse_mode=ParseMode.MARKDOWN
        )

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
# Admin Handlers (Updated with Inline Keyboard)
# =========================

async def admin_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show pending registrations"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    
    pending_users = db.get_pending_users()
    
    if not pending_users:
        await update.message.reply_text(
            "✅ *No Pending Registrations*\n\n"
            "All caught up! Check back later.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    text = f"⏳ *━━━━━━━━━━━━━━━━━━━━*\n"
    text += f"     PENDING REGISTRATIONS ({len(pending_users)})\n"
    text += f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
    
    keyboard = []
    
    for user in pending_users[:5]:  # Show first 5
        masked_phone = SecurityUtils.mask_phone(user['phone_number'])
        masked_email = SecurityUtils.mask_email(user['email'])
        
        text += f"👤 *{user['full_name']}*\n"
        text += f"🆔 `{user['telegram_id']}`\n"
        text += f"📱 {masked_phone}\n"
        text += f"📧 {masked_email}\n"
        text += f"📅 {user['created_at'].strftime('%Y-%m-%d %H:%M')}\n"
        text += "━━━━━━━━━━━━━━\n"
        
        keyboard.append([
            InlineKeyboardButton(
                f"✅ Approve {user['full_name'][:10]}", 
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

async def admin_unverified(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show unverified email users"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    
    unverified = db.get_unverified_users()
    
    if not unverified:
        await update.message.reply_text(
            "✅ *No Unverified Users*\n\n"
            "All emails are verified.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    text = f"📧 *━━━━━━━━━━━━━━━━━━━━*\n"
    text += f"     UNVERIFIED EMAILS ({len(unverified)})\n"
    text += f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
    
    for user in unverified[:5]:
        text += f"👤 *{user['full_name']}*\n"
        text += f"🆔 `{user['telegram_id']}`\n"
        text += f"📧 {user['email']}\n"
        text += f"📅 {user['created_at'].strftime('%Y-%m-%d %H:%M')}\n"
        text += "━━━━━━━━━━━━━━\n"
    
    if len(unverified) > 5:
        text += f"... and {len(unverified) - 5} more\n"
    
    await update.message.reply_text(
        text,
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
        user = db.get_user(user_id)
        
        if not user:
            await query.edit_message_text(f"❌ User {user_id} not found.")
            return
        
        # Update user status
        if db.update_user_status(user_id, 'APPROVED'):
            # Add $5 registration bonus
            db.add_registration_bonus(user_id)
            
            # Log audit
            db.log_audit(
                action='USER_APPROVED',
                actor='ADMIN',
                actor_id=ADMIN_ID,
                description=f"User {user_id} ({user['full_name']}) approved with $5 bonus",
                reference_id=user_id
            )
            
            # Notify user
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "✅ *━━━━━━━━━━━━━━━━━━━━*\n"
                        "  ACCOUNT APPROVED!\n"
                        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
                        f"👤 Welcome, *{user['full_name']}*!\n\n"
                        f"💰 $5.00 registration bonus has been added to your account.\n\n"
                        f"📋 *Next Steps:*\n"
                        f"• Use /start to access your account\n"
                        f"• Check your balance with /balance\n"
                        f"• Start saving with /savings\n\n"
                        f"Thank you for choosing Pillar Digital Bank!"
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"❌ Failed to notify user {user_id}: {e}")
            
            await query.edit_message_text(
                f"✅ *User Approved*\n\n"
                f"👤 Name: {user['full_name']}\n"
                f"🆔 ID: `{user_id}`\n"
                f"📧 Email: {SecurityUtils.mask_email(user['email'])}\n"
                f"💰 $5.00 bonus added\n\n"
                f"User has been notified.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.edit_message_text(f"❌ Failed to approve user {user_id}")
    
    elif data.startswith("reject_"):
        user_id = int(data.replace("reject_", ""))
        user = db.get_user(user_id)
        
        if not user:
            await query.edit_message_text(f"❌ User {user_id} not found.")
            return
        
        if db.update_user_status(user_id, 'REJECTED'):
            db.log_audit(
                action='USER_REJECTED',
                actor='ADMIN',
                actor_id=ADMIN_ID,
                description=f"User {user_id} ({user['full_name']}) rejected",
                reference_id=user_id
            )
            
            # Notify user
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "❌ *━━━━━━━━━━━━━━━━━━━━*\n"
                        "  REGISTRATION UPDATE\n"
                        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
                        f"Dear {user['full_name']},\n\n"
                        "Unfortunately, your account registration has been rejected.\n\n"
                        "*Possible reasons:*\n"
                        "• Incomplete information\n"
                        "• Unable to verify identity\n"
                        "• Duplicate account\n\n"
                        "📞 Please contact @PillarSupport for assistance.",
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"❌ Failed to notify user {user_id}: {e}")
            
            await query.edit_message_text(
                f"❌ *User Rejected*\n\n"
                f"👤 Name: {user['full_name']}\n"
                f"🆔 ID: `{user_id}`\n"
                f"📧 Email: {SecurityUtils.mask_email(user['email'])}\n\n"
                f"User has been notified.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.edit_message_text(f"❌ Failed to reject user {user_id}")
    
    elif data.startswith("view_"):
        user_id = int(data.replace("view_", ""))
        user = db.get_user(user_id)
        
        if user:
            account = db.get_account(user_id)
            balance = account['balance'] if account else 0
            
            masked_phone = SecurityUtils.mask_phone(user['phone_number'])
            masked_email = SecurityUtils.mask_email(user['email'])
            
            status_icon = {
                'PENDING': '⏳',
                'APPROVED': '✅',
                'REJECTED': '❌'
            }.get(user['status'], '❓')
            
            await query.edit_message_text(
                f"👤 *━━━━━━━━━━━━━━━━━━━━*\n"
                f"     USER PROFILE\n"
                f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
                f"*Name:* {user['full_name']}\n"
                f"*ID:* `{user['telegram_id']}`\n"
                f"*Phone:* {masked_phone}\n"
                f"*Email:* {masked_email}\n"
                f"*Status:* {status_icon} {user['status']}\n"
                f"*Email Verified:* {'✅ Yes' if user.get('is_email_verified') else '❌ No'}\n"
                f"*Balance:* `${balance:.2f}`\n"
                f"*Registered:* {user['created_at'].strftime('%Y-%m-%d %H:%M')}\n"
                f"*Referral Code:* `{user['referral_code']}`\n"
                f"*Referred By:* {user['referred_by'] or 'None'}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.edit_message_text(f"❌ User {user_id} not found.")

# =========================
# Seed Test Data (Admin Only - Updated with Email)
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
            "pin": "pass1234",
            "email": "john.smith@example.com"
        },
        {
            "name": "Sarah Johnson",
            "phone": "(212) 555-1234",
            "pin": "sarah456",
            "email": "sarah.johnson@example.com"
        },
        {
            "name": "Michael Chen",
            "phone": "305-555-6789",
            "pin": "mike7890",
            "email": "michael.chen@example.com"
        },
        {
            "name": "Emily Davis",
            "phone": "8175554321",
            "pin": "emily321",
            "email": "emily.davis@example.com"
        }
    ]
    
    created = 0
    for i, user in enumerate(test_users):
        telegram_id = int(f"1000{i+1:02d}")
        formatted_phone = SecurityUtils.format_phone(user["phone"])
        hashed_pin = SecurityUtils.hash_pin(user["pin"])
        
        if db.create_user(telegram_id, user["name"], formatted_phone, 
                         hashed_pin, user["email"]):
            # Auto-verify email for test users
            db.save_otp(telegram_id, "123456")
            db.verify_otp(telegram_id, "123456")
            created += 1
    
    await update.message.reply_text(
        f"✅ *Created {created} Test Users*\n\n"
        f"📋 *Credentials:*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 John Smith: +14155552671 / pass1234\n"
        f"📧 john.smith@example.com\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Sarah Johnson: (212) 555-1234 / sarah456\n"
        f"📧 sarah.johnson@example.com\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Michael Chen: 305-555-6789 / mike7890\n"
        f"📧 michael.chen@example.com\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Emily Davis: 8175554321 / emily321\n"
        f"📧 emily.davis@example.com\n\n"
        f"✅ All test users have email auto-verified.\n"
        f"📬 Admin notifications sent.\n"
        f"💡 Use /pending to view pending registrations.",
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
            f"💰 *━━━━━━━━━━━━━━━━━━━━*\n"
            f"     ACCOUNT BALANCE\n"
            f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
            f"*Total Balance:* `${account['balance']:.2f}`\n"
            f"*Available:* `${account['available_balance']:.2f}`\n\n"
            f"💡 *Note:* Available balance can be withdrawn.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text("❌ Account not found.")

async def handle_savings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📈 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     SAVINGS PLANS\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "Coming Soon!\n\n"
        "*Available Plans:*\n"
        "• Basic: 1 day, 1% daily\n"
        "• Silver: 7 days, 8.4% total\n"
        "• Gold: 15 days, 21% total\n"
        "• Platinum: 30 days, 48% total\n"
        "• Diamond: 90 days, 153% total",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "💳 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     DEPOSIT REQUEST\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
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
        "🏧 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     WITHDRAWAL REQUEST\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
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
        "📜 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     TRANSACTION HISTORY\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "Coming soon! This feature will show:\n"
        "• Deposits\n"
        "• Withdrawals\n"
        "• Interest earned\n"
        "• Account activity",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_statement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📊 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     ACCOUNT STATEMENT\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "Coming soon! This feature will provide:\n"
        "• 30-day transaction summary\n"
        "• Account details\n"
        "• Balance history",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📞 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     CUSTOMER SUPPORT\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
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
        f"✅ *━━━━━━━━━━━━━━━━━━━━*\n"
        f"     HEALTH CHECK\n"
        f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
        f"📊 *Status:* {status}\n"
        f"⏰ *NY Time:* {datetime.now(NY_TZ).strftime('%I:%M %p')}\n"
        f"🏦 *Banking:* {'🟢 Open' if _is_banking_hours() else '🔴 Closed'}\n"
        f"📧 *Email:* {'✅ Configured' if SENDER_EMAIL else '❌ Not Configured'}",
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
        # Check if user exists and is approved
        user_data = db.get_user(user_id)
        if not user_data:
            await update.message.reply_text(
                "❌ Please register first.\n"
                "Use /register to create an account.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if not user_data.get('is_email_verified', False):
            await update.message.reply_text(
                "📧 Please verify your email first.\n"
                "Use /verify <OTP> or /resend.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if user_data['status'] != 'APPROVED':
            await update.message.reply_text(
                "⏳ Your account is pending approval.\n"
                "Please wait for admin confirmation.",
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
# Interest Job (Scheduler)
# =========================

async def interest_job():
    """Calculate and apply daily interest - Runs at 4:30 PM NY Time"""
    logger.info("💰 Running daily interest calculation...")
    
    if not db.is_connected:
        logger.warning("⚠️ Database not connected. Skipping interest calculation.")
        return
    
    try:
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
            if plan['last_interest_calc'] == today:
                continue
            
            daily_interest = plan['principal_amount'] * plan['daily_interest_rate']
            
            db.cursor.execute("""
                UPDATE savings_plans 
                SET total_interest_earned = total_interest_earned + %s,
                    last_interest_calc = %s
                WHERE plan_id = %s
            """, (daily_interest, today, plan['plan_id']))
            
            db.cursor.execute("""
                UPDATE accounts 
                SET balance = balance + %s,
                    available_balance = available_balance + %s,
                    updated_at = NOW()
                WHERE user_telegram_id = %s
            """, (daily_interest, daily_interest, plan['user_telegram_id']))
            
            total_interest += daily_interest
        
        db.conn.commit()
        
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
    
    logger.info("🚀 Starting Pillar Digital Bank...")
    
    # Create request config
    request = HTTPXRequest(
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0
    )
    
    # Setup Persistence
    persistence = DictPersistence()
    
    # Setup Scheduler
    scheduler = AsyncIOScheduler(timezone=NY_TZ)
    
    async def start_scheduler(application: Application):
        scheduler.add_job(interest_job, 'cron', hour=16, minute=30)
        scheduler.start()
        logger.info("✅ Scheduler started - Daily interest at 4:30 PM NY Time")

    # Build Application
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .persistence(persistence)
        .post_init(start_scheduler)
        .build()
    )
    
    # =========================
    # Command Handlers
    # =========================

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("health", health_check))
    app.add_handler(CommandHandler("pending", admin_pending))
    app.add_handler(CommandHandler("unverified", admin_unverified))
    app.add_handler(CommandHandler("seed", seed_test_users))
    app.add_handler(CommandHandler("verify", verify_otp_command))
    app.add_handler(CommandHandler("resend", resend_otp_command))
    app.add_handler(CommandHandler("cancel", cancel_registration))

    # =========================
    # Registration Conversation (Updated)
    # =========================
    
    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("register", register_start)],
        states={
            FULL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_fullname)],
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_phone)],
            PIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_pin)],
            EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_email)],
        },
        fallbacks=[CommandHandler("cancel", cancel_registration)],
        allow_reentry=False,
    )
    app.add_handler(reg_conv)
    
    # =========================
    # Callback Handlers
    # =========================
    
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^(approve_|reject_|view_)"))
    
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