#!/usr/bin/env python3
"""
PILLAR DIGITAL BANK - COMPLETE TELEGRAM BOT
Professional Digital Banking Platform
Author: Pillar Digital Bank Team
Version: 1.0.0
"""

import os
import logging
import secrets
import hashlib
import re
import random
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any, List, Tuple

import pytz
import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# =========================
# CONFIGURATION
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "PillarDigitalBankCS47")

# Crypto Addresses
BTC_ADDRESS = "bc1qr4ksawcdxxnrwqv3jy7hnanqzvkzvf3jerrgja"
ETH_ADDRESS = "0x13c7acDfBc5842C311dEB2f33D98f62d02Bc4f37"
USDT_ADDRESS = "0x13c7acDfBc5842C311dEB2f33D98f62d02Bc4f37"
USDC_ADDRESS = "0x13c7acDfBc5842C311dEB2f33D98f62d02Bc4f37"

# Timezone
NY_TZ = pytz.timezone("America/New_York")

# Validation
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable is required")
if ADMIN_ID == 0:
    raise ValueError("❌ ADMIN_ID environment variable is required")

# =========================
# CONSTANTS
# =========================

# Registration States
(FULL_NAME, PHONE, EMAIL, OTP, REFERRAL) = range(5)

# Deposit States
(DEPOSIT_AMOUNT, DEPOSIT_METHOD) = range(10, 12)

# Withdrawal States
(WITHDRAW_AMOUNT, WITHDRAW_METHOD, WITHDRAW_OTP, WITHDRAW_ADDRESS) = range(20, 24)

# Savings Plan States
(SAVINGS_PLAN_SELECT, SAVINGS_AMOUNT, SAVINGS_CONFIRM) = range(30, 33)

# OTP Settings
OTP_EXPIRY_MINUTES = 10
OTP_LENGTH = 6

# Registration Bonus
REGISTRATION_BONUS = Decimal('5.00')
REFERRAL_BONUS = Decimal('1.00')

# =========================
# LOGGING
# =========================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# DATABASE MANAGER
# =========================

class DatabaseManager:
    """Complete PostgreSQL database manager"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.is_connected = False
        self._connect()
        self._init_tables()

    def _connect(self):
        """Establish database connection"""
        try:
            if DATABASE_URL:
                self.conn = psycopg2.connect(DATABASE_URL, sslmode='require')
                self.conn.autocommit = False
                self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
                self.is_connected = True
                logger.info("✅ Database connected successfully")
            else:
                logger.warning("⚠️ DATABASE_URL not found. Running without persistent storage.")
                self._init_memory_storage()
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            self._init_memory_storage()

    def _init_memory_storage(self):
        """Initialize in-memory storage for development"""
        self.users = {}
        self.accounts = {}
        self.transactions = []
        self.savings_plans = []
        self.audit_logs = []
        self.referrals = {}
        self.is_connected = False
        logger.info("📁 Using in-memory storage (development mode)")

    def _init_tables(self):
        """Create all necessary tables with complete schema"""
        if not self.is_connected:
            return

        try:
            # Users table - Complete profile
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    full_name VARCHAR(255) NOT NULL,
                    phone_number VARCHAR(20) NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    referral_code VARCHAR(20) UNIQUE,
                    referred_by BIGINT,
                    status VARCHAR(20) DEFAULT 'PENDING',
                    is_email_verified BOOLEAN DEFAULT FALSE,
                    otp_code VARCHAR(6),
                    otp_expiry TIMESTAMP WITH TIME ZONE,
                    registration_bonus_given BOOLEAN DEFAULT FALSE,
                    referral_bonus_given BOOLEAN DEFAULT FALSE,
                    last_activity TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Accounts table - Balances
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id BIGSERIAL PRIMARY KEY,
                    user_telegram_id BIGINT UNIQUE REFERENCES users(telegram_id) ON DELETE CASCADE,
                    balance DECIMAL(15,2) DEFAULT 0.00,
                    locked_balance DECIMAL(15,2) DEFAULT 0.00,
                    available_balance DECIMAL(15,2) DEFAULT 0.00,
                    total_deposits DECIMAL(15,2) DEFAULT 0.00,
                    total_withdrawals DECIMAL(15,2) DEFAULT 0.00,
                    total_interest_earned DECIMAL(15,2) DEFAULT 0.00,
                    status VARCHAR(20) DEFAULT 'ACTIVE',
                    last_interest_calc TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Savings Plans Templates
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS savings_plan_templates (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    description TEXT,
                    duration_days INTEGER NOT NULL,
                    min_amount DECIMAL(15,2) NOT NULL,
                    daily_rate DECIMAL(5,4) NOT NULL,
                    total_rate DECIMAL(5,2) NOT NULL,
                    is_locked BOOLEAN DEFAULT TRUE,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # User Savings Plans
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_savings_plans (
                    id BIGSERIAL PRIMARY KEY,
                    plan_id VARCHAR(50) UNIQUE NOT NULL,
                    user_telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE CASCADE,
                    template_id INTEGER REFERENCES savings_plan_templates(id),
                    plan_name VARCHAR(50) NOT NULL,
                    principal_amount DECIMAL(15,2) NOT NULL,
                    current_value DECIMAL(15,2) DEFAULT 0.00,
                    interest_earned DECIMAL(15,2) DEFAULT 0.00,
                    daily_rate DECIMAL(5,4) NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    last_interest_calc TIMESTAMP WITH TIME ZONE,
                    status VARCHAR(20) DEFAULT 'ACTIVE',
                    is_locked BOOLEAN DEFAULT TRUE,
                    auto_renew BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Transactions table - Complete ledger
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id BIGSERIAL PRIMARY KEY,
                    transaction_id VARCHAR(50) UNIQUE NOT NULL,
                    user_telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE CASCADE,
                    type VARCHAR(30) NOT NULL,
                    method VARCHAR(20),
                    amount DECIMAL(15,2) NOT NULL,
                    fee DECIMAL(15,2) DEFAULT 0.00,
                    net_amount DECIMAL(15,2),
                    status VARCHAR(20) DEFAULT 'PENDING',
                    crypto_address TEXT,
                    crypto_currency VARCHAR(10),
                    tx_hash VARCHAR(255),
                    user_reference TEXT,
                    reviewed_by BIGINT,
                    admin_note TEXT,
                    requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    reviewed_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE,
                    metadata JSONB
                )
            """)

            # Daily Interest Logs
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_interest_logs (
                    id BIGSERIAL PRIMARY KEY,
                    user_telegram_id BIGINT REFERENCES users(telegram_id),
                    savings_plan_id BIGINT REFERENCES user_savings_plans(id),
                    calculation_date DATE NOT NULL,
                    interest_amount DECIMAL(15,2) NOT NULL,
                    principal_amount DECIMAL(15,2) NOT NULL,
                    daily_rate DECIMAL(5,4) NOT NULL,
                    is_applied BOOLEAN DEFAULT FALSE,
                    applied_at TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Referrals table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id BIGSERIAL PRIMARY KEY,
                    referrer_id BIGINT REFERENCES users(telegram_id) ON DELETE CASCADE,
                    referred_id BIGINT UNIQUE REFERENCES users(telegram_id) ON DELETE CASCADE,
                    bonus_paid BOOLEAN DEFAULT FALSE,
                    bonus_amount DECIMAL(15,2) DEFAULT 1.00,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Audit logs - Complete audit trail
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id BIGSERIAL PRIMARY KEY,
                    action VARCHAR(50) NOT NULL,
                    actor VARCHAR(20) NOT NULL,
                    actor_id BIGINT NOT NULL,
                    target_user BIGINT,
                    reference_id BIGINT,
                    description TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    ip_address INET,
                    user_agent TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Seed savings plan templates if empty
            self.cursor.execute("SELECT COUNT(*) as count FROM savings_plan_templates")
            count = self.cursor.fetchone()['count']
            
            if count == 0:
                plans = [
                    ('Basic', '24-hour savings plan with daily interest', 1, 100.00, 0.01, 1.0, False),
                    ('Silver', '7-day savings plan with locked principal', 7, 1000.00, 0.012, 8.4, True),
                    ('Gold', '15-day premium savings plan', 15, 5000.00, 0.014, 21.0, True),
                    ('Platinum', '30-day premium savings plan', 30, 10000.00, 0.016, 48.0, True),
                    ('Diamond', '90-day premium savings plan', 90, 25000.00, 0.017, 153.0, True)
                ]
                
                for plan in plans:
                    self.cursor.execute("""
                        INSERT INTO savings_plan_templates 
                        (name, description, duration_days, min_amount, daily_rate, total_rate, is_locked)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, plan)
                
                self.conn.commit()
                logger.info("✅ Savings plan templates seeded")

            self.conn.commit()
            logger.info("✅ All database tables initialized successfully")

        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            self.conn.rollback()

    # ========== USER OPERATIONS ==========

    def get_user(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Get user by Telegram ID"""
        if not self.is_connected:
            return self.users.get(str(telegram_id))
        
        try:
            self.cursor.execute("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting user: {e}")
            return None

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        if not self.is_connected:
            for user in self.users.values():
                if user.get('email') == email:
                    return user
            return None
        
        try:
            self.cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting user by email: {e}")
            return None

    def get_user_by_referral(self, referral_code: str) -> Optional[Dict[str, Any]]:
        """Get user by referral code"""
        if not self.is_connected:
            for user in self.users.values():
                if user.get('referral_code') == referral_code:
                    return user
            return None
        
        try:
            self.cursor.execute("SELECT * FROM users WHERE referral_code = %s", (referral_code,))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting user by referral: {e}")
            return None

    def create_user(self, telegram_id: int, full_name: str, phone: str, email: str, 
                   password_hash: str, referred_by: Optional[str] = None) -> bool:
        """Create new user"""
        if not self.is_connected:
            user_id = str(telegram_id)
            if user_id in self.users:
                return False
            
            ref_code = f"REF{secrets.token_hex(4).upper()}"
            
            self.users[user_id] = {
                'telegram_id': telegram_id,
                'full_name': full_name,
                'phone_number': phone,
                'email': email,
                'password_hash': password_hash,
                'referral_code': ref_code,
                'referred_by': referred_by,
                'status': 'PENDING',
                'is_email_verified': False,
                'created_at': datetime.now()
            }
            
            # Create account
            self.accounts[user_id] = {
                'user_telegram_id': telegram_id,
                'balance': Decimal('0.00'),
                'locked_balance': Decimal('0.00'),
                'available_balance': Decimal('0.00'),
                'total_deposits': Decimal('0.00'),
                'total_withdrawals': Decimal('0.00'),
                'total_interest_earned': Decimal('0.00'),
                'status': 'ACTIVE',
                'created_at': datetime.now()
            }
            
            return True
        
        try:
            # Generate unique referral code
            ref_code = f"REF{secrets.token_hex(4).upper()}"
            
            self.cursor.execute("""
                INSERT INTO users 
                (telegram_id, full_name, phone_number, email, password_hash, 
                 referral_code, referred_by, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING', NOW(), NOW())
                RETURNING telegram_id
            """, (telegram_id, full_name, phone, email, password_hash, ref_code, referred_by))
            
            # Create account for user
            self.cursor.execute("""
                INSERT INTO accounts 
                (user_telegram_id, balance, locked_balance, available_balance, 
                 total_deposits, total_withdrawals, total_interest_earned, status, created_at, updated_at)
                VALUES (%s, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 'ACTIVE', NOW(), NOW())
            """, (telegram_id,))
            
            self.conn.commit()
            logger.info(f"✅ User {telegram_id} created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            self.conn.rollback()
            return False

    def save_otp(self, telegram_id: int, otp_code: str) -> bool:
        """Save OTP for user"""
        if not self.is_connected:
            user_id = str(telegram_id)
            if user_id in self.users:
                self.users[user_id]['otp_code'] = otp_code
                self.users[user_id]['otp_expiry'] = datetime.now() + timedelta(minutes=OTP_EXPIRY_MINUTES)
                return True
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
            user_id = str(telegram_id)
            if user_id not in self.users:
                return False, "User not found"
            
            user = self.users[user_id]
            if not user.get('otp_code'):
                return False, "No OTP found"
            
            if user['otp_code'] != otp_code:
                return False, "Invalid OTP"
            
            if datetime.now() > user.get('otp_expiry', datetime.now()):
                return False, "OTP expired"
            
            user['is_email_verified'] = True
            user['otp_code'] = None
            user['otp_expiry'] = None
            return True, "Email verified"
        
        try:
            self.cursor.execute("""
                SELECT otp_code, otp_expiry FROM users 
                WHERE telegram_id = %s
            """, (telegram_id,))
            user = self.cursor.fetchone()
            
            if not user:
                return False, "User not found"
            
            if not user['otp_code']:
                return False, "No OTP found"
            
            if user['otp_code'] != otp_code:
                return False, "Invalid OTP"
            
            if datetime.now() > user['otp_expiry']:
                return False, "OTP expired"
            
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
            return False, f"Error: {str(e)}"

    def update_user_status(self, telegram_id: int, status: str) -> bool:
        """Update user status"""
        if not self.is_connected:
            user_id = str(telegram_id)
            if user_id in self.users:
                self.users[user_id]['status'] = status
                return True
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
            return [u for u in self.users.values() if u['status'] == 'PENDING' and u.get('is_email_verified')]
        
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

    def get_all_users(self) -> List[Dict[str, Any]]:
        """Get all users (admin only)"""
        if not self.is_connected:
            return list(self.users.values())
        
        try:
            self.cursor.execute("""
                SELECT u.*, a.balance, a.available_balance, a.total_deposits, a.total_withdrawals 
                FROM users u
                LEFT JOIN accounts a ON u.telegram_id = a.user_telegram_id
                ORDER BY u.created_at DESC
            """)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return []

    # ========== ACCOUNT OPERATIONS ==========

    def get_account(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Get account by user ID"""
        if not self.is_connected:
            return self.accounts.get(str(telegram_id))
        
        try:
            self.cursor.execute("SELECT * FROM accounts WHERE user_telegram_id = %s", (telegram_id,))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting account: {e}")
            return None

    def add_registration_bonus(self, telegram_id: int) -> bool:
        """Add registration bonus to user"""
        if not self.is_connected:
            user_id = str(telegram_id)
            if user_id in self.accounts:
                self.accounts[user_id]['balance'] += REGISTRATION_BONUS
                self.accounts[user_id]['available_balance'] += REGISTRATION_BONUS
                return True
            return False
        
        try:
            self.cursor.execute("""
                UPDATE accounts 
                SET balance = balance + %s,
                    available_balance = available_balance + %s,
                    updated_at = NOW()
                WHERE user_telegram_id = %s
            """, (REGISTRATION_BONUS, REGISTRATION_BONUS, telegram_id))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error adding bonus: {e}")
            self.conn.rollback()
            return False

    def add_referral_bonus(self, referrer_id: int) -> bool:
        """Add referral bonus to referrer"""
        if not self.is_connected:
            user_id = str(referrer_id)
            if user_id in self.accounts:
                self.accounts[user_id]['balance'] += REFERRAL_BONUS
                self.accounts[user_id]['available_balance'] += REFERRAL_BONUS
                return True
            return False
        
        try:
            self.cursor.execute("""
                UPDATE accounts 
                SET balance = balance + %s,
                    available_balance = available_balance + %s,
                    updated_at = NOW()
                WHERE user_telegram_id = %s
            """, (REFERRAL_BONUS, REFERRAL_BONUS, referrer_id))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error adding referral bonus: {e}")
            self.conn.rollback()
            return False

    def update_balance(self, telegram_id: int, amount: Decimal, 
                      is_deposit: bool = True, is_locked: bool = False) -> bool:
        """Update account balance"""
        if not self.is_connected:
            user_id = str(telegram_id)
            if user_id not in self.accounts:
                return False
            
            account = self.accounts[user_id]
            if is_deposit:
                account['balance'] += amount
                account['available_balance'] += amount
                account['total_deposits'] += amount
            else:
                if account['available_balance'] >= amount:
                    account['balance'] -= amount
                    account['available_balance'] -= amount
                    account['total_withdrawals'] += amount
                else:
                    return False
            return True
        
        try:
            if is_deposit:
                self.cursor.execute("""
                    UPDATE accounts 
                    SET balance = balance + %s,
                        available_balance = available_balance + %s,
                        total_deposits = total_deposits + %s,
                        updated_at = NOW()
                    WHERE user_telegram_id = %s
                """, (amount, amount, amount, telegram_id))
            else:
                self.cursor.execute("""
                    UPDATE accounts 
                    SET balance = balance - %s,
                        available_balance = available_balance - %s,
                        total_withdrawals = total_withdrawals + %s,
                        updated_at = NOW()
                    WHERE user_telegram_id = %s 
                    AND available_balance >= %s
                """, (amount, amount, amount, telegram_id, amount))
            
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating balance: {e}")
            self.conn.rollback()
            return False

    def lock_funds(self, telegram_id: int, amount: Decimal) -> bool:
        """Lock funds for savings plan"""
        if not self.is_connected:
            user_id = str(telegram_id)
            if user_id not in self.accounts:
                return False
            
            account = self.accounts[user_id]
            if account['available_balance'] >= amount:
                account['available_balance'] -= amount
                account['locked_balance'] += amount
                return True
            return False
        
        try:
            self.cursor.execute("""
                UPDATE accounts 
                SET available_balance = available_balance - %s,
                    locked_balance = locked_balance + %s,
                    updated_at = NOW()
                WHERE user_telegram_id = %s 
                AND available_balance >= %s
            """, (amount, amount, telegram_id, amount))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error locking funds: {e}")
            self.conn.rollback()
            return False

    def unlock_funds(self, telegram_id: int, amount: Decimal) -> bool:
        """Unlock funds from savings plan"""
        if not self.is_connected:
            user_id = str(telegram_id)
            if user_id not in self.accounts:
                return False
            
            account = self.accounts[user_id]
            if account['locked_balance'] >= amount:
                account['locked_balance'] -= amount
                account['available_balance'] += amount
                return True
            return False
        
        try:
            self.cursor.execute("""
                UPDATE accounts 
                SET locked_balance = locked_balance - %s,
                    available_balance = available_balance + %s,
                    updated_at = NOW()
                WHERE user_telegram_id = %s 
                AND locked_balance >= %s
            """, (amount, amount, telegram_id, amount))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error unlocking funds: {e}")
            self.conn.rollback()
            return False

    # ========== SAVINGS PLAN OPERATIONS ==========

    def get_savings_templates(self) -> List[Dict[str, Any]]:
        """Get all active savings plan templates"""
        if not self.is_connected:
            return [
                {'id': 1, 'name': 'Basic', 'description': '24-hour savings plan', 'duration_days': 1, 
                 'min_amount': 100.00, 'daily_rate': 0.01, 'total_rate': 1.0, 'is_locked': False},
                {'id': 2, 'name': 'Silver', 'description': '7-day locked savings', 'duration_days': 7,
                 'min_amount': 1000.00, 'daily_rate': 0.012, 'total_rate': 8.4, 'is_locked': True},
                {'id': 3, 'name': 'Gold', 'description': '15-day premium', 'duration_days': 15,
                 'min_amount': 5000.00, 'daily_rate': 0.014, 'total_rate': 21.0, 'is_locked': True},
                {'id': 4, 'name': 'Platinum', 'description': '30-day premium', 'duration_days': 30,
                 'min_amount': 10000.00, 'daily_rate': 0.016, 'total_rate': 48.0, 'is_locked': True},
                {'id': 5, 'name': 'Diamond', 'description': '90-day premium', 'duration_days': 90,
                 'min_amount': 25000.00, 'daily_rate': 0.017, 'total_rate': 153.0, 'is_locked': True}
            ]
        
        try:
            self.cursor.execute("""
                SELECT * FROM savings_plan_templates 
                WHERE is_active = TRUE 
                ORDER BY min_amount
            """)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting savings templates: {e}")
            return []

    def create_savings_plan(self, telegram_id: int, template_id: int, plan_name: str,
                           principal_amount: Decimal, daily_rate: Decimal, 
                           duration_days: int, is_locked: bool) -> Optional[str]:
        """Create user savings plan"""
        plan_id = f"SP{secrets.token_hex(4).upper()}"
        start_date = datetime.now().date()
        end_date = start_date + timedelta(days=duration_days)
        
        if not self.is_connected:
            self.savings_plans.append({
                'plan_id': plan_id,
                'user_telegram_id': telegram_id,
                'template_id': template_id,
                'plan_name': plan_name,
                'principal_amount': principal_amount,
                'current_value': principal_amount,
                'interest_earned': 0,
                'daily_rate': daily_rate,
                'start_date': start_date,
                'end_date': end_date,
                'status': 'ACTIVE',
                'is_locked': is_locked
            })
            return plan_id
        
        try:
            self.cursor.execute("""
                INSERT INTO user_savings_plans 
                (plan_id, user_telegram_id, template_id, plan_name, principal_amount, 
                 current_value, daily_rate, start_date, end_date, status, is_locked, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE', %s, NOW())
                RETURNING plan_id
            """, (plan_id, telegram_id, template_id, plan_name, principal_amount,
                  principal_amount, daily_rate, start_date, end_date, is_locked))
            
            result = self.cursor.fetchone()
            self.conn.commit()
            return result['plan_id'] if result else None
            
        except Exception as e:
            logger.error(f"Error creating savings plan: {e}")
            self.conn.rollback()
            return None

    def get_user_savings_plans(self, telegram_id: int) -> List[Dict[str, Any]]:
        """Get all user's savings plans"""
        if not self.is_connected:
            return [p for p in self.savings_plans if p['user_telegram_id'] == telegram_id]
        
        try:
            self.cursor.execute("""
                SELECT * FROM user_savings_plans 
                WHERE user_telegram_id = %s 
                ORDER BY created_at DESC
            """, (telegram_id,))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting user savings plans: {e}")
            return []

    def calculate_and_add_interest(self, telegram_id: int) -> Decimal:
        """Calculate and add interest for all user's active savings plans"""
        total_interest = Decimal('0.00')
        plans = self.get_user_savings_plans(telegram_id)
        
        for plan in plans:
            if plan['status'] != 'ACTIVE':
                continue
            
            last_calc = plan.get('last_interest_calc')
            if last_calc:
                last_calc = last_calc.replace(tzinfo=NY_TZ)
            else:
                last_calc = plan['start_date']
                if isinstance(last_calc, date):
                    last_calc = datetime.combine(last_calc, datetime.min.time()).replace(tzinfo=NY_TZ)
            
            now = datetime.now(NY_TZ)
            days_diff = (now - last_calc).days
            
            if days_diff > 0:
                # Calculate interest for each day
                for day in range(days_diff):
                    if plan['start_date'] <= (last_calc + timedelta(days=day+1)).date() <= plan['end_date']:
                        daily_interest = plan['principal_amount'] * plan['daily_rate']
                        total_interest += daily_interest
                        
                        if self.is_connected:
                            self.cursor.execute("""
                                INSERT INTO daily_interest_logs 
                                (user_telegram_id, savings_plan_id, calculation_date, 
                                 interest_amount, principal_amount, daily_rate, is_applied)
                                VALUES (%s, %s, %s, %s, %s, %s, FALSE)
                            """, (telegram_id, plan['id'], 
                                  (last_calc + timedelta(days=day+1)).date(),
                                  daily_interest, plan['principal_amount'], plan['daily_rate']))
        
        if total_interest > 0 and self.is_connected:
            self.conn.commit()
        
        return total_interest

    # ========== TRANSACTION OPERATIONS ==========

    def create_transaction(self, telegram_id: int, tx_type: str, amount: Decimal,
                          method: str = None, crypto_currency: str = None,
                          crypto_address: str = None) -> Optional[str]:
        """Create new transaction"""
        tx_id = f"TX{secrets.token_hex(4).upper()}"
        
        if not self.is_connected:
            self.transactions.append({
                'transaction_id': tx_id,
                'user_telegram_id': telegram_id,
                'type': tx_type,
                'method': method,
                'amount': amount,
                'net_amount': amount,
                'status': 'PENDING',
                'crypto_currency': crypto_currency,
                'crypto_address': crypto_address,
                'requested_at': datetime.now()
            })
            return tx_id
        
        try:
            self.cursor.execute("""
                INSERT INTO transactions 
                (transaction_id, user_telegram_id, type, method, amount, net_amount,
                 crypto_currency, crypto_address, status, requested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', NOW())
                RETURNING transaction_id
            """, (tx_id, telegram_id, tx_type, method, amount, amount,
                  crypto_currency, crypto_address))
            
            result = self.cursor.fetchone()
            self.conn.commit()
            return result['transaction_id'] if result else None
            
        except Exception as e:
            logger.error(f"Error creating transaction: {e}")
            self.conn.rollback()
            return None

    def update_transaction_status(self, transaction_id: str, status: str,
                                  admin_id: int = None, note: str = None) -> bool:
        """Update transaction status"""
        if not self.is_connected:
            for tx in self.transactions:
                if tx['transaction_id'] == transaction_id:
                    tx['status'] = status
                    tx['reviewed_by'] = admin_id
                    tx['admin_note'] = note
                    tx['reviewed_at'] = datetime.now()
                    if status == 'COMPLETED':
                        tx['completed_at'] = datetime.now()
                    return True
            return False
        
        try:
            self.cursor.execute("""
                UPDATE transactions 
                SET status = %s,
                    reviewed_by = %s,
                    admin_note = %s,
                    reviewed_at = NOW(),
                    completed_at = CASE WHEN %s = 'COMPLETED' THEN NOW() ELSE completed_at END
                WHERE transaction_id = %s
            """, (status, admin_id, note, status, transaction_id))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating transaction: {e}")
            self.conn.rollback()
            return False

    def get_user_transactions(self, telegram_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get user's recent transactions"""
        if not self.is_connected:
            return [tx for tx in self.transactions if tx['user_telegram_id'] == telegram_id][:limit]
        
        try:
            self.cursor.execute("""
                SELECT * FROM transactions 
                WHERE user_telegram_id = %s 
                ORDER BY requested_at DESC 
                LIMIT %s
            """, (telegram_id, limit))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting user transactions: {e}")
            return []

    def get_pending_transactions(self, tx_type: str = None) -> List[Dict[str, Any]]:
        """Get pending transactions"""
        if not self.is_connected:
            if tx_type:
                return [tx for tx in self.transactions if tx['status'] == 'PENDING' and tx['type'] == tx_type]
            return [tx for tx in self.transactions if tx['status'] == 'PENDING']
        
        try:
            if tx_type:
                self.cursor.execute("""
                    SELECT * FROM transactions 
                    WHERE status = 'PENDING' AND type = %s 
                    ORDER BY requested_at
                """, (tx_type,))
            else:
                self.cursor.execute("""
                    SELECT * FROM transactions 
                    WHERE status = 'PENDING' 
                    ORDER BY requested_at
                """)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting pending transactions: {e}")
            return []

    # ========== REFERRAL OPERATIONS ==========

    def add_referral(self, referrer_id: int, referred_id: int) -> bool:
        """Add referral relationship"""
        if not self.is_connected:
            key = f"{referrer_id}_{referred_id}"
            self.referrals[key] = {
                'referrer_id': referrer_id,
                'referred_id': referred_id,
                'bonus_paid': False,
                'created_at': datetime.now()
            }
            return True
        
        try:
            self.cursor.execute("""
                INSERT INTO referrals (referrer_id, referred_id, created_at)
                VALUES (%s, %s, NOW())
            """, (referrer_id, referred_id))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding referral: {e}")
            self.conn.rollback()
            return False

    def process_referral_bonus(self, referred_id: int) -> bool:
        """Process referral bonus for referrer"""
        if not self.is_connected:
            for key, ref in self.referrals.items():
                if ref['referred_id'] == referred_id and not ref['bonus_paid']:
                    referrer_id = ref['referrer_id']
                    if self.add_referral_bonus(referrer_id):
                        ref['bonus_paid'] = True
                        return True
            return False
        
        try:
            # Get referrer
            self.cursor.execute("""
                SELECT referrer_id FROM referrals 
                WHERE referred_id = %s AND bonus_paid = FALSE
            """, (referred_id,))
            result = self.cursor.fetchone()
            
            if result and self.add_referral_bonus(result['referrer_id']):
                self.cursor.execute("""
                    UPDATE referrals 
                    SET bonus_paid = TRUE 
                    WHERE referred_id = %s
                """, (referred_id,))
                self.conn.commit()
                return True
            return False
        except Exception as e:
            logger.error(f"Error processing referral bonus: {e}")
            self.conn.rollback()
            return False

    # ========== AUDIT OPERATIONS ==========

    def log_audit(self, action: str, actor: str, actor_id: int, description: str,
                 target_user: int = None, reference_id: int = None,
                 old_value: str = None, new_value: str = None) -> bool:
        """Log audit entry"""
        if not self.is_connected:
            self.audit_logs.append({
                'action': action,
                'actor': actor,
                'actor_id': actor_id,
                'target_user': target_user,
                'reference_id': reference_id,
                'description': description,
                'old_value': old_value,
                'new_value': new_value,
                'timestamp': datetime.now()
            })
            return True
        
        try:
            self.cursor.execute("""
                INSERT INTO audit_logs 
                (action, actor, actor_id, target_user, reference_id, 
                 description, old_value, new_value, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (action, actor, actor_id, target_user, reference_id,
                  description, old_value, new_value))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error logging audit: {e}")
            self.conn.rollback()
            return False

    def get_audit_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent audit logs"""
        if not self.is_connected:
            return self.audit_logs[-limit:]
        
        try:
            self.cursor.execute("""
                SELECT * FROM audit_logs 
                ORDER BY timestamp DESC 
                LIMIT %s
            """, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting audit logs: {e}")
            return []

    def close(self):
        """Close database connection"""
        if self.is_connected and self.conn:
            self.cursor.close()
            self.conn.close()
            logger.info("✅ Database connection closed")

# Initialize database
db = DatabaseManager()

# =========================
# SECURITY UTILITIES
# =========================

class SecurityUtils:
    """Security and validation utilities"""
    
    @staticmethod
    def hash_password(password: str) -> str:
        """Hash password"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        """Verify password"""
        return SecurityUtils.hash_password(password) == hashed
    
    @staticmethod
    def generate_otp() -> str:
        """Generate 6-digit OTP"""
        return ''.join([str(random.randint(0, 9)) for _ in range(OTP_LENGTH)])
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def validate_phone(phone: str) -> bool:
        """Validate phone number"""
        pattern = r'^\+?[1-9]\d{1,14}$'
        return bool(re.match(pattern, phone))
    
    @staticmethod
    def validate_name(name: str) -> bool:
        """Validate full name"""
        return 2 <= len(name.strip()) <= 100
    
    @staticmethod
    def validate_amount(amount: str) -> Tuple[bool, Decimal, str]:
        """Validate monetary amount"""
        try:
            amount = Decimal(amount)
            if amount <= 0:
                return False, Decimal('0'), "Amount must be greater than 0"
            if amount > 1000000:
                return False, Decimal('0'), "Amount cannot exceed $1,000,000"
            return True, amount, "Valid"
        except:
            return False, Decimal('0'), "Invalid amount format"
    
    @staticmethod
    def mask_email(email: str) -> str:
        """Mask email for display"""
        if not email or '@' not in email:
            return email
        local, domain = email.split('@')
        if len(local) <= 2:
            masked = local[0] + '*' * (len(local) - 1)
        else:
            masked = local[:2] + '*' * (len(local) - 2)
        return f"{masked}@{domain}"
    
    @staticmethod
    def mask_phone(phone: str) -> str:
        """Mask phone for display"""
        if not phone:
            return phone
        if len(phone) <= 4:
            return '*' * len(phone)
        return phone[:3] + '*' * (len(phone) - 5) + phone[-2:]

# =========================
# EMAIL SERVICE
# =========================

class EmailService:
    """Real email service using Gmail SMTP"""
    
    @staticmethod
    async def send_otp(email: str, otp: str, name: str) -> Tuple[bool, str]:
        """Send OTP email via Gmail SMTP"""
        try:
            message = EmailMessage()
            message["From"] = SENDER_EMAIL
            message["To"] = email
            message["Subject"] = "🔐 Pillar Digital Bank - Email Verification"
            
            html = f"""
            <html>
                <body style="font-family: Arial, sans-serif; padding: 20px;">
                    <h2>Email Verification</h2>
                    <p>Hello {name},</p>
                    <p>Your verification code is:</p>
                    <h1 style="font-size: 32px; letter-spacing: 5px;">{otp}</h1>
                    <p>Valid for 10 minutes.</p>
                    <hr>
                    <p>If you didn't request this, please ignore this email.</p>
                </body>
            </html>
            """
            
            message.set_content(f"Your OTP is: {otp}")
            message.add_alternative(html, subtype="html")
            
            await aiosmtplib.send(
                message,
                hostname="smtp.gmail.com",
                port=587,
                start_tls=True,
                username=SENDER_EMAIL,
                password=EMAIL_APP_PASSWORD,
                timeout=30
            )
            
            logger.info(f"✅ OTP sent to {email}")
            return True, "OTP sent successfully"
            
        except Exception as e:
            logger.error(f"❌ Email error: {e}")
            return False, str(e)

# =========================
# HELPER FUNCTIONS
# =========================

def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id == ADMIN_ID

def get_main_menu(is_approved: bool = True) -> ReplyKeyboardMarkup:
    """Get main menu keyboard"""
    if is_approved:
        keyboard = [
            ["💰 My Savings", "📈 Savings Plans"],
            ["➕ Add Funds", "➖ Withdraw"],
            ["📜 History", "📞 Support & About"]
        ]
    else:
        keyboard = [["⏳ Pending Approval"]]
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_crypto_methods_keyboard() -> InlineKeyboardMarkup:
    """Get crypto methods keyboard"""
    keyboard = [
        [InlineKeyboardButton("₿ BTC", callback_data="method_btc")],
        [InlineKeyboardButton("Ξ ETH", callback_data="method_eth")],
        [InlineKeyboardButton("💲 USDT (ERC20)", callback_data="method_usdt")],
        [InlineKeyboardButton("💲 USDC (ERC20)", callback_data="method_usdc")],
        [InlineKeyboardButton("🔙 Cancel", callback_data="method_cancel")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_crypto_address(currency: str) -> str:
    """Get crypto address for currency"""
    addresses = {
        'btc': BTC_ADDRESS,
        'eth': ETH_ADDRESS,
        'usdt': USDT_ADDRESS,
        'usdc': USDC_ADDRESS
    }
    return addresses.get(currency.lower(), "Address not available")

def get_support_button() -> InlineKeyboardMarkup:
    """Get support contact button"""
    keyboard = [[InlineKeyboardButton("📞 Contact Support", url=f"https://t.me/{SUPPORT_USERNAME}")]]
    return InlineKeyboardMarkup(keyboard)

# =========================
# START HANDLER
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    user_id = user.id
    
    # Admin
    if is_admin(user_id):
        await show_admin_panel(update, context)
        return
    
    # Check if user exists
    db_user = db.get_user(user_id)
    
    if not db_user:
        # New user - show welcome and ask for referral
        welcome_text = (
            "👋 *Welcome to Pillar Digital Bank!*\n\n"
            "We're glad to have you. Secure, simple, and smart banking starts here.\n\n"
            "📝 *Registration Steps:*\n"
            "1️⃣ Full Name\n"
            "2️⃣ Phone Number\n"
            "3️⃣ Email Address\n"
            "4️⃣ Email Verification (OTP)\n"
            "5️⃣ Admin Approval\n\n"
            "💡 *Benefits:*\n"
            "• $5 Registration Bonus\n"
            "• Referral Bonus ($1 per referral)\n"
            "• Daily Interest on Savings\n"
            "• 24/7 Customer Support\n\n"
            "👇 *To begin, please answer a few questions.*"
        )
        
        await update.message.reply_text(
            welcome_text,
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Ask for referral code
        await ask_referral(update, context)
        
    elif db_user['status'] == 'PENDING':
        if not db_user.get('is_email_verified', False):
            await update.message.reply_text(
                "📧 *Email Verification Required*\n\n"
                "Please check your email for OTP code.\n\n"
                "Use `/verify <code>` to verify your email.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                "⏳ *Account Pending Approval*\n\n"
                "Your registration is under review by our admin team.\n"
                "You'll be notified within 24-48 hours.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    elif db_user['status'] == 'APPROVED':
        await show_user_dashboard(update, context, db_user)
    
    elif db_user['status'] == 'REJECTED':
        await update.message.reply_text(
            "❌ *Registration Declined*\n\n"
            "Your account registration has been rejected.\n\n"
            "Please contact customer support for assistance.",
            reply_markup=get_support_button(),
            parse_mode=ParseMode.MARKDOWN
        )

async def ask_referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ask for referral code"""
    keyboard = [
        [InlineKeyboardButton("⏭️ Skip Referral", callback_data="skip_referral")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_registration")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "👥 *Referral Code*\n\n"
        "Do you have a referral code?\n\n"
        "• If yes, please enter it now\n"
        "• If not, click 'Skip Referral'\n\n"
        "Both you and your referrer will get $1 bonus!",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )
    
    return REFERRAL

# =========================
# REGISTRATION HANDLERS
# =========================

async def handle_referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle referral code input"""
    referral_code = update.message.text.strip().upper() if update.message.text else None
    
    if referral_code:
        # Validate referral code
        referrer = db.get_user_by_referral(referral_code)
        if referrer:
            context.user_data['referred_by'] = referrer['telegram_id']
            await update.message.reply_text("✅ Referral code accepted!")
        else:
            await update.message.reply_text(
                "❌ Invalid referral code. Please try again or skip.",
                parse_mode=ParseMode.MARKDOWN
            )
            return REFERRAL
    
    context.user_data['referral_processed'] = True
    
    # Start full name collection
    await update.message.reply_text(
        "📝 *Step 1/4: Full Name*\n\n"
        "Please enter your full legal name:\n"
        "• Example: `John Smith`\n"
        "• Minimum 2 characters\n"
        "• Use your official name\n\n"
        "Type /cancel to cancel registration.",
        parse_mode=ParseMode.MARKDOWN
    )
    return FULL_NAME

async def referral_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle referral callback"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "skip_referral":
        await query.edit_message_text("⏭️ Referral code skipped.")
        context.user_data['referral_processed'] = True
        
        await query.message.reply_text(
            "📝 *Step 1/4: Full Name*\n\n"
            "Please enter your full legal name:\n"
            "• Example: `John Smith`\n"
            "• Minimum 2 characters\n\n"
            "Type /cancel to cancel registration.",
            parse_mode=ParseMode.MARKDOWN
        )
        return FULL_NAME
    else:
        await query.edit_message_text("❌ Registration cancelled.")
        return ConversationHandler.END

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
        "📞 *Step 2/4: Phone Number*\n\n"
        "Please enter your phone number:\n"
        "• Include country code\n"
        "• Example: `+1234567890`\n\n"
        "Type /cancel to cancel.",
        parse_mode=ParseMode.MARKDOWN
    )
    return PHONE

async def register_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process phone number"""
    phone = update.message.text.strip()
    
    if not SecurityUtils.validate_phone(phone):
        await update.message.reply_text(
            "❌ Invalid phone number.\n"
            "Please use format: `+1234567890`",
            parse_mode=ParseMode.MARKDOWN
        )
        return PHONE
    
    context.user_data['phone'] = phone
    
    await update.message.reply_text(
        "📧 *Step 3/4: Email Address*\n\n"
        "Please enter your email address:\n"
        "• Example: `name@example.com`\n"
        "• You'll receive a 6-digit OTP code\n"
        "• Valid for 10 minutes\n\n"
        "Type /cancel to cancel.",
        parse_mode=ParseMode.MARKDOWN
    )
    return EMAIL

async def register_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process email and send OTP"""
    email = update.message.text.strip().lower()
    
    if not SecurityUtils.validate_email(email):
        await update.message.reply_text(
            "❌ Invalid email format.\n"
            "Please enter a valid email: `name@example.com`",
            parse_mode=ParseMode.MARKDOWN
        )
        return EMAIL
    
    # Check if email exists
    if db.get_user_by_email(email):
        await update.message.reply_text(
            "❌ This email is already registered.\n"
            "Please use a different email address.",
            parse_mode=ParseMode.MARKDOWN
        )
        return EMAIL
    
    # Store email
    context.user_data['email'] = email
    
    # Create user (temporary)
    user_id = update.effective_user.id
    full_name = context.user_data.get('full_name')
    phone = context.user_data.get('phone')
    referred_by = context.user_data.get('referred_by')
    
    # Temporary password (user will set later)
    temp_password = SecurityUtils.hash_password(secrets.token_hex(8))
    
    success = db.create_user(
        telegram_id=user_id,
        full_name=full_name,
        phone=phone,
        email=email,
        password_hash=temp_password,
        referred_by=referred_by
    )
    
    if not success:
        await update.message.reply_text("❌ Registration failed. Please try again.")
        return ConversationHandler.END
    
    # Generate and send OTP
    otp = SecurityUtils.generate_otp()
    db.save_otp(user_id, otp)
    
    success, message = await EmailService.send_otp(email, otp, full_name)
    
    if success:
        await update.message.reply_text(
            "✅ *Email Verification Required*\n\n"
            f"📧 Email: `{SecurityUtils.mask_email(email)}`\n\n"
            "A 6-digit OTP code has been sent to your email.\n\n"
            "Please use `/verify <code>` to verify your email.\n"
            "Example: `/verify 123456`\n\n"
            f"⏳ Code expires in {OTP_EXPIRY_MINUTES} minutes.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            f"❌ Failed to send email: {message}\n"
            "Please contact support.",
            reply_markup=get_support_button(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    return ConversationHandler.END

async def verify_otp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /verify command"""
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
        # Get user data
        user = db.get_user(user_id)
        
        # Notify admin
        await notify_admin_new_user(context.bot, user)
        
        await update.message.reply_text(
            "✅ *Email Verified!*\n\n"
            "Your email has been successfully verified.\n\n"
            "⏳ Your account is now pending admin approval.\n"
            "You'll be notified within 24-48 hours.\n\n"
            "📞 For urgent matters, contact support.",
            reply_markup=get_support_button(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            f"❌ {message}",
            parse_mode=ParseMode.MARKDOWN
        )

# =========================
# ADMIN NOTIFICATION
# =========================

async def notify_admin_new_user(bot, user: Dict[str, Any]):
    """Notify admin about new user registration"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"admin_approve_{user['telegram_id']}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"admin_reject_{user['telegram_id']}")
        ],
        [InlineKeyboardButton("👤 View Details", callback_data=f"admin_view_{user['telegram_id']}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    masked_email = SecurityUtils.mask_email(user['email'])
    masked_phone = SecurityUtils.mask_phone(user['phone_number'])
    
    message = (
        "🆕 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     NEW REGISTRATION\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        f"👤 *Name:* {user['full_name']}\n"
        f"🆔 *ID:* `{user['telegram_id']}`\n"
        f"📱 *Phone:* {masked_phone}\n"
        f"📧 *Email:* {masked_email}\n"
        f"👥 *Referral:* {user.get('referred_by', 'None')}\n"
        f"📅 *Time:* {datetime.now(NY_TZ).strftime('%Y-%m-%d %I:%M %p')} NY\n\n"
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
    except Exception as e:
        logger.error(f"Failed to notify admin: {e}")

# =========================
# ADMIN CALLBACK HANDLER
# =========================

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle admin callbacks"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Unauthorized.")
        return
    
    data = query.data
    
    if data.startswith("admin_approve_"):
        user_id = int(data.replace("admin_approve_", ""))
        await approve_user(query, context, user_id)
    
    elif data.startswith("admin_reject_"):
        user_id = int(data.replace("admin_reject_", ""))
        await reject_user(query, context, user_id)
    
    elif data.startswith("admin_view_"):
        user_id = int(data.replace("admin_view_", ""))
        await view_user_details(query, user_id)

async def approve_user(query, context, user_id: int):
    """Approve user registration"""
    user = db.get_user(user_id)
    if not user:
        await query.edit_message_text(f"❌ User {user_id} not found.")
        return
    
    # Update status
    if db.update_user_status(user_id, 'APPROVED'):
        # Add registration bonus
        db.add_registration_bonus(user_id)
        
        # Process referral bonus if any
        if user.get('referred_by'):
            db.process_referral_bonus(user_id)
        
        # Log audit
        db.log_audit(
            action='USER_APPROVED',
            actor='ADMIN',
            actor_id=ADMIN_ID,
            target_user=user_id,
            description=f"User {user_id} approved with $5 bonus"
        )
        
        # Notify user
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "✅ *━━━━━━━━━━━━━━━━━━━━*\n"
                    "     ACCOUNT APPROVED!\n"
                    "*━━━━━━━━━━━━━━━━━━━━*\n\n"
                    f"👤 Welcome, *{user['full_name']}*!\n\n"
                    f"💰 *$5.00 registration bonus* has been added to your account.\n\n"
                    f"📋 *Your Client ID:* `{user['telegram_id']}`\n\n"
                    f"*Available Services:*\n"
                    f"• 💰 My Savings - Check balance\n"
                    f"• 📈 Savings Plans - Start saving\n"
                    f"• ➕ Add Funds - Deposit crypto\n"
                    f"• ➖ Withdraw - Request withdrawal\n"
                    f"• 📜 History - View transactions\n\n"
                    f"Use the menu below to get started!",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_main_menu(True)
                )
            )
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")
        
        await query.edit_message_text(
            f"✅ *User Approved*\n\n"
            f"👤 Name: {user['full_name']}\n"
            f"🆔 ID: `{user_id}`\n"
            f"💰 $5.00 bonus added\n\n"
            f"User has been notified.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await query.edit_message_text(f"❌ Failed to approve user {user_id}")

async def reject_user(query, context, user_id: int):
    """Reject user registration"""
    user = db.get_user(user_id)
    if not user:
        await query.edit_message_text(f"❌ User {user_id} not found.")
        return
    
    # Update status
    if db.update_user_status(user_id, 'REJECTED'):
        # Log audit
        db.log_audit(
            action='USER_REJECTED',
            actor='ADMIN',
            actor_id=ADMIN_ID,
            target_user=user_id,
            description=f"User {user_id} rejected"
        )
        
        # Notify user
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "❌ *━━━━━━━━━━━━━━━━━━━━*\n"
                    "     REGISTRATION UPDATE\n"
                    "*━━━━━━━━━━━━━━━━━━━━*\n\n"
                    f"Dear {user['full_name']},\n\n"
                    "Unfortunately, your account registration has been rejected.\n\n"
                    "*Possible reasons:*\n"
                    "• Incomplete information\n"
                    "• Unable to verify identity\n"
                    "• Duplicate account\n\n"
                    "📞 Please contact customer support for assistance.",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_support_button()
                )
            )
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")
        
        # Delete from database
        # In production, you might want to keep rejected users
        # db.delete_user(user_id)
        
        await query.edit_message_text(
            f"❌ *User Rejected*\n\n"
            f"👤 Name: {user['full_name']}\n"
            f"🆔 ID: `{user_id}`\n\n"
            f"User has been notified.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await query.edit_message_text(f"❌ Failed to reject user {user_id}")

async def view_user_details(query, user_id: int):
    """View user details"""
    user = db.get_user(user_id)
    account = db.get_account(user_id)
    
    if not user:
        await query.edit_message_text(f"❌ User {user_id} not found.")
        return
    
    status_icon = {
        'PENDING': '⏳',
        'APPROVED': '✅',
        'REJECTED': '❌'
    }.get(user['status'], '❓')
    
    message = (
        f"👤 *━━━━━━━━━━━━━━━━━━━━*\n"
        f"     USER DETAILS\n"
        f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
        f"*Name:* {user['full_name']}\n"
        f"*ID:* `{user['telegram_id']}`\n"
        f"*Phone:* {SecurityUtils.mask_phone(user['phone_number'])}\n"
        f"*Email:* {SecurityUtils.mask_email(user['email'])}\n"
        f"*Status:* {status_icon} {user['status']}\n"
        f"*Email Verified:* {'✅ Yes' if user.get('is_email_verified') else '❌ No'}\n"
        f"*Balance:* ${account['balance'] if account else 0:.2f}\n"
        f"*Available:* ${account['available_balance'] if account else 0:.2f}\n"
        f"*Locked:* ${account['locked_balance'] if account else 0:.2f}\n"
        f"*Referral Code:* `{user.get('referral_code', 'N/A')}`\n"
        f"*Referred By:* {user.get('referred_by', 'None')}\n"
        f"*Registered:* {user['created_at'].strftime('%Y-%m-%d %H:%M')}\n\n"
        f"*━━━━━━━━━━━━━━━━━━━━*"
    )
    
    await query.edit_message_text(
        message,
        parse_mode=ParseMode.MARKDOWN
    )

# =========================
# ADMIN PANEL
# =========================

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show admin control panel"""
    # Get statistics
    all_users = db.get_all_users()
    pending_users = [u for u in all_users if u['status'] == 'PENDING']
    approved_users = [u for u in all_users if u['status'] == 'APPROVED']
    
    total_balance = sum(float(u.get('balance', 0)) for u in all_users if u.get('balance'))
    
    pending_deposits = db.get_pending_transactions('DEPOSIT')
    pending_withdrawals = db.get_pending_transactions('WITHDRAW')
    
    message = (
        "🔐 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     ADMIN CONTROL PANEL\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        f"📊 *System Overview*\n"
        f"• Total Users: `{len(all_users)}`\n"
        f"• Pending: `{len(pending_users)}`\n"
        f"• Approved: `{len(approved_users)}`\n"
        f"• Total Balance: `${total_balance:.2f}`\n\n"
        f"⏳ *Pending Actions*\n"
        f"• Deposits: `{len(pending_deposits)}`\n"
        f"• Withdrawals: `{len(pending_withdrawals)}`\n\n"
        f"🕐 *NY Time:* {datetime.now(NY_TZ).strftime('%I:%M %p')}\n\n"
        f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
        f"*Commands:*\n"
        f"/users - View all users\n"
        f"/pending - View pending users\n"
        f"/transactions - View pending transactions\n"
        f"/audit - View audit logs\n"
        f"/stats - Detailed statistics"
    )
    
    keyboard = [
        ["📋 Pending Users", "👥 All Users"],
        ["💰 Pending Deposits", "💸 Pending Withdrawals"],
        ["📊 Statistics", "📜 Audit Logs"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(
        message,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def admin_pending_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show pending users for admin"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    
    pending = db.get_pending_users()
    
    if not pending:
        await update.message.reply_text("✅ No pending users.")
        return
    
    message = "⏳ *Pending Users*\n\n"
    keyboard = []
    
    for user in pending[:5]:
        message += (
            f"👤 *{user['full_name']}*\n"
            f"🆔 `{user['telegram_id']}`\n"
            f"📧 {SecurityUtils.mask_email(user['email'])}\n"
            f"📅 {user['created_at'].strftime('%Y-%m-%d')}\n"
            "━━━━━━━━━━━━━━\n"
        )
        
        keyboard.append([
            InlineKeyboardButton(f"✅ Approve {user['full_name'][:10]}", 
                               callback_data=f"admin_approve_{user['telegram_id']}"),
            InlineKeyboardButton("❌ Reject", 
                               callback_data=f"admin_reject_{user['telegram_id']}")
        ])
    
    if len(pending) > 5:
        message += f"... and {len(pending) - 5} more\n"
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        message,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def admin_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all users for admin"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    
    users = db.get_all_users()
    
    if not users:
        await update.message.reply_text("📭 No users found.")
        return
    
    message = "👥 *All Users*\n\n"
    
    for user in users[:10]:
        status_icon = {
            'PENDING': '⏳',
            'APPROVED': '✅',
            'REJECTED': '❌'
        }.get(user['status'], '❓')
        
        message += (
            f"{status_icon} *{user['full_name']}*\n"
            f"🆔 `{user['telegram_id']}`\n"
            f"💰 ${user.get('balance', 0):.2f}\n"
            f"📅 {user['created_at'].strftime('%Y-%m-%d')}\n"
            "━━━━━━━━━━━━━━\n"
        )
    
    if len(users) > 10:
        message += f"... and {len(users) - 10} more\n"
    
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.MARKDOWN
    )

# =========================
# USER DASHBOARD
# =========================

async def show_user_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE, user: Dict[str, Any]):
    """Show user main dashboard"""
    account = db.get_account(user['telegram_id'])
    
    # Calculate pending interest
    pending_interest = db.calculate_and_add_interest(user['telegram_id'])
    
    if pending_interest > 0 and db.is_connected:
        # Apply interest to account
        db.cursor.execute("""
            UPDATE accounts 
            SET balance = balance + %s,
                available_balance = available_balance + %s,
                total_interest_earned = total_interest_earned + %s
            WHERE user_telegram_id = %s
        """, (pending_interest, pending_interest, pending_interest, user['telegram_id']))
        db.conn.commit()
    
    # Refresh account
    account = db.get_account(user['telegram_id'])
    
    message = (
        f"🏦 *━━━━━━━━━━━━━━━━━━━━*\n"
        f"     WELCOME BACK!\n"
        f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
        f"👤 *{user['full_name']}*\n"
        f"🆔 `{user['telegram_id']}`\n\n"
        f"💰 *Balance Summary*\n"
        f"• Total Balance: `${account['balance']:.2f}`\n"
        f"• Available: `${account['available_balance']:.2f}`\n"
        f"• Locked: `${account['locked_balance']:.2f}`\n"
        f"• Interest Earned: `${account['total_interest_earned']:.2f}`\n\n"
        f"📊 *Today*\n"
        f"• NY Time: {datetime.now(NY_TZ).strftime('%I:%M %p')}\n"
        f"• Banking: {'🟢 Open' if 8 <= datetime.now(NY_TZ).hour < 16 else '🔴 Closed'}\n\n"
        f"Select an option below:"
    )
    
    await update.message.reply_text(
        message,
        reply_markup=get_main_menu(True),
        parse_mode=ParseMode.MARKDOWN
    )

# =========================
# MY SAVINGS HANDLER
# =========================

async def my_savings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle My Savings button"""
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    if not user or user['status'] != 'APPROVED':
        await update.message.reply_text(
            "⏳ Your account is pending approval.",
            reply_markup=get_main_menu(False)
        )
        return
    
    account = db.get_account(user_id)
    plans = db.get_user_savings_plans(user_id)
    
    # Calculate any pending interest
    pending_interest = db.calculate_and_add_interest(user_id)
    if pending_interest > 0 and db.is_connected:
        # Apply interest
        db.cursor.execute("""
            UPDATE accounts 
            SET balance = balance + %s,
                available_balance = available_balance + %s,
                total_interest_earned = total_interest_earned + %s
            WHERE user_telegram_id = %s
        """, (pending_interest, pending_interest, pending_interest, user_id))
        db.conn.commit()
        account = db.get_account(user_id)
    
    message = (
        f"💰 *━━━━━━━━━━━━━━━━━━━━*\n"
        f"     MY SAVINGS\n"
        f"*━━━━━━━━━━━━━━━━━━━━*\n\n"
        f"👤 *User ID:* `{user_id}`\n\n"
        f"💳 *Account Balance*\n"
        f"• Total: `${account['balance']:.2f}`\n"
        f"• Available: `${account['available_balance']:.2f}`\n"
        f"• Locked in Plans: `${account['locked_balance']:.2f}`\n\n"
        f"📈 *Savings Plans*\n"
    )
    
    if not plans:
        message += "• You don't have any active savings plans.\n"
        message += "• Use /savings to start a plan!\n"
    else:
        for plan in plans:
            status_icon = "🟢" if plan['status'] == 'ACTIVE' else "🔴"
            progress = (datetime.now().date() - plan['start_date']).days
            total_days = (plan['end_date'] - plan['start_date']).days
            progress_pct = min(100, int((progress / total_days) * 100))
            
            message += (
                f"\n{status_icon} *{plan['plan_name']}*\n"
                f"   Principal: `${plan['principal_amount']:.2f}`\n"
                f"   Current: `${plan['current_value']:.2f}`\n"
                f"   Interest: `${plan['interest_earned']:.2f}`\n"
                f"   Progress: {progress}/{total_days} days ({progress_pct}%)\n"
            )
    
    message += f"\n*━━━━━━━━━━━━━━━━━━━━*"
    
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.MARKDOWN
    )

# =========================
# SAVINGS PLANS HANDLER
# =========================

async def savings_plans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show available savings plans"""
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    if not user or user['status'] != 'APPROVED':
        await update.message.reply_text(
            "⏳ Your account is pending approval.",
            reply_markup=get_main_menu(False)
        )
        return
    
    templates = db.get_savings_templates()
    
    message = "📈 *━━━━━━━━━━━━━━━━━━━━*\n"
    message += "     SAVINGS PLANS\n"
    message += "*━━━━━━━━━━━━━━━━━━━━*\n\n"
    
    keyboard = []
    
    for template in templates:
        lock_icon = "🔒" if template['is_locked'] else "🔓"
        message += (
            f"{lock_icon} *{template['name']}*\n"
            f"📝 {template['description']}\n"
            f"⏱️ Duration: {template['duration_days']} days\n"
            f"💰 Minimum: `${template['min_amount']:.2f}`\n"
            f"📈 Daily Rate: {template['daily_rate']*100:.2f}%\n"
            f"🎯 Total Return: {template['total_rate']}%\n"
            f"━━━━━━━━━━━━━━\n\n"
        )
        
        keyboard.append([
            InlineKeyboardButton(
                f"Select {template['name']}",
                callback_data=f"plan_{template['id']}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        message,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def plan_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle plan selection"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "back_to_menu":
        await start(update, context)
        return
    
    if query.data.startswith("plan_"):
        plan_id = int(query.data.replace("plan_", ""))
        templates = db.get_savings_templates()
        selected_plan = next((p for p in templates if p['id'] == plan_id), None)
        
        if selected_plan:
            context.user_data['selected_plan'] = selected_plan
            
            await query.edit_message_text(
                f"💰 *{selected_plan['name']} Plan Selected*\n\n"
                f"📋 *Plan Details:*\n"
                f"• Minimum: `${selected_plan['min_amount']:.2f}`\n"
                f"• Duration: {selected_plan['duration_days']} days\n"
                f"• Daily Rate: {selected_plan['daily_rate']*100:.2f}%\n"
                f"• Total Return: {selected_plan['total_rate']}%\n"
                f"• Locked: {'Yes 🔒' if selected_plan['is_locked'] else 'No 🔓'}\n\n"
                f"💵 *Enter amount to save:*\n\n"
                f"Type /cancel to cancel.",
                parse_mode=ParseMode.MARKDOWN
            )
            return SAVINGS_AMOUNT

async def savings_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process savings amount"""
    amount_str = update.message.text.strip()
    
    is_valid, amount, error = SecurityUtils.validate_amount(amount_str)
    if not is_valid:
        await update.message.reply_text(f"❌ {error}\n\nPlease try again:")
        return SAVINGS_AMOUNT
    
    selected_plan = context.user_data.get('selected_plan')
    if not selected_plan:
        await update.message.reply_text("❌ Plan selection expired. Please start over.")
        return ConversationHandler.END
    
    if amount < selected_plan['min_amount']:
        await update.message.reply_text(
            f"❌ Minimum amount for {selected_plan['name']} is ${selected_plan['min_amount']:.2f}\n"
            f"Please enter a valid amount:"
        )
        return SAVINGS_AMOUNT
    
    # Check balance
    user_id = update.effective_user.id
    account = db.get_account(user_id)
    
    if account['available_balance'] < amount:
        await update.message.reply_text(
            f"❌ *Insufficient Balance*\n\n"
            f"Your available balance: `${account['available_balance']:.2f}`\n"
            f"Required: `${amount:.2f}`\n\n"
            f"Please add funds first.",
            reply_markup=get_main_menu(True)
        )
        return ConversationHandler.END
    
    context.user_data['savings_amount'] = amount
    
    # Calculate expected returns
    daily_interest = amount * selected_plan['daily_rate']
    total_interest = daily_interest * selected_plan['duration_days']
    final_amount = amount + total_interest
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Confirm", callback_data="confirm_savings"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_savings")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"✅ *Confirm Savings Plan*\n\n"
        f"📋 *Plan:* {selected_plan['name']}\n"
        f"💰 *Principal:* `${amount:.2f}`\n"
        f"📅 *Duration:* {selected_plan['duration_days']} days\n"
        f"📈 *Daily Interest:* `${daily_interest:.4f}`\n"
        f"🎯 *Total Interest:* `${total_interest:.2f}`\n"
        f"💵 *Maturity Value:* `${final_amount:.2f}`\n"
        f"🔒 *Locked:* {'Yes' if selected_plan['is_locked'] else 'No'}\n\n"
        f"*Confirm to proceed:*",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )
    return SAVINGS_CONFIRM

async def savings_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle savings confirmation"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "cancel_savings":
        await query.edit_message_text("❌ Savings plan cancelled.")
        return ConversationHandler.END
    
    user_id = query.from_user.id
    selected_plan = context.user_data.get('selected_plan')
    amount = context.user_data.get('savings_amount')
    
    if not selected_plan or not amount:
        await query.edit_message_text("❌ Session expired. Please start over.")
        return ConversationHandler.END
    
    # Create savings plan
    plan_id = db.create_savings_plan(
        telegram_id=user_id,
        template_id=selected_plan['id'],
        plan_name=selected_plan['name'],
        principal_amount=amount,
        daily_rate=selected_plan['daily_rate'],
        duration_days=selected_plan['duration_days'],
        is_locked=selected_plan['is_locked']
    )
    
    if plan_id:
        # Lock funds
        db.lock_funds(user_id, amount)
        
        # Create transaction record
        db.create_transaction(
            telegram_id=user_id,
            tx_type='SAVINGS_CREATED',
            amount=amount,
            method='SAVINGS_PLAN'
        )
        
        # Log audit
        db.log_audit(
            action='SAVINGS_CREATED',
            actor='USER',
            actor_id=user_id,
            description=f"Created {selected_plan['name']} savings plan with ${amount}",
            reference_id=None
        )
        
        await query.edit_message_text(
            f"✅ *Savings Plan Created Successfully!*\n\n"
            f"📋 *Plan ID:* `{plan_id}`\n"
            f"💰 *Amount:* `${amount:.2f}`\n"
            f"📈 *Daily Interest:* {selected_plan['daily_rate']*100:.2f}%\n\n"
            f"⏳ Interest will be calculated daily at 4:30 PM NY Time.\n\n"
            f"Use /mysavings to track your progress!",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await query.edit_message_text("❌ Failed to create savings plan. Please try again.")
    
    # Clear context
    context.user_data.pop('selected_plan', None)
    context.user_data.pop('savings_amount', None)
    
    return ConversationHandler.END

# =========================
# ADD FUNDS HANDLER
# =========================

async def add_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Add Funds button"""
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    if not user or user['status'] != 'APPROVED':
        await update.message.reply_text(
            "⏳ Your account is pending approval.",
            reply_markup=get_main_menu(False)
        )
        return
    
    await update.message.reply_text(
        "➕ *Add Funds*\n\n"
        "Select your deposit method:",
        reply_markup=get_crypto_methods_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    return DEPOSIT_METHOD

async def deposit_method_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle deposit method selection"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "method_cancel":
        await query.edit_message_text("❌ Deposit cancelled.")
        return ConversationHandler.END
    
    method = query.data.replace("method_", "").upper()
    context.user_data['deposit_method'] = method
    
    await query.edit_message_text(
        f"💵 *Enter Amount*\n\n"
        f"Method: *{method}*\n\n"
        f"Please enter the amount you wish to deposit:\n"
        f"(Minimum: $10.00, Maximum: $1,000,000.00)\n\n"
        f"Type /cancel to cancel.",
        parse_mode=ParseMode.MARKDOWN
    )
    return DEPOSIT_AMOUNT

async def deposit_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process deposit amount"""
    amount_str = update.message.text.strip()
    
    is_valid, amount, error = SecurityUtils.validate_amount(amount_str)
    if not is_valid:
        await update.message.reply_text(f"❌ {error}\n\nPlease try again:")
        return DEPOSIT_AMOUNT
    
    user_id = update.effective_user.id
    method = context.user_data.get('deposit_method')
    
    # Create transaction
    tx_id = db.create_transaction(
        telegram_id=user_id,
        tx_type='DEPOSIT',
        amount=amount,
        method=method,
        crypto_currency=method
    )
    
    if tx_id:
        # Log audit
        db.log_audit(
            action='DEPOSIT_REQUESTED',
            actor='USER',
            actor_id=user_id,
            description=f"Deposit request: ${amount} via {method}",
            reference_id=None
        )
        
        # Notify admin
        await notify_admin_deposit(context.bot, user_id, amount, method, tx_id)
        
        address = get_crypto_address(method)
        
        await update.message.reply_text(
            f"✅ *Deposit Request Submitted*\n\n"
            f"📋 *Transaction ID:* `{tx_id}`\n"
            f"💰 *Amount:* `${amount:.2f}`\n"
            f"💳 *Method:* {method}\n\n"
            f"📤 *Please send the exact amount to:*\n"
            f"`{address}`\n\n"
            f"📸 *After sending, please submit your transaction screenshot to:*\n"
            f"https://t.me/{SUPPORT_USERNAME}\n\n"
            f"⏳ *Status:* Pending Confirmation\n"
            f"• Admin will verify your payment\n"
            f"• Funds will be credited within 1-24 hours\n"
            f"• You'll be notified when completed",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text("❌ Failed to create deposit request. Please try again.")
    
    context.user_data.pop('deposit_method', None)
    return ConversationHandler.END

async def notify_admin_deposit(bot, user_id: int, amount: Decimal, method: str, tx_id: str):
    """Notify admin about deposit request"""
    user = db.get_user(user_id)
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Confirm", callback_data=f"admin_confirm_deposit_{tx_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"admin_reject_deposit_{tx_id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            f"💰 *New Deposit Request*\n\n"
            f"👤 User: {user['full_name']}\n"
            f"🆔 ID: `{user_id}`\n"
            f"💰 Amount: `${amount:.2f}`\n"
            f"💳 Method: {method}\n"
            f"📋 TX ID: `{tx_id}`\n"
            f"⏰ Time: {datetime.now(NY_TZ).strftime('%Y-%m-%d %I:%M %p')} NY\n\n"
            f"Verify and confirm when payment is received."
        ),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

# =========================
# WITHDRAW HANDLER
# =========================

async def withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Withdraw button"""
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    if not user or user['status'] != 'APPROVED':
        await update.message.reply_text(
            "⏳ Your account is pending approval.",
            reply_markup=get_main_menu(False)
        )
        return
    
    account = db.get_account(user_id)
    
    await update.message.reply_text(
        f"➖ *Withdrawal*\n\n"
        f"Your available balance: `${account['available_balance']:.2f}`\n\n"
        f"Please enter the amount you wish to withdraw:\n"
        f"(Minimum: $10.00)\n\n"
        f"Type /cancel to cancel.",
        parse_mode=ParseMode.MARKDOWN
    )
    return WITHDRAW_AMOUNT

async def withdraw_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process withdrawal amount"""
    amount_str = update.message.text.strip()
    
    is_valid, amount, error = SecurityUtils.validate_amount(amount_str)
    if not is_valid:
        await update.message.reply_text(f"❌ {error}\n\nPlease try again:")
        return WITHDRAW_AMOUNT
    
    user_id = update.effective_user.id
    account = db.get_account(user_id)
    
    if account['available_balance'] < amount:
        await update.message.reply_text(
            f"❌ *Insufficient Balance*\n\n"
            f"Available: `${account['available_balance']:.2f}`\n"
            f"Requested: `${amount:.2f}`\n\n"
            f"Please try a smaller amount.",
            parse_mode=ParseMode.MARKDOWN
        )
        return WITHDRAW_AMOUNT
    
    context.user_data['withdraw_amount'] = amount
    
    # Generate and send OTP
    otp = SecurityUtils.generate_otp()
    db.save_otp(user_id, otp)
    
    await EmailService.send_otp(user['email'], otp, user['full_name'])
    
    await update.message.reply_text(
        f"📧 *Verification Required*\n\n"
        f"A 6-digit OTP code has been sent to your email: {SecurityUtils.mask_email(user['email'])}\n\n"
        f"Please enter the code to continue:",
        parse_mode=ParseMode.MARKDOWN
    )
    return WITHDRAW_OTP

async def withdraw_otp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Verify OTP for withdrawal"""
    otp = update.message.text.strip()
    user_id = update.effective_user.id
    
    # Verify OTP
    self.cursor.execute("SELECT otp_code, otp_expiry FROM users WHERE telegram_id = %s", (user_id,))
    result = self.cursor.fetchone()
    
    if not result or result['otp_code'] != otp:
        await update.message.reply_text("❌ Invalid OTP. Please try again:")
        return WITHDRAW_OTP
    
    if datetime.now() > result['otp_expiry']:
        await update.message.reply_text("❌ OTP expired. Please start over.")
        return ConversationHandler.END
    
    # Clear OTP
    db.cursor.execute("UPDATE users SET otp_code = NULL, otp_expiry = NULL WHERE telegram_id = %s", (user_id,))
    db.conn.commit()
    
    await update.message.reply_text(
        "💳 *Select Withdrawal Method*",
        reply_markup=get_crypto_methods_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    return WITHDRAW_METHOD

async def withdraw_method_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle withdrawal method selection"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "method_cancel":
        await query.edit_message_text("❌ Withdrawal cancelled.")
        return ConversationHandler.END
    
    method = query.data.replace("method_", "").upper()
    context.user_data['withdraw_method'] = method
    
    await query.edit_message_text(
        f"📤 *Enter Your {method} Address*\n\n"
        f"Please provide your {method} wallet address:\n\n"
        f"⚠️ *Double-check your address!*\n"
        f"Wrong addresses cannot be recovered.",
        parse_mode=ParseMode.MARKDOWN
    )
    return WITHDRAW_ADDRESS

async def withdraw_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process withdrawal address"""
    address = update.message.text.strip()
    
    if len(address) < 10:
        await update.message.reply_text("❌ Invalid address. Please try again:")
        return WITHDRAW_ADDRESS
    
    user_id = update.effective_user.id
    amount = context.user_data.get('withdraw_amount')
    method = context.user_data.get('withdraw_method')
    
    # Create transaction
    tx_id = db.create_transaction(
        telegram_id=user_id,
        tx_type='WITHDRAW',
        amount=amount,
        method=method,
        crypto_currency=method,
        crypto_address=address
    )
    
    if tx_id:
        # Log audit
        db.log_audit(
            action='WITHDRAWAL_REQUESTED',
            actor='USER',
            actor_id=user_id,
            description=f"Withdrawal request: ${amount} via {method}",
            reference_id=None
        )
        
        # Notify admin
        await notify_admin_withdrawal(context.bot, user_id, amount, method, address, tx_id)
        
        await update.message.reply_text(
            f"✅ *Withdrawal Request Submitted*\n\n"
            f"📋 *Transaction ID:* `{tx_id}`\n"
            f"💰 *Amount:* `${amount:.2f}`\n"
            f"💳 *Method:* {method}\n"
            f"📤 *Address:* `{address}`\n\n"
            f"⏳ *Status:* Pending Admin Approval\n"
            f"• Admin will process your request\n"
            f"• Funds will be sent within 1-24 hours\n"
            f"• You'll be notified when completed",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text("❌ Failed to create withdrawal request. Please try again.")
    
    # Clear context
    context.user_data.pop('withdraw_amount', None)
    context.user_data.pop('withdraw_method', None)
    
    return ConversationHandler.END

async def notify_admin_withdrawal(bot, user_id: int, amount: Decimal, method: str, address: str, tx_id: str):
    """Notify admin about withdrawal request"""
    user = db.get_user(user_id)
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"admin_approve_withdraw_{tx_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"admin_reject_withdraw_{tx_id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            f"💸 *New Withdrawal Request*\n\n"
            f"👤 User: {user['full_name']}\n"
            f"🆔 ID: `{user_id}`\n"
            f"💰 Amount: `${amount:.2f}`\n"
            f"💳 Method: {method}\n"
            f"📤 Address: `{address}`\n"
            f"📋 TX ID: `{tx_id}`\n"
            f"⏰ Time: {datetime.now(NY_TZ).strftime('%Y-%m-%d %I:%M %p')} NY\n\n"
            f"Verify and process this withdrawal."
        ),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

# =========================
# HISTORY HANDLER
# =========================

async def history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show transaction history"""
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    if not user or user['status'] != 'APPROVED':
        await update.message.reply_text(
            "⏳ Your account is pending approval.",
            reply_markup=get_main_menu(False)
        )
        return
    
    transactions = db.get_user_transactions(user_id, limit=10)
    
    if not transactions:
        await update.message.reply_text(
            "📭 *No Transactions Found*\n\n"
            "Your transaction history will appear here.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    message = "📜 *━━━━━━━━━━━━━━━━━━━━*\n"
    message += "     TRANSACTION HISTORY\n"
    message += "*━━━━━━━━━━━━━━━━━━━━*\n\n"
    
    for tx in transactions:
        icon = {
            'DEPOSIT': '➕',
            'WITHDRAW': '➖',
            'SAVINGS_CREATED': '📈',
            'INTEREST': '🎁'
        }.get(tx['type'], '🔄')
        
        status_icon = {
            'PENDING': '⏳',
            'COMPLETED': '✅',
            'REJECTED': '❌'
        }.get(tx['status'], '❓')
        
        message += (
            f"{status_icon} {icon} *{tx['type']}*\n"
            f"🆔 `{tx['transaction_id']}`\n"
            f"💰 `${tx['amount']:.2f}`\n"
            f"💳 {tx.get('method', 'BANK')}\n"
            f"⏰ {tx['requested_at'].strftime('%Y-%m-%d %H:%M')}\n"
            f"━━━━━━━━━━━━━━\n"
        )
    
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.MARKDOWN
    )

# =========================
# SUPPORT & ABOUT HANDLER
# =========================

async def support_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show support and about information"""
    message = (
        "📞 *━━━━━━━━━━━━━━━━━━━━*\n"
        "     CUSTOMER SUPPORT\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "*Official Contact Channels*\n"
        "📞 Phone: `+1 252 612 8324`\n"
        "📧 Email: `pillardigitalbank47@gmail.com`\n"
        "💬 Telegram: https://t.me/PillarDigitalBankCS47\n\n"
        "⏰ *Support Hours:* 24/7\n"
        "⏱️ *Response Time:* Within 24 hours\n\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "🏦 *ABOUT PILLAR DIGITAL BANK*\n\n"
        "Pillar Digital Bank is a digital financial services platform focused on structured savings solutions, secure account management, and transparent fund administration.\n\n"
        "*Core Values:*\n"
        "• 🔒 Security First\n"
        "• 📊 Transparency\n"
        "• 🤝 Trust\n"
        "• 💡 Innovation\n\n"
        "*Services:*\n"
        "• Structured digital savings programs\n"
        "• Secure balance monitoring\n"
        "• Manual verification protocols\n"
        "• Dedicated client support\n\n"
        "*━━━━━━━━━━━━━━━━━━━━*\n\n"
        "📄 *Terms of Use*\n"
        "By using our services, you agree to our terms and conditions. We reserve the right to modify policies without prior notice.\n\n"
        "🔐 *Privacy Policy*\n"
        "Your data is protected and never shared with third parties. All information is securely maintained.\n\n"
        "💰 *Funds Policy*\n"
        "• Deposits confirmed within 1-24 hours\n"
        "• Withdrawals processed manually\n"
        "• Daily interest calculated at 4:30 PM NY Time\n\n"
        "*━━━━━━━━━━━━━━━━━━━━*"
    )
    
    keyboard = [[InlineKeyboardButton("📞 Contact Support", url=f"https://t.me/{SUPPORT_USERNAME}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        message,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

# =========================
# CANCEL HANDLER
# =========================

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel current operation"""
    context.user_data.clear()
    await update.message.reply_text(
        "❌ Operation cancelled.\n\n"
        "Use /start to return to main menu.",
        reply_markup=get_main_menu(True)
    )
    return ConversationHandler.END

# =========================
# MENU ROUTER
# =========================

async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route menu button presses"""
    text = update.message.text
    
    if text == "💰 My Savings":
        await my_savings(update, context)
    elif text == "📈 Savings Plans":
        await savings_plans(update, context)
    elif text == "➕ Add Funds":
        await add_funds(update, context)
    elif text == "➖ Withdraw":
        await withdraw(update, context)
    elif text == "📜 History":
        await history(update, context)
    elif text == "📞 Support & About":
        await support_about(update, context)
    elif text == "⏳ Pending Approval":
        await update.message.reply_text(
            "⏳ Your account is pending admin approval.\n"
            "You'll be notified within 24-48 hours."
        )

# =========================
# MAIN APPLICATION
# =========================

def main():
    """Main application entry point"""
    
    logger.info("🚀 Starting Pillar Digital Bank...")
    
    # Create application
    app = Application.builder().token(BOT_TOKEN).build()
    
    # =========================
    # COMMAND HANDLERS
    # =========================
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("verify", verify_otp_command))
    app.add_handler(CommandHandler("cancel", cancel))
    
    # Admin commands
    app.add_handler(CommandHandler("users", admin_all_users))
    app.add_handler(CommandHandler("pending", admin_pending_users))
    app.add_handler(CommandHandler("audit", lambda u,c: u.message.reply_text("Audit logs coming soon.")))
    app.add_handler(CommandHandler("stats", lambda u,c: u.message.reply_text("Statistics coming soon.")))
    
    # =========================
    # REGISTRATION CONVERSATION
    # =========================
    
    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("register", ask_referral)],
        states={
            REFERRAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_referral),
                CallbackQueryHandler(referral_callback)
            ],
            FULL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_fullname)],
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_phone)],
            EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_email)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=False
    )
    app.add_handler(reg_conv)
    
    # =========================
    # SAVINGS PLAN CONVERSATION
    # =========================
    
    savings_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(plan_callback, pattern="^plan_")],
        states={
            SAVINGS_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, savings_amount)],
            SAVINGS_CONFIRM: [CallbackQueryHandler(savings_confirm, pattern="^(confirm_savings|cancel_savings)$")]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=False
    )
    app.add_handler(savings_conv)
    
    # =========================
    # DEPOSIT CONVERSATION
    # =========================
    
    deposit_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Text("➕ Add Funds"), add_funds)],
        states={
            DEPOSIT_METHOD: [CallbackQueryHandler(deposit_method_callback)],
            DEPOSIT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, deposit_amount)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=False
    )
    app.add_handler(deposit_conv)
    
    # =========================
    # WITHDRAWAL CONVERSATION
    # =========================
    
    withdraw_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Text("➖ Withdraw"), withdraw)],
        states={
            WITHDRAW_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_amount)],
            WITHDRAW_OTP: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_otp)],
            WITHDRAW_METHOD: [CallbackQueryHandler(withdraw_method_callback)],
            WITHDRAW_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_address)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=False
    )
    app.add_handler(withdraw_conv)
    
    # =========================
    # CALLBACK HANDLERS
    # =========================
    
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^admin_"))
    app.add_handler(CallbackQueryHandler(savings_plans, pattern="^back_to_menu$"))
    
    # =========================
    # MENU ROUTER
    # =========================
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))
    
    # =========================
    # START APPLICATION
    # =========================
    
    logger.info("✅ Bot is running. Press Ctrl+C to stop.")
    
    app.run_polling(allowed_updates=Update.ALL_TYPES)

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