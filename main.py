#!/usr/bin/env python3
"""
Pillar Digital Bank - Telegram Bot
Complete Production-Ready Version for Replit
"""

import os
import sys
import logging
import asyncio
import pytz
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any, List, Tuple
import secrets
import re

from telegram import (
    Update, 
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters
)
from telegram.constants import ParseMode

# ==================== CONFIGURATION ====================
class Config:
    """Application configuration for Replit"""
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID = int(os.environ.get("ADMIN_ID", "6730157589"))
    
    # NY Timezone settings
    TIMEZONE = pytz.timezone("America/New_York")
    
    # Banking hours (8:30 AM - 4:30 PM NY Time)
    BANK_OPEN_HOUR = 8
    BANK_OPEN_MINUTE = 30
    BANK_CLOSE_HOUR = 16
    BANK_CLOSE_MINUTE = 30
    
    # Daily interest calculation time (4:30 PM NY Time)
    INTEREST_HOUR = 16
    INTEREST_MINUTE = 30
    
    # Registration settings
    MIN_PASSWORD_LENGTH = 6
    MAX_PASSWORD_LENGTH = 20
    REFERRAL_CODE_LENGTH = 8
    
    # Registration bonus
    REGISTRATION_BONUS = Decimal('5.00')
    REFERRAL_BONUS = Decimal('1.00')
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        if not cls.BOT_TOKEN:
            raise ValueError("BOT_TOKEN is required in environment variables")
        return True

# ==================== IN-MEMORY DATABASE ====================
class DatabaseManager:
    """Simple in-memory database for Replit"""
    
    def __init__(self):
        self.users = {}  # telegram_id -> user_data
        self.accounts = {}  # telegram_id -> account_data
        self.transactions = []  # list of transactions
        self.savings_templates = {}  # savings plan templates
        self.savings_plans = {}  # plan_id -> savings_plan
        self.audit_logs = []  # audit trail
        
    def init_tables(self):
        """Initialize with default data"""
        # Seed savings plans
        self.savings_templates = {
            1: {
                'id': 1,
                'name': 'Basic',
                'description': '24-hour savings plan with daily interest',
                'duration_days': 1,
                'min_amount': Decimal('100.00'),
                'daily_interest_rate': Decimal('0.01'),  # 1%
                'is_locked': False,
                'is_active': True
            },
            2: {
                'id': 2,
                'name': 'Silver',
                'description': '7-day savings plan with locked principal',
                'duration_days': 7,
                'min_amount': Decimal('1000.00'),
                'daily_interest_rate': Decimal('0.012'),  # 1.2% daily = 8.4% total
                'is_locked': True,
                'is_active': True
            },
            3: {
                'id': 3,
                'name': 'Gold',
                'description': '15-day premium savings plan',
                'duration_days': 15,
                'min_amount': Decimal('5000.00'),
                'daily_interest_rate': Decimal('0.014'),  # 1.4% daily = 21% total
                'is_locked': True,
                'is_active': True
            },
            4: {
                'id': 4,
                'name': 'Platinum',
                'description': '30-day premium savings plan',
                'duration_days': 30,
                'min_amount': Decimal('10000.00'),
                'daily_interest_rate': Decimal('0.016'),  # 1.6% daily = 48% total
                'is_locked': True,
                'is_active': True
            },
            5: {
                'id': 5,
                'name': 'Diamond',
                'description': '90-day premium savings plan',
                'duration_days': 90,
                'min_amount': Decimal('25000.00'),
                'daily_interest_rate': Decimal('0.017'),  # 1.7% daily = 153% total
                'is_locked': True,
                'is_active': True
            }
        }
        print("✅ In-memory database initialized with savings plans")
        return True
    
    # ========== USER OPERATIONS ==========
    
    def create_user(self, telegram_id: int, full_name: str, phone_number: str, 
                   password_hash: str, referral_code: str = None) -> bool:
        """Create a new user"""
        user_id = str(telegram_id)
        if user_id in self.users:
            return False
        
        self.users[user_id] = {
            'telegram_id': telegram_id,
            'full_name': full_name,
            'phone_number': phone_number,
            'transaction_password': password_hash,
            'referral_code': referral_code or f"REF{telegram_id % 10000:04d}",
            'status': 'PENDING',
            'registration_bonus_given': False,
            'referral_bonus_given': False,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        return True
    
    def get_user(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Get user by Telegram ID"""
        return self.users.get(str(telegram_id))
    
    def update_user_status(self, telegram_id: int, status: str) -> bool:
        """Update user status"""
        user_id = str(telegram_id)
        if user_id in self.users:
            self.users[user_id]['status'] = status
            self.users[user_id]['updated_at'] = datetime.now()
            return True
        return False
    
    # ========== ACCOUNT OPERATIONS ==========
    
    def create_account(self, telegram_id: int) -> bool:
        """Create account for user"""
        user_id = str(telegram_id)
        if user_id in self.accounts:
            return False
        
        self.accounts[user_id] = {
            'user_telegram_id': telegram_id,
            'balance': Decimal('0.00'),
            'available_balance': Decimal('0.00'),
            'status': 'ACTIVE',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        return True
    
    def get_account(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Get account by user Telegram ID"""
        return self.accounts.get(str(telegram_id))
    
    def update_account_balance(self, telegram_id: int, amount: Decimal, is_deposit: bool = True) -> bool:
        """Update account balance"""
        user_id = str(telegram_id)
        if user_id not in self.accounts:
            return False
        
        account = self.accounts[user_id]
        if is_deposit:
            account['balance'] += amount
            account['available_balance'] += amount
        else:
            if account['available_balance'] >= amount:
                account['balance'] -= amount
                account['available_balance'] -= amount
            else:
                return False
        
        account['updated_at'] = datetime.now()
        return True
    
    # ========== TRANSACTION OPERATIONS ==========
    
    def create_transaction(self, telegram_id: int, tx_type: str, amount: Decimal, 
                          user_reference: str = None) -> Optional[str]:
        """Create a new transaction"""
        tx_id = f"TX{secrets.token_hex(4).upper()}"
        
        transaction = {
            'transaction_id': tx_id,
            'user_telegram_id': telegram_id,
            'type': tx_type,
            'amount': amount,
            'status': 'PENDING',
            'user_reference': user_reference,
            'requested_at': datetime.now()
        }
        self.transactions.append(transaction)
        return tx_id
    
    def get_pending_transactions(self, tx_type: str = None) -> List[Dict[str, Any]]:
        """Get pending transactions"""
        if tx_type:
            return [tx for tx in self.transactions if tx['status'] == 'PENDING' and tx['type'] == tx_type]
        return [tx for tx in self.transactions if tx['status'] == 'PENDING']
    
    def update_transaction_status(self, tx_id: str, status: str, admin_id: int = None, 
                                 note: str = None) -> bool:
        """Update transaction status"""
        for tx in self.transactions:
            if tx['transaction_id'] == tx_id and tx['status'] == 'PENDING':
                tx['status'] = status
                tx['reviewed_by'] = admin_id
                tx['admin_note'] = note
                tx['reviewed_at'] = datetime.now()
                if status == 'APPROVED':
                    tx['completed_at'] = datetime.now()
                return True
        return False
    
    # ========== SAVINGS PLAN OPERATIONS ==========
    
    def get_savings_templates(self) -> List[Dict[str, Any]]:
        """Get all active savings plan templates"""
        return [template for template in self.savings_templates.values() if template['is_active']]
    
    def create_savings_plan(self, telegram_id: int, template_id: int, plan_name: str,
                           principal_amount: Decimal, daily_rate: Decimal, 
                           duration_days: int, is_locked: bool) -> Optional[str]:
        """Create a user savings plan"""
        plan_id = f"SP{secrets.token_hex(4).upper()}"
        start_date = datetime.now(Config.TIMEZONE).date()
        end_date = start_date + timedelta(days=duration_days)
        
        plan = {
            'plan_id': plan_id,
            'user_telegram_id': telegram_id,
            'template_id': template_id,
            'plan_name': plan_name,
            'principal_amount': principal_amount,
            'daily_interest_rate': daily_rate,
            'total_interest_earned': Decimal('0.00'),
            'start_date': start_date,
            'end_date': end_date,
            'last_interest_calc': None,
            'status': 'ACTIVE',
            'is_locked': is_locked,
            'created_at': datetime.now()
        }
        self.savings_plans[plan_id] = plan
        return plan_id
    
    # ========== AUDIT LOGGING ==========
    
    def log_audit(self, action: str, actor: str, actor_id: int, description: str,
                 reference_id: int = None, old_value: str = None, new_value: str = None) -> bool:
        """Log audit entry"""
        self.audit_logs.append({
            'action': action,
            'actor': actor,
            'actor_id': actor_id,
            'reference_id': reference_id,
            'description': description,
            'old_value': old_value,
            'new_value': new_value,
            'timestamp': datetime.now()
        })
        return True

# ==================== SECURITY UTILS ====================
class SecurityUtils:
    """Security utilities"""
    
    @staticmethod
    def hash_password(password: str) -> str:
        """Simple password hashing"""
        import hashlib
        return hashlib.sha256(password.encode()).hexdigest()
    
    @staticmethod
    def validate_phone(phone: str) -> bool:
        """Validate phone number"""
        pattern = r'^\+?[1-9]\d{7,14}$'
        return bool(re.match(pattern, phone))
    
    @staticmethod
    def validate_name(name: str) -> bool:
        """Validate full name"""
        return 2 <= len(name.strip()) <= 100
    
    @staticmethod
    def validate_password(password: str) -> Tuple[bool, str]:
        """Validate password strength"""
        if len(password) < Config.MIN_PASSWORD_LENGTH:
            return False, f"Password must be at least {Config.MIN_PASSWORD_LENGTH} characters"
        if len(password) > Config.MAX_PASSWORD_LENGTH:
            return False, f"Password must be at most {Config.MAX_PASSWORD_LENGTH} characters"
        return True, ""

# ==================== BOT APPLICATION ====================
class PillarDigitalBankBot:
    """Main bot application"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.app = None
        
        # Setup logging
        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )
        self.logger = logging.getLogger(__name__)
        
        # Registration states
        self.REG_FULLNAME, self.REG_PHONE, self.REG_PASSWORD, self.REG_REFERRAL = range(4)
    
    # ==================== START COMMAND ====================
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user = update.effective_user
        telegram_id = user.id
        
        self.logger.info(f"User {telegram_id} started the bot")
        
        # Check if user is admin
        if telegram_id == Config.ADMIN_ID:
            await self._show_admin_panel(update, context)
            return
        
        # Check user status
        db_user = self.db.get_user(telegram_id)
        
        if not db_user:
            # New user - show welcome
            await self._welcome_new_user(update, context)
        elif db_user['status'] == 'PENDING':
            await self._handle_pending_user(update, context, db_user)
        elif db_user['status'] == 'APPROVED':
            await self._show_main_menu(update, context, db_user)
        elif db_user['status'] == 'REJECTED':
            await self._handle_rejected_user(update, context, db_user)
    
    async def _show_admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show admin control panel"""
        admin_text = (
            "🔐 **ADMIN CONTROL PANEL**\n\n"
            "*Available Commands:*\n"
            "• /dashboard - Admin dashboard\n"
            "• /pending - View pending requests\n"
            "• /users - View all users\n"
            "• /approve <user_id> - Approve user\n"
            "• /reject <user_id> - Reject user\n"
            "• /calc_interest - Calculate interest\n"
            "• /apply_interest - Apply interest\n"
            "• /complete_plans - Complete matured plans\n"
            "• /audit - View audit logs\n\n"
            "*Quick Stats:*\n"
        )
        
        # Get stats
        pending_users = 0
        for user in self.db.users.values():
            if user['status'] == 'PENDING':
                pending_users += 1
        
        admin_text += f"⏳ Pending Users: {pending_users}\n"
        
        await update.message.reply_text(
            admin_text,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def _welcome_new_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Welcome new user"""
        welcome_text = (
            "👋 **Welcome to Pillar Digital Bank!**\n\n"
            "Secure, simple, and smart savings starts here.\n\n"
            "📝 *Registration Process:*\n"
            "1. Provide your details\n"
            "2. Set a transaction password\n"
            "3. Wait for admin approval\n\n"
            "⚠️ *Important:*\n"
            "• This is a **Savings Platform**, NOT investment\n"
            "• All transactions require admin approval\n"
            "• Banking hours: 8:30 AM - 4:30 PM NY Time\n\n"
            "🚀 *Get Started:*\n"
            "Use /register to create your account"
        )
        
        keyboard = [[KeyboardButton("/register")]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def _handle_pending_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user: dict):
        """Handle users with PENDING status"""
        pending_text = (
            "⏳ **Account Pending Approval**\n\n"
            "Your registration is under review by our admin team.\n\n"
            "You will be notified once your account is approved.\n"
            "Expected review time: 24-48 hours.\n\n"
            "Thank you for your patience."
        )
        
        await update.message.reply_text(
            pending_text,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def _show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user: dict):
        """Show main menu for APPROVED users"""
        # Get or create account
        account = self.db.get_account(user['telegram_id'])
        if not account:
            self.db.create_account(user['telegram_id'])
            account = self.db.get_account(user['telegram_id'])
        
        menu_text = (
            "🏦 **Pillar Digital Bank**\n\n"
            f"Welcome back, {user['full_name']}!\n\n"
            "*Your Account:*\n"
            f"💰 Balance: ${account['balance']:.2f}\n"
            f"💳 Available: ${account['available_balance']:.2f}\n"
            f"✅ Status: {user['status']}\n\n"
            "*Available Commands:*\n"
            "• /savings - Savings plans\n"
            "• /deposit <amount> - Request deposit\n"
            "• /withdraw <amount> - Request withdrawal\n"
            "• /history - View transactions\n"
            "• /statement - Account statement\n"
            "• /support - Customer support"
        )
        
        keyboard = [
            ["💰 Savings Plans", "➕ Deposit"],
            ["➖ Withdraw", "📜 History"],
            ["📊 Statement", "📞 Support"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            menu_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def _handle_rejected_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user: dict):
        """Handle users with REJECTED status"""
        rejected_text = (
            "❌ **Registration Declined**\n\n"
            "Your account registration has been rejected.\n\n"
            "Please contact customer support for assistance:\n"
            "📞 @PillarSupport\n\n"
            "If you believe this is an error, provide your:\n"
            "• Full Name\n"
            "• Phone Number\n"
            "• Registration Date"
        )
        
        keyboard = [[KeyboardButton("📞 Contact Support")]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            rejected_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ==================== REGISTRATION FLOW ====================
    async def register(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start registration process"""
        user = update.effective_user
        telegram_id = user.id
        
        # Check if already registered
        if self.db.get_user(telegram_id):
            await update.message.reply_text(
                "⚠️ You already have an account. Use /start to access your account.",
                parse_mode=ParseMode.MARKDOWN
            )
            return ConversationHandler.END
        
        # Ask for full name
        await update.message.reply_text(
            "📝 *Registration Step 1/4: Full Name*\n\n"
            "Please enter your full legal name:\n"
            "• Use your official name as per ID\n"
            "• Minimum 2 characters\n\n"
            "Type /cancel to cancel registration.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return self.REG_FULLNAME
    
    async def reg_fullname(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process full name"""
        full_name = update.message.text.strip()
        
        # Validate name
        if not SecurityUtils.validate_name(full_name):
            await update.message.reply_text(
                "❌ Invalid name format.\n"
                "Please enter a valid full name (2-100 characters):\n\n"
                "Try again:",
                parse_mode=ParseMode.MARKDOWN
            )
            return self.REG_FULLNAME
        
        # Store in context
        context.user_data['full_name'] = full_name
        
        # Ask for phone number
        await update.message.reply_text(
            "📞 *Registration Step 2/4: Phone Number*\n\n"
            "Please enter your phone number:\n"
            "• Include country code (e.g., +959123456789)\n"
            "• Must be a valid phone number\n\n"
            "Type /cancel to cancel registration.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return self.REG_PHONE
    
    async def reg_phone(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process phone number"""
        phone_number = update.message.text.strip()
        
        # Validate phone
        if not SecurityUtils.validate_phone(phone_number):
            await update.message.reply_text(
                "❌ Invalid phone number format.\n"
                "Please enter a valid phone number with country code (e.g., +959123456789):\n\n"
                "Try again:",
                parse_mode=ParseMode.MARKDOWN
            )
            return self.REG_PHONE
        
        # Store in context
        context.user_data['phone_number'] = phone_number
        
        # Ask for transaction password
        await update.message.reply_text(
            "🔐 *Registration Step 3/4: Transaction Password*\n\n"
            "Create a secure password for transaction confirmation:\n"
            "• Minimum 6 characters\n"
            "• Maximum 20 characters\n"
            "• This password is required for deposits & withdrawals\n\n"
            "⚠️ *Important:* Store this password safely. It cannot be recovered.\n\n"
            "Enter your password:",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return self.REG_PASSWORD
    
    async def reg_password(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process password"""
        password = update.message.text.strip()
        
        # Validate password
        is_valid, error_msg = SecurityUtils.validate_password(password)
        if not is_valid:
            await update.message.reply_text(
                f"❌ {error_msg}\n\n"
                "Please enter a valid password (6-20 characters):",
                parse_mode=ParseMode.MARKDOWN
            )
            return self.REG_PASSWORD
        
        # Hash password
        hashed_password = SecurityUtils.hash_password(password)
        context.user_data['password_hash'] = hashed_password
        
        # Ask for referral code (optional)
        keyboard = [
            [InlineKeyboardButton("Skip Referral", callback_data="skip_referral")],
            [InlineKeyboardButton("Cancel Registration", callback_data="cancel_registration")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "👥 *Registration Step 4/4: Referral Code (Optional)*\n\n"
            "If you have a referral code, enter it now:\n"
            "• 8-character code (e.g., ABC123XY)\n"
            "• Or click 'Skip Referral' to continue\n\n"
            "Enter referral code or click button below:",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        
        return self.REG_REFERRAL
    
    async def reg_referral(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process referral code"""
        referral_code = update.message.text.strip().upper() if update.message.text else None
        
        # Get user data from context
        full_name = context.user_data.get('full_name')
        phone_number = context.user_data.get('phone_number')
        password_hash = context.user_data.get('password_hash')
        telegram_id = update.effective_user.id
        
        if not all([full_name, phone_number, password_hash]):
            await update.message.reply_text("❌ Registration data missing. Please start over.")
            return ConversationHandler.END
        
        # Create user
        success = self.db.create_user(
            telegram_id=telegram_id,
            full_name=full_name,
            phone_number=phone_number,
            password_hash=password_hash,
            referral_code=referral_code
        )
        
        if success:
            # Create account
            self.db.create_account(telegram_id)
            
            # Log audit
            self.db.log_audit(
                action='USER_REGISTERED',
                actor='USER',
                actor_id=telegram_id,
                description=f"New user registered: {full_name}",
                reference_id=telegram_id
            )
            
            await update.message.reply_text(
                "✅ *Registration Successful!*\n\n"
                "Your account has been created and is now *PENDING ADMIN APPROVAL*.\n\n"
                "⏳ *Next Steps:*\n"
                "1. Wait for admin approval (24-48 hours)\n"
                "2. You'll receive a notification when approved\n"
                "3. Once approved, use /start to access banking features\n\n"
                "📞 *Support:* @PillarSupport for questions.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                "❌ Registration failed. Please try again or contact support.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        # Clear context
        context.user_data.clear()
        return ConversationHandler.END
    
    async def reg_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle registration callbacks"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "skip_referral":
            # Skip referral and complete registration
            await query.edit_message_text("✅ Referral code skipped.")
            return await self.reg_referral(update, context)
        elif query.data == "cancel_registration":
            await query.edit_message_text("❌ Registration cancelled.")
            context.user_data.clear()
            return ConversationHandler.END
    
    async def cancel_registration(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel registration"""
        context.user_data.clear()
        await update.message.reply_text(
            "❌ Registration cancelled.\n\n"
            "Use /start to begin again.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationHandler.END
    
    # ==================== ADMIN COMMANDS ====================
    async def admin_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin dashboard"""
        if update.effective_user.id != Config.ADMIN_ID:
            await update.message.reply_text("❌ Unauthorized.")
            return
        
        # Get stats
        total_users = len(self.db.users)
        pending_users = 0
        approved_users = 0
        
        for user in self.db.users.values():
            if user['status'] == 'PENDING':
                pending_users += 1
            elif user['status'] == 'APPROVED':
                approved_users += 1
        
        dashboard_text = (
            "🔐 **Admin Dashboard**\n\n"
            f"📊 *User Statistics:*\n"
            f"👥 Total Users: {total_users}\n"
            f"⏳ Pending: {pending_users}\n"
            f"✅ Approved: {approved_users}\n\n"
            
            "📋 *Quick Actions:*\n"
            "Use buttons below to manage users."
        )
        
        # Get pending users (first 5)
        pending_list = [user for user in self.db.users.values() if user['status'] == 'PENDING'][:5]
        
        keyboard = []
        for user in pending_list:
            keyboard.append([
                InlineKeyboardButton(
                    f"✅ Approve {user['full_name'][:15]}...",
                    callback_data=f"approve_{user['telegram_id']}"
                ),
                InlineKeyboardButton(
                    f"❌ Reject",
                    callback_data=f"reject_{user['telegram_id']}"
                )
            ])
        
        if pending_list:
            keyboard.append([InlineKeyboardButton("📋 View All Pending", callback_data="view_all_pending")])
        
        keyboard.append([
            InlineKeyboardButton("💰 Calculate Interest", callback_data="calc_interest"),
            InlineKeyboardButton("📊 View Reports", callback_data="view_reports")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            dashboard_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin callbacks"""
        query = update.callback_query
        await query.answer()
        
        callback_data = query.data
        
        if callback_data.startswith("approve_"):
            user_id = int(callback_data.replace("approve_", ""))
            await self._approve_user(query, user_id)
        elif callback_data.startswith("reject_"):
            user_id = int(callback_data.replace("reject_", ""))
            await self._reject_user(query, user_id)
        elif callback_data == "calc_interest":
            await self._calculate_interest(query)
        elif callback_data == "view_reports":
            await query.edit_message_text(
                "📊 *Reports*\n\n"
                "Use commands:\n"
                "• /pending - Pending transactions\n"
                "• /users - All users\n"
                "• /audit - Audit logs\n"
                "• /stats - System statistics",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def _approve_user(self, query, user_id: int):
        """Approve user registration"""
        # Update user status
        success = self.db.update_user_status(user_id, 'APPROVED')
        
        if success:
            # Create account if not exists
            if not self.db.get_account(user_id):
                self.db.create_account(user_id)
            
            # Add registration bonus
            account = self.db.get_account(user_id)
            if account:
                self.db.update_account_balance(user_id, Config.REGISTRATION_BONUS, is_deposit=True)
            
            # Log audit
            self.db.log_audit(
                action='USER_APPROVED',
                actor='ADMIN',
                actor_id=Config.ADMIN_ID,
                description=f"User {user_id} approved by admin",
                reference_id=user_id
            )
            
            await query.edit_message_text(f"✅ User {user_id} approved successfully!")
            
            # In real implementation, would send message to user
            print(f"📢 User {user_id} has been approved!")
        else:
            await query.edit_message_text(f"❌ Failed to approve user {user_id}")
    
    async def _reject_user(self, query, user_id: int):
        """Reject user registration"""
        success = self.db.update_user_status(user_id, 'REJECTED')
        
        if success:
            # Log audit
            self.db.log_audit(
                action='USER_REJECTED',
                actor='ADMIN',
                actor_id=Config.ADMIN_ID,
                description=f"User {user_id} rejected by admin",
                reference_id=user_id
            )
            
            await query.edit_message_text(f"❌ User {user_id} rejected.")
        else:
            await query.edit_message_text(f"❌ Failed to reject user {user_id}")
    
    async def _calculate_interest(self, query):
        """Calculate daily interest"""
        try:
            # Get active savings plans
            active_plans = [plan for plan in self.db.savings_plans.values() if plan['status'] == 'ACTIVE']
            
            if not active_plans:
                await query.edit_message_text("✅ No active savings plans for interest calculation.")
                return
            
            total_interest = Decimal('0.00')
            today = datetime.now(Config.TIMEZONE).date()
            
            for plan in active_plans:
                # Check if plan is active today
                start_date = plan['start_date']
                end_date = plan['end_date']
                
                if start_date <= today <= end_date:
                    # Calculate daily interest
                    daily_interest = plan['principal_amount'] * plan['daily_interest_rate']
                    total_interest += daily_interest
                    
                    # Update plan interest
                    plan['total_interest_earned'] += daily_interest
                    plan['last_interest_calc'] = today
            
            # Log audit
            self.db.log_audit(
                action='INTEREST_CALCULATED',
                actor='SYSTEM',
                actor_id=0,
                description=f"Daily interest calculated: ${total_interest:.2f}",
                reference_id=None
            )
            
            await query.edit_message_text(
                f"✅ *Interest Calculation Complete*\n\n"
                f"📅 Date: {today}\n"
                f"📊 Active Plans: {len(active_plans)}\n"
                f"💰 Total Interest: ${total_interest:.2f}\n\n"
                f"💡 *Note:* Interest is logged and will be applied to balances.",
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            await query.edit_message_text(f"❌ Error calculating interest: {str(e)}")
    
    # ==================== BANKING COMMANDS ====================
    async def savings_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show savings menu"""
        user = update.effective_user
        telegram_id = user.id
        
        # Check if user is approved
        db_user = self.db.get_user(telegram_id)
        if not db_user or db_user['status'] != 'APPROVED':
            await update.message.reply_text("❌ Account not approved or not found.")
            return
        
        # Get savings templates
        templates = self.db.get_savings_templates()
        
        if not templates:
            await update.message.reply_text("❌ No savings plans available at the moment.")
            return
        
        # Build menu
        menu_text = "💰 *Savings Plans*\n\n"
        menu_text += "Choose a plan to start saving:\n\n"
        
        keyboard = []
        for template in templates:
            total_rate = template['daily_interest_rate'] * Decimal(str(template['duration_days'])) * Decimal('100')
            menu_text += (
                f"📋 *{template['name']}*\n"
                f"💎 Min: ${template['min_amount']:.2f}\n"
                f"📅 Duration: {template['duration_days']} days\n"
                f"📈 Daily: {template['daily_interest_rate'] * 100:.2f}%\n"
                f"🎁 Total: {total_rate:.2f}%\n"
                f"🔒 Locked: {'Yes' if template['is_locked'] else 'No'}\n"
                f"📝 {template['description']}\n"
                f"━━━━━━━━━━━━━━\n"
            )
            
            keyboard.append([
                InlineKeyboardButton(
                    f"{template['name']} - ${template['min_amount']:.0f}+",
                    callback_data=f"choose_plan_{template['id']}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            menu_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def deposit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle deposit request"""
        user = update.effective_user
        telegram_id = user.id
        
        # Check if user is approved
        db_user = self.db.get_user(telegram_id)
        if not db_user or db_user['status'] != 'APPROVED':
            await update.message.reply_text("❌ Account not approved or not found.")
            return
        
        # Check if amount provided
        if not context.args:
            await update.message.reply_text(
                "❌ Please specify amount.\n"
                "Usage: `/deposit <amount>`\n"
                "Example: `/deposit 100.50`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        try:
            amount = Decimal(context.args[0])
            if amount <= 0:
                await update.message.reply_text("❌ Amount must be greater than 0.")
                return
            
            # Create transaction
            tx_id = self.db.create_transaction(telegram_id, 'DEPOSIT', amount)
            
            if tx_id:
                # Log audit
                self.db.log_audit(
                    action='DEPOSIT_REQUESTED',
                    actor='USER',
                    actor_id=telegram_id,
                    description=f"Deposit request: ${amount:.2f}",
                    reference_id=None
                )
                
                await update.message.reply_text(
                    f"✅ *Deposit Request Submitted*\n\n"
                    f"💰 Amount: ${amount:.2f}\n"
                    f"📋 Transaction ID: `{tx_id}`\n\n"
                    f"⏳ *Status:* Pending Admin Approval\n"
                    f"⏰ *Processing:* 1-4 business hours\n\n"
                    f"📞 Contact support if you have questions.",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("❌ Failed to create deposit request.")
                
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
    async def withdraw(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle withdrawal request"""
        user = update.effective_user
        telegram_id = user.id
        
        # Check if user is approved
        db_user = self.db.get_user(telegram_id)
        if not db_user or db_user['status'] != 'APPROVED':
            await update.message.reply_text("❌ Account not approved or not found.")
            return
        
        # Check account
        account = self.db.get_account(telegram_id)
        if not account:
            await update.message.reply_text("❌ Account not found.")
            return
        
        # Check if amount provided
        if not context.args:
            await update.message.reply_text(
                "❌ Please specify amount.\n"
                "Usage: `/withdraw <amount>`\n"
                "Example: `/withdraw 50.00`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        try:
            amount = Decimal(context.args[0])
            if amount <= 0:
                await update.message.reply_text("❌ Amount must be greater than 0.")
                return
            
            # Check balance
            if account['available_balance'] < amount:
                await update.message.reply_text(
                    f"❌ Insufficient balance.\n"
                    f"Available: ${account['available_balance']:.2f}"
                )
                return
            
            # Create transaction
            tx_id = self.db.create_transaction(telegram_id, 'WITHDRAW', amount)
            
            if tx_id:
                # Log audit
                self.db.log_audit(
                    action='WITHDRAWAL_REQUESTED',
                    actor='USER',
                    actor_id=telegram_id,
                    description=f"Withdrawal request: ${amount:.2f}",
                    reference_id=None
                )
                
                await update.message.reply_text(
                    f"✅ *Withdrawal Request Submitted*\n\n"
                    f"💰 Amount: ${amount:.2f}\n"
                    f"📋 Transaction ID: `{tx_id}`\n"
                    f"💳 Available Balance: ${account['available_balance'] - amount:.2f}\n\n"
                    f"⏳ *Status:* Pending Admin Approval\n"
                    f"⏰ *Processing:* 2-6 business hours\n\n"
                    f"📞 Contact support if you have questions.",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("❌ Failed to create withdrawal request.")
                
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
    async def history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show transaction history"""
        user = update.effective_user
        telegram_id = user.id
        
        # Check if user is approved
        db_user = self.db.get_user(telegram_id)
        if not db_user or db_user['status'] != 'APPROVED':
            await update.message.reply_text("❌ Account not approved or not found.")
            return
        
        # Get transactions
        transactions = [tx for tx in self.db.transactions if tx['user_telegram_id'] == telegram_id][-10:]
        
        if not transactions:
            await update.message.reply_text("📭 No transactions found.")
            return
        
        history_text = "📜 *Transaction History*\n\n"
        
        for tx in transactions:
            icon = "➕" if tx['type'] == 'DEPOSIT' else "➖"
            status_icon = "✅" if tx['status'] == 'APPROVED' else "⏳" if tx['status'] == 'PENDING' else "❌"
            
            history_text += (
                f"{status_icon} *{tx['transaction_id']}*\n"
                f"{icon} {tx['type']}: ${tx['amount']:.2f}\n"
                f"📅 {tx['requested_at'].strftime('%Y-%m-%d %H:%M')}\n"
                f"📋 Status: {tx['status']}\n"
            )
            
            if tx.get('admin_note'):
                history_text += f"💬 Note: {tx['admin_note']}\n"
            
            history_text += "━━━━━━━━━━━━━━\n"
        
        await update.message.reply_text(
            history_text,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def statement(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show account statement"""
        user = update.effective_user
        telegram_id = user.id
        
        # Check if user is approved
        db_user = self.db.get_user(telegram_id)
        if not db_user or db_user['status'] != 'APPROVED':
            await update.message.reply_text("❌ Account not approved or not found.")
            return
        
        # Get account
        account = self.db.get_account(telegram_id)
        if not account:
            await update.message.reply_text("❌ Account not found.")
            return
        
        statement_text = (
            "📊 *Account Statement*\n\n"
            f"👤 *Account Holder:* {db_user['full_name']}\n"
            f"📱 *Phone:* {db_user['phone_number'][:3]}****\n"
            f"📅 *Statement Date:* {datetime.now(Config.TIMEZONE).strftime('%Y-%m-%d %I:%M %p')} NY\n\n"
            
            f"💰 *Balance Summary:*\n"
            f"• Total Balance: ${account['balance']:.2f}\n"
            f"• Available Balance: ${account['available_balance']:.2f}\n\n"
            
            f"📋 *Account Details:*\n"
            f"• Status: {db_user['status']}\n"
            f"• Account Created: {db_user['created_at'].strftime('%Y-%m-%d')}\n"
            f"• Referral Code: `{db_user.get('referral_code', 'N/A')}`\n\n"
            
            f"💡 *Banking Information:*\n"
            f"• Banking Hours: 8:30 AM - 4:30 PM NY Time\n"
            f"• Interest Calculation: 4:30 PM NY Time Daily\n"
            f"• All transactions require admin approval\n\n"
            
            f"📞 *Support:* @PillarSupport"
        )
        
        await update.message.reply_text(
            statement_text,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def support(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show support information"""
        support_text = (
            "📞 *Customer Support*\n\n"
            "*Contact Information:*\n"
            "• Support Bot: @PillarSupport\n"
            "• Email: support@pillarbank.com\n"
            "• Hours: 9 AM - 5 PM NY Time, Mon-Fri\n\n"
            
            "*Frequently Asked Questions:*\n"
            "❓ *How long does approval take?*\n"
            "   → 24-48 hours during business days\n\n"
            "❓ *When are banking hours?*\n"
            "   → 8:30 AM - 4:30 PM NY Time\n\n"
            "❓ *How do I reset my password?*\n"
            "   → Contact support with your account details\n\n"
            "❓ *Can I withdraw early from savings?*\n"
            "   → Locked plans cannot be withdrawn early\n\n"
            
            "*Important Security Notes:*\n"
            "• Never share your transaction password\n"
            "• We will NEVER ask for your password\n"
            "• All transactions require admin approval\n"
            "• Report suspicious activity immediately\n\n"
            
            "*About Pillar Digital Bank:*\n"
            "We are a secure, manual-approval based digital savings platform. "
            "All funds are controlled by administrators with full audit trails. "
            "This is a savings platform, NOT an investment platform."
        )
        
        keyboard = [[KeyboardButton("📞 Contact Support")]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            support_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ==================== INTEREST SCHEDULER ====================
    async def schedule_interest_calculation(self):
        """Schedule daily interest calculation at 4:30 PM NY Time"""
        while True:
            now_ny = datetime.now(Config.TIMEZONE)
            
            # Check if it's 4:30 PM NY Time
            if now_ny.hour == Config.INTEREST_HOUR and now_ny.minute == Config.INTEREST_MINUTE:
                self.logger.info("🕒 Running scheduled interest calculation...")
                
                try:
                    # Calculate interest for all active plans
                    active_plans = [plan for plan in self.db.savings_plans.values() if plan['status'] == 'ACTIVE']
                    
                    total_interest = Decimal('0.00')
                    today = now_ny.date()
                    
                    for plan in active_plans:
                        # Check if plan is active today
                        start_date = plan['start_date']
                        end_date = plan['end_date']
                        
                        if start_date <= today <= end_date:
                            # Calculate daily interest
                            daily_interest = plan['principal_amount'] * plan['daily_interest_rate']
                            total_interest += daily_interest
                            
                            # Update plan interest
                            plan['total_interest_earned'] += daily_interest
                            plan['last_interest_calc'] = today
                    
                    # Log audit
                    self.db.log_audit(
                        action='SCHEDULED_INTEREST_CALCULATED',
                        actor='SYSTEM',
                        actor_id=0,
                        description=f"Scheduled interest calculation: ${total_interest:.2f}",
                        reference_id=None
                    )
                    
                    self.logger.info(f"✅ Interest calculated: ${total_interest:.2f} for {len(active_plans)} plans")
                    
                except Exception as e:
                    self.logger.error(f"❌ Error in scheduled interest calculation: {e}")
                
                # Sleep for 24 hours minus a few seconds
                await asyncio.sleep(86400 - 60)
            
            # Sleep for 1 minute and check again
            await asyncio.sleep(60)
    
    # ==================== SETUP AND RUN ====================
    def setup_handlers(self):
        """Setup bot command handlers"""
        
        # Registration conversation handler
        reg_conversation = ConversationHandler(
            entry_points=[CommandHandler("register", self.register)],
            states={
                self.REG_FULLNAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.reg_fullname)
                ],
                self.REG_PHONE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.reg_phone)
                ],
                self.REG_PASSWORD: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.reg_password)
                ],
                self.REG_REFERRAL: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.reg_referral),
                    CallbackQueryHandler(self.reg_callback, pattern="^(skip_referral|cancel_registration)$")
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_registration)],
            allow_reentry=False
        )
        
        # Add handlers
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(reg_conversation)
        
        # Admin handlers
        self.app.add_handler(CommandHandler("dashboard", self.admin_dashboard))
        self.app.add_handler(CallbackQueryHandler(self.admin_callback, pattern="^(approve_|reject_|calc_interest|view_reports|view_all_pending)$"))
        
        # Banking handlers
        self.app.add_handler(CommandHandler("savings", self.savings_menu))
        self.app.add_handler(CommandHandler("deposit", self.deposit))
        self.app.add_handler(CommandHandler("withdraw", self.withdraw))
        self.app.add_handler(CommandHandler("history", self.history))
        self.app.add_handler(CommandHandler("statement", self.statement))
        self.app.add_handler(CommandHandler("support", self.support))
        
        # Callback handlers
        self.app.add_handler(CallbackQueryHandler(self.savings_menu, pattern="^back_to_main$"))
    
    async def run(self):
        """Run the bot application"""
        try:
            # Validate configuration
            Config.validate()
            
            # Initialize database
            self.db.init_tables()
            
            # Create application
            self.app = Application.builder().token(Config.BOT_TOKEN).build()
            
            # Setup handlers
            self.setup_handlers()
            
            # Start interest scheduler
            asyncio.create_task(self.schedule_interest_calculation())
            
            # Print startup message
            print("=" * 60)
            print("🏦 Pillar Digital Bank Bot")
            print("=" * 60)
            print(f"🤖 Bot Token: {'✓' if Config.BOT_TOKEN else '✗'}")
            print(f"🔐 Admin ID: {Config.ADMIN_ID}")
            print(f"🗄️ Storage: In-Memory Database")
            print(f"⏰ Timezone: {Config.TIMEZONE}")
            print(f"🕐 Banking Hours: 8:30 AM - 4:30 PM NY Time")
            print(f"💰 Interest Calculation: 4:30 PM NY Time Daily")
            print("=" * 60)
            print("✅ Bot is running. Press Ctrl+C to stop.")
            print("=" * 60)
            
            # Start the bot
            await self.app.run_polling(allowed_updates=None)
            
        except ValueError as e:
            print(f"❌ Configuration Error: {e}")
            print("Please set the following environment variables:")
            print("• BOT_TOKEN - Your Telegram Bot Token")
            print("• ADMIN_ID - Your Telegram User ID (default: 6730157589)")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Unexpected Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

# ==================== MAIN ENTRY POINT ====================
def main():
    """Main entry point"""
    bot = PillarDigitalBankBot()
    
    # Run the bot
   import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(bot.run())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()