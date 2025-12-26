import logging
import sqlite3
import hashlib
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import asyncio
from datetime import datetime
import sys
import subprocess

# Принудительно устанавливаем aiogram при запуске
try:
    from aiogram import Bot, Dispatcher, types
except ImportError:
    print("Устанавливаю aiogram...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "aiogram==3.17.0"])
    from aiogram import Bot, Dispatcher, types

# Остальной код бота...

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Токен вашего бота
API_TOKEN = '7954519727:AAFzwqlnPvn_kyMS-FvseTz5G6gGB_jxssQ'

# ID администраторов (добавьте свои ID)
ADMIN_IDS = [6918105685]  # Замените на свой реальный ID!

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Состояния FSM
class SendMessageState(StatesGroup):
    waiting_for_link = State()
    waiting_for_message = State()
    confirm_message = State()

class AdminState(StatesGroup):
    waiting_broadcast_message = State()
    waiting_user_id = State()

# Подключение к базе данных
conn = sqlite3.connect('anonymous_bot.db', check_same_thread=False)
cursor = conn.cursor()

# Функция для обновления структуры базы данных
def update_database_structure():
    """Обновляет структуру базы данных, добавляя недостающие колонки"""
    try:
        # Проверяем существующие колонки в таблице users
        cursor.execute("PRAGMA table_info(users)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        # Добавляем недостающие колонки
        if 'is_blocked' not in column_names:
            cursor.execute("ALTER TABLE users ADD COLUMN is_blocked BOOLEAN DEFAULT 0")
            logger.info("Добавлена колонка is_blocked в таблицу users")
        
        if 'last_activity' not in column_names:
            cursor.execute("ALTER TABLE users ADD COLUMN last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            logger.info("Добавлена колонка last_activity в таблицу users")
        
        # Проверяем существование таблицы admin_logs
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='admin_logs'")
        if not cursor.fetchone():
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            logger.info("Создана таблица admin_logs")
        
        conn.commit()
        logger.info("Структура базы данных обновлена")
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении структуры базы данных: {e}")

# Создание/обновление таблиц
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    anon_link TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_blocked BOOLEAN DEFAULT 0,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS messages (
    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id INTEGER,
    receiver_id INTEGER,
    message_text TEXT,
    is_anonymous BOOLEAN DEFAULT 1,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sender_id) REFERENCES users (user_id),
    FOREIGN KEY (receiver_id) REFERENCES users (user_id)
)
''')

# Обновляем структуру базы данных
update_database_structure()

conn.commit()

# Статистика (для демонстрации)
stats = {
    "users": 67582,
    "chat_messages": 38900
}

# Проверка является ли пользователь администратором
def is_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором"""
    return user_id in ADMIN_IDS

# Логирование действий админа
async def log_admin_action(admin_id: int, action: str, details: str = ""):
    try:
        cursor.execute('''
        INSERT INTO admin_logs (admin_id, action, details)
        VALUES (?, ?, ?)
        ''', (admin_id, action, details))
        conn.commit()
    except Exception as e:
        logger.error(f"Error logging admin action: {e}")

# Генерация анонимной ссылки
def generate_anon_link(user_id):
    return hashlib.md5(str(user_id).encode()).hexdigest()[:10]

# ================== КОМАНДА /START ==================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    
    try:
        # Проверяем, заблокирован ли пользователь
        cursor.execute('SELECT is_blocked FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        
        if user and user[0] == 1:
            await message.answer("⛔ К сожалению у вас заблокирован доступ к этому боту, обратитесь в Тех.Поддержку @poverty2221.")
            return
    except sqlite3.OperationalError as e:
        # Если возникает ошибка колонки, игнорируем проверку блокировки
        logger.warning(f"Ошибка проверки блокировки: {e}. Пропускаем проверку.")
    
    # Проверяем, есть ли пользователь в базе
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    
    if not user:
        anon_link = generate_anon_link(user_id)
        cursor.execute('''
        INSERT INTO users (user_id, username, first_name, last_name, anon_link, last_activity)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, username, first_name, last_name, anon_link, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
    else:
        anon_link = user[4]
        try:
            cursor.execute('UPDATE users SET last_activity = ? WHERE user_id = ?', 
                         (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user_id))
            conn.commit()
        except:
            pass
    
    # Проверяем, если есть параметр в команде start (кто-то перешел по ссылке)
    args = message.text.split()
    if len(args) > 1:
        link_code = args[1]
        await handle_anonymous_link(message, link_code, user_id)
        return
    
    welcome_text = f"""
🎭 *Добро пожаловать в анонимные сообщения!*

*Статистика:*
👥 Пользователей: {stats['users']:,}
💬 Сообщений в чате: {stats['chat_messages']:,}

*Ваша персональная ссылка для получения анонимных сообщений:*
`https://t.me/{(await bot.get_me()).username}?start={anon_link}`

*Как это работает:*
1. Отправьте свою ссылку друзьям
2. Они могут написать вам анонимно
3. Вы получите сообщения без имени отправителя

*Доступные команды:*
/start - Начало работы
/link - Получить вашу ссылку
/send - Отправить анонимное сообщение
/stop - Остановить получение сообщений
/stats - Статистика бота
    """
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="📨 Получить ссылку", callback_data="get_link"),
        InlineKeyboardButton(text="✉️ Отправить сообщение", callback_data="send_message")
    )
    keyboard.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
        InlineKeyboardButton(text="⏹️ Остановить", callback_data="stop_bot")
    )
    
    # Добавляем кнопку админ-панели для администраторов
    if is_admin(user_id):
        welcome_text += "\n👑 /admin - Админ-панель"
        keyboard.row(InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel"))
    
    await message.answer(welcome_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())

# ================== ОБРАБОТКА ПЕРЕХОДА ПО АНОНИМНОЙ ССЫЛКЕ ==================
async def handle_anonymous_link(message: types.Message, link_code: str, sender_id: int):
    # Находим получателя по коду ссылки
    cursor.execute('SELECT user_id, first_name FROM users WHERE anon_link = ?', (link_code,))
    receiver = cursor.fetchone()
    
    if receiver:
        receiver_id = receiver[0]
        receiver_name = receiver[1] or "пользователь"
        
        # Создаем клавиатуру для быстрой отправки сообщения
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="✉️ Написать сообщение", callback_data=f"quick_send_{link_code}")
        )
        
        await message.answer(
            f"🔗 Вы перешли по ссылке к *{receiver_name}*!\n\n"
            f"Теперь вы можете отправить анонимное сообщение этому пользователю.\n\n"
            f"Используйте кнопку ниже или команду /send",
            parse_mode="Markdown",
            reply_markup=keyboard.as_markup()
        )
    else:
        await message.answer("❌ Ссылка недействительна или пользователь не найден.")

# ================== КОМАНДА /LINK ==================
@dp.message(Command("link"))
async def cmd_link(message: types.Message):
    user_id = message.from_user.id
    cursor.execute('SELECT anon_link FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    
    if result:
        anon_link = result[0]
        bot_username = (await bot.get_me()).username
        link_text = f"""
*Ваша ссылка для получения анонимных сообщений:*

`https://t.me/{bot_username}?start={anon_link}`

Поделитесь этой ссылкой с друзьями, чтобы они могли отправлять вам анонимные сообщения!
        """
        await message.answer(link_text, parse_mode="Markdown")
    else:
        await message.answer("Сначала зарегистрируйтесь с помощью /start")

# ================== КОМАНДА /SEND ==================
@dp.message(Command("send"))
async def cmd_send(message: types.Message, state: FSMContext):
    await message.answer(
        "✉️ *Отправить анонимное сообщение*\n\n"
        "Для отправки сообщения вам нужно:\n\n"
        "1. Получить ссылку пользователя через команду /link\n"
        "2. Использовать команду /send и отправить ссылку\n"
        "3. Написать текст сообщения\n\n"
        "*Пример ссылки:*\n"
        "`https://t.me/AnonymousPoverty2221_bot?start=abc123def`\n\n"
        "Введите ссылку пользователя:",
        parse_mode="Markdown"
    )
    
    # Устанавливаем состояние ожидания ссылки
    await state.set_state(SendMessageState.waiting_for_link)

# ================== ОБРАБОТКА ССЫЛКИ ==================
@dp.message(SendMessageState.waiting_for_link)
async def process_link_step(message: types.Message, state: FSMContext):
    link = message.text.strip()
    
    # Извлекаем anon_link из URL
    try:
        if '?start=' in link:
            anon_link = link.split('?start=')[1].split()[0]  # Берем только код
        else:
            await message.answer("❌ Неверный формат ссылки. Ссылка должна содержать ?start=\n\nПопробуйте еще раз или нажмите /cancel")
            return
        
        # Проверяем существование пользователя
        cursor.execute('SELECT user_id, first_name FROM users WHERE anon_link = ?', (anon_link,))
        receiver = cursor.fetchone()
        
        if not receiver:
            await message.answer("❌ Пользователь не найден. Проверьте правильность ссылки.\n\nПопробуйте еще раз или нажмите /cancel")
            return
        
        # Сохраняем данные в состоянии
        await state.update_data(
            receiver_link=anon_link,
            receiver_id=receiver[0],
            receiver_name=receiver[1] or "пользователю"
        )
        
        # Переходим к следующему шагу
        await message.answer(
            f"✅ Отлично! Ссылка принята.\n\n"
            f"Теперь напишите текст сообщения для *{receiver[1] or 'пользователя'}*.\n\n"
            f"Сообщение будет отправлено *анонимно*.\n\n"
            f"*Максимальная длина:* 1000 символов\n"
            f"Или нажмите /cancel для отмена",
            parse_mode="Markdown"
        )
        
        await state.set_state(SendMessageState.waiting_for_message)
        
    except Exception as e:
        logger.error(f"Error processing link: {e}")
        await message.answer("❌ Произошла ошибка. Проверьте формат ссылки и попробуйте еще раз.\n\nИли нажмите /cancel")

# ================== ОБРАБОТКА ТЕКСТА СООБЩЕНИЯ ==================
@dp.message(SendMessageState.waiting_for_message)
async def process_message_step(message: types.Message, state: FSMContext):
    message_text = message.text.strip()
    
    if len(message_text) > 1000:
        await message.answer("❌ Сообщение слишком длинное (максимум 1000 символов).\n\nПожалуйста, сократите сообщение и отправьте снова.")
        return
    
    if len(message_text) < 1:
        await message.answer("❌ Сообщение не может быть пустым.\n\nПожалуйста, напишите текст сообщения.")
        return
    
    # Сохраняем сообщение в состоянии
    await state.update_data(message_text=message_text)
    
    # Получаем данные из состояния
    data = await state.get_data()
    receiver_name = data.get('receiver_name', 'пользователю')
    
    # Показываем предпросмотр
    preview_text = f"""
📝 *Подтверждение отправки*

*Получатель:* {receiver_name}
*Сообщение будет отправлено анонимно*

*Ваше сообщение:*
{message_text}

*Длина:* {len(message_text)} символов
    """
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="✅ Отправить", callback_data="confirm_send"),
        InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_send")
    )
    
    await message.answer(preview_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())
    await state.set_state(SendMessageState.confirm_message)

# ================== ПОДТВЕРЖДЕНИЕ ОТПРАВКИ ==================
@dp.callback_query(F.data == "confirm_send")
async def confirm_send_message(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer()
    
    # Получаем данные из состояния
    data = await state.get_data()
    receiver_id = data.get('receiver_id')
    receiver_name = data.get('receiver_name', 'пользователю')
    message_text = data.get('message_text')
    sender_id = callback_query.from_user.id
    
    if not all([receiver_id, message_text]):
        await callback_query.message.answer("❌ Ошибка: данные не найдены. Пожалуйста, начните заново.")
        await state.clear()
        return
    
    try:
        # Сохраняем сообщение в БД
        cursor.execute('''
        INSERT INTO messages (sender_id, receiver_id, message_text, is_anonymous)
        VALUES (?, ?, ?, ?)
        ''', (sender_id, receiver_id, message_text, 1))
        conn.commit()
        
        # Отправляем получателю
        await bot.send_message(
            receiver_id,
            f"📨 *Новое анонимное сообщение:*\n\n{message_text}\n\n"
            f"_💌 Чтобы ответить анонимно, поделитесь своей ссылкой из /link_",
            parse_mode="Markdown"
        )
        
        # Уведомляем отправителя
        success_text = f"""
✅ *Сообщение успешно отправлено!*

Сообщение было отправлено *{receiver_name}* анонимно.

Отправитель не будет знать, кто вы.
Вы можете отправить еще одно сообщение с помощью /send
        """
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="✉️ Отправить еще", callback_data="send_message")
        )
        
        await callback_query.message.answer(success_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())
        
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        await callback_query.message.answer("❌ Произошла ошибка при отправке сообщения. Пожалуйста, попробуйте позже.")
    
    # Очищаем состояние
    await state.clear()

# ================== ОТМЕНА ОТПРАВКИ ==================
@dp.callback_query(F.data == "cancel_send")
async def cancel_send_message(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer("Отправка отменена")
    await callback_query.message.answer("❌ Отправка сообщения отменена.\n\nЧтобы начать заново, используйте /send")
    await state.clear()

# ================== БЫСТРАЯ ОТПРАВКА ПО ССЫЛКЕ ==================
@dp.callback_query(F.data.startswith("quick_send_"))
async def quick_send_message(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer()
    
    link_code = callback_query.data.replace("quick_send_", "")
    
    # Находим получателя
    cursor.execute('SELECT user_id, first_name FROM users WHERE anon_link = ?', (link_code,))
    receiver = cursor.fetchone()
    
    if receiver:
        await state.update_data(
            receiver_link=link_code,
            receiver_id=receiver[0],
            receiver_name=receiver[1] or "пользователю"
        )
        
        await callback_query.message.answer(
            f"✉️ *Быстрая отправка*\n\n"
            f"Вы собираетесь отправить сообщение *{receiver[1] or 'пользователю'}*.\n\n"
            f"Пожалуйста, напишите текст сообщения (максимум 1000 символов):\n\n"
            f"Или нажмите /cancel для отмены",
            parse_mode="Markdown"
        )
        
        await state.set_state(SendMessageState.waiting_for_message)
    else:
        await callback_query.message.answer("❌ Пользователь не найден. Используйте /send для отправки сообщения.")

# ================== КОМАНДА /CANCEL ==================
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return
    
    await state.clear()
    await message.answer("❌ Действие отменено.")

# ================== КОМАНДА /STATS ==================
@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    # Получаем реальную статистику из БД
    cursor.execute('SELECT COUNT(*) FROM users')
    real_users = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM messages')
    real_messages = cursor.fetchone()[0]
    
    stats_text = f"""
📊 *Статистика бота:*

*Общая статистика:*
👥 Пользователей: {stats['users']:,}
💬 Сообщений в чате: {stats['chat_messages']:,}

*Реальная статистика:*
👤 Зарегистрировано: {real_users}
✉️ Отправлено сообщений: {real_messages}

*Последние действия:*
Анонимные сообщения активно отправляются!
    """
    
    await message.answer(stats_text, parse_mode="Markdown")

# ================== КОМАНДА /STOP ==================
@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    keyboard = InlineKeyboardBuilder()
    keyboard.add(InlineKeyboardButton(text="⏹️ Остановить бота", callback_data="stop_bot"))
    
    await message.answer(
        "⚠️ *Остановка бота*\n\n"
        "Вы уверены, что хотите остановить бота? Вы перестанете получать анонимные сообщения.",
        parse_mode="Markdown",
        reply_markup=keyboard.as_markup()
    )

# ================== ОБРАБОТЧИКИ CALLBACK КНОПОК ==================
@dp.callback_query(F.data == "send_message")
async def send_message_callback(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer()
    await cmd_send(callback_query.message, state)

@dp.callback_query(F.data.in_(["get_link", "stats", "stop_bot"]))
async def process_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    
    if data == "get_link":
        cursor.execute('SELECT anon_link FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result:
            anon_link = result[0]
            bot_username = (await bot.get_me()).username
            await callback_query.answer("Ваша ссылка скопирована в чат!")
            await bot.send_message(
                user_id,
                f"🔗 *Ваша анонимная ссылка:*\n\n`https://t.me/{bot_username}?start={anon_link}`\n\n"
                f"Поделитесь этой ссылкой, чтобы получать анонимные сообщения!",
                parse_mode="Markdown"
            )
        else:
            await callback_query.answer("Сначала используйте /start")
    
    elif data == "stats":
        await callback_query.answer()
        await cmd_stats(callback_query.message)
    
    elif data == "stop_bot":
        await callback_query.answer("Бот остановлен!", show_alert=True)
        await bot.send_message(
            user_id,
            "⏹️ *Бот остановлен*\n\n"
            "Вы больше не будете получать анонимные сообщения. "
            "Для возобновления работы отправьте /start",
            parse_mode="Markdown"
        )

# ================== АДМИН-ПАНЕЛЬ: КОМАНДА /ADMIN ==================
@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    user_id = message.from_user.id
    
    # Проверяем права администратора
    if not is_admin(user_id):
        await message.answer("⛔ У вас нет доступа к админ-панели.")
        return
    
    admin_text = """
👑 *Админ-панель*

*Доступные функции:*
1. 📊 Просмотр статистики
2. 👥 Управление пользователями
3. 📢 Рассылка сообщений
4. 📝 Просмотр логов

Выберите действие:
    """
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
        InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")
    )
    keyboard.row(
        InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
        InlineKeyboardButton(text="📝 Логи", callback_data="admin_logs")
    )
    keyboard.row(
        InlineKeyboardButton(text="🔄 Обновить данные", callback_data="admin_refresh"),
        InlineKeyboardButton(text="🚪 Выйти", callback_data="admin_exit")
    )
    
    await message.answer(admin_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())
    await log_admin_action(user_id, "open_admin_panel")

# ================== АДМИН-ПАНЕЛЬ: СТАТИСТИКА ==================
@dp.callback_query(F.data == "admin_stats")
async def admin_stats_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer()
    
    try:
        # Получаем реальную статистику из БД
        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE is_blocked = 1')
        blocked_users = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(*) FROM messages')
        total_messages = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(DISTINCT sender_id) FROM messages')
        active_senders = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(DISTINCT receiver_id) FROM messages')
        active_receivers = cursor.fetchone()[0] or 0
        
        # Получаем дату последнего сообщения
        cursor.execute('SELECT MAX(sent_at) FROM messages')
        last_message_date = cursor.fetchone()[0] or "Нет сообщений"
        
        stats_text = f"""
📊 *Статистика администратора*

*Пользователи:*
👥 Всего пользователей: {total_users}
⛔ Заблокировано: {blocked_users}
✅ Активных: {total_users - blocked_users}

*Сообщения:*
✉️ Всего отправлено: {total_messages}
📤 Отправителей: {active_senders}
📥 Получателей: {active_receivers}
🕒 Последнее сообщение: {last_message_date}

*Система:*
💾 Размер БД: ~{total_users * 0.1:.1f} KB
🔄 Последнее обновление: {datetime.now().strftime('%H:%M:%S')}
        """
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        stats_text = f"""
📊 *Статистика администратора*

*Ошибка при получении данных: {e}*

Попробуйте обновить данные или проверьте структуру базы.
        """
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_stats"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")
    )
    
    await callback_query.message.edit_text(stats_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())
    await log_admin_action(user_id, "view_stats")

# ================== АДМИН-ПАНЕЛЬ: ПОЛЬЗОВАТЕЛИ ==================
@dp.callback_query(F.data == "admin_users")
async def admin_users_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer()
    
    try:
        # Получаем последних 5 пользователей
        cursor.execute('''
        SELECT user_id, username, first_name, last_name, is_blocked, created_at 
        FROM users 
        ORDER BY created_at DESC 
        LIMIT 5
        ''')
        users = cursor.fetchall()
        
        users_text = "👥 *Последние пользователи*\n\n"
        
        for user in users:
            user_id_col = user[0]
            username = f"@{user[1]}" if user[1] else "Нет username"
            first_name = user[2] or ""
            last_name = user[3] or ""
            status = "⛔ Заблокирован" if user[4] else "✅ Активен"
            created = user[5]
            
            users_text += f"• *ID:* `{user_id_col}`\n"
            users_text += f"  *Имя:* {first_name} {last_name}\n"
            users_text += f"  *Username:* {username}\n"
            users_text += f"  *Статус:* {status}\n"
            users_text += f"  *Зарегистрирован:* {created}\n\n"
        
        users_text += f"\n_Всего пользователей: {len(users)} из 5 показано_"
    except Exception as e:
        logger.error(f"Ошибка при получении пользователей: {e}")
        users_text = f"❌ *Ошибка при получении пользователей:* {e}"
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="admin_search_user"),
    )
    keyboard.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_users"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")
    )
    
    await callback_query.message.edit_text(users_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())
    await log_admin_action(user_id, "view_users")

# ================== АДМИН-ПАНЕЛЬ: ПОИСК ПОЛЬЗОВАТЕЛЯ ==================
@dp.callback_query(F.data == "admin_search_user")
async def admin_search_user_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer()
    
    await callback_query.message.edit_text(
        "🔍 *Поиск пользователя*\n\n"
        "Введите ID пользователя для поиска:\n\n"
        "_Используйте /cancel для отмены_",
        parse_mode="Markdown"
    )
    
    await state.set_state(AdminState.waiting_user_id)
    await log_admin_action(user_id, "search_user_started")

# ================== АДМИН-ПАНЕЛЬ: БЛОКИРОВКА ПОЛЬЗОВАТЕЛЯ ==================
@dp.callback_query(F.data.startswith("admin_block_"))
async def admin_block_user_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    target_user_id = int(callback_query.data.replace("admin_block_", ""))
    
    try:
        cursor.execute('UPDATE users SET is_blocked = 1 WHERE user_id = ?', (target_user_id,))
        conn.commit()
        
        await callback_query.answer("✅ Пользователь заблокирован")
        
        # Обновляем сообщение
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="✅ Разблокировать", callback_data=f"admin_unblock_{target_user_id}"),
            InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")
        )
        
        await callback_query.message.edit_text(
            f"✅ *Пользователь `{target_user_id}` заблокирован*\n\n"
            f"Теперь он не сможет использовать бота.",
            parse_mode="Markdown",
            reply_markup=keyboard.as_markup()
        )
        
        await log_admin_action(user_id, "block_user", f"User ID: {target_user_id}")
        
    except Exception as e:
        logger.error(f"Error blocking user: {e}")
        await callback_query.answer("❌ Ошибка при блокировке")

# ================== АДМИН-ПАНЕЛЬ: РАЗБЛОКИРОВКА ПОЛЬЗОВАТЕЛЯ ==================
@dp.callback_query(F.data.startswith("admin_unblock_"))
async def admin_unblock_user_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    target_user_id = int(callback_query.data.replace("admin_unblock_", ""))
    
    try:
        cursor.execute('UPDATE users SET is_blocked = 0 WHERE user_id = ?', (target_user_id,))
        conn.commit()
        
        await callback_query.answer("✅ Пользователь разблокирован")
        
        # Обновляем сообщение
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="⛔ Заблокировать", callback_data=f"admin_block_{target_user_id}"),
            InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")
        )
        
        await callback_query.message.edit_text(
            f"✅ *Пользователь `{target_user_id}` разблокирован*\n\n"
            f"Теперь он снова может использовать бота.",
            parse_mode="Markdown",
            reply_markup=keyboard.as_markup()
        )
        
        await log_admin_action(user_id, "unblock_user", f"User ID: {target_user_id}")
        
    except Exception as e:
        logger.error(f"Error unblocking user: {e}")
        await callback_query.answer("❌ Ошибка при разблокировке")

# ================== АДМИН-ПАНЕЛЬ: РАССЫЛКА ==================
@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer()
    
    # Получаем количество активных пользователей
    try:
        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0] or 0
    except:
        total_users = 0
    
    await callback_query.message.edit_text(
        f"📢 *Рассылка сообщений*\n\n"
        f"Всего пользователей: {total_users}\n\n"
        f"Введите сообщение для рассылки:\n\n"
        f"_Используйте /cancel для отмены_",
        parse_mode="Markdown"
    )
    
    await state.set_state(AdminState.waiting_broadcast_message)
    await log_admin_action(user_id, "broadcast_started")

# ================== АДМИН-ПАНЕЛЬ: ЛОГИ ==================
@dp.callback_query(F.data == "admin_logs")
async def admin_logs_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer()
    
    try:
        # Проверяем существование таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='admin_logs'")
        if not cursor.fetchone():
            await callback_query.message.edit_text(
                "📝 *Логи*\n\n"
                "Таблица логов еще не создана.\n"
                "Действия администраторов начнут записываться после первого использования админ-панели.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardBuilder()
                    .add(InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel"))
                    .as_markup()
            )
            return
        
        # Получаем последние 10 логов
        cursor.execute('''
        SELECT admin_id, action, details, created_at 
        FROM admin_logs 
        ORDER BY created_at DESC 
        LIMIT 10
        ''')
        logs = cursor.fetchall()
        
        logs_text = "📝 *Последние действия администраторов*\n\n"
        
        if not logs:
            logs_text += "_Логов пока нет_"
        else:
            for log in logs:
                admin_id = log[0]
                action = log[1]
                details = log[2] or ""
                time = log[3]
                
                logs_text += f"• *{time}*\n"
                logs_text += f"  *Admin:* `{admin_id}`\n"
                logs_text += f"  *Действие:* {action}\n"
                if details:
                    logs_text += f"  *Детали:* {details[:50]}...\n"
                logs_text += "\n"
    except Exception as e:
        logs_text = f"❌ *Ошибка при получении логов:* {e}"
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🗑️ Очистить логи", callback_data="admin_clear_logs"),
        InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_logs")
    )
    keyboard.row(
        InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")
    )
    
    await callback_query.message.edit_text(logs_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())
    await log_admin_action(user_id, "view_logs")

# ================== АДМИН-ПАНЕЛЬ: ОЧИСТКА ЛОГОВ ==================
@dp.callback_query(F.data == "admin_clear_logs")
async def admin_clear_logs_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer()
    
    try:
        cursor.execute('DELETE FROM admin_logs')
        conn.commit()
        
        await callback_query.message.edit_text(
            "✅ *Логи очищены*\n\n"
            "Все записи логов удалены.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardBuilder()
                .add(InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_logs"))
                .as_markup()
        )
        
        await log_admin_action(user_id, "clear_logs")
        
    except Exception as e:
        logger.error(f"Error clearing logs: {e}")
        await callback_query.answer("❌ Ошибка при очистке логов")

# ================== АДМИН-ПАНЕЛЬ: ОБНОВЛЕНИЕ ==================
@dp.callback_query(F.data == "admin_refresh")
async def admin_refresh_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer("🔄 Данные обновлены")
    
    # Отправляем админ-панель напрямую
    admin_text = """
👑 *Админ-панель*

*Доступные функции:*
1. 📊 Просмотр статистики
2. 👥 Управление пользователями
3. 📢 Рассылка сообщений
4. 📝 Просмотр логов

Выберите действие:
    """
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
        InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")
    )
    keyboard.row(
        InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
        InlineKeyboardButton(text="📝 Логи", callback_data="admin_logs")
    )
    keyboard.row(
        InlineKeyboardButton(text="🔄 Обновить данные", callback_data="admin_refresh"),
        InlineKeyboardButton(text="🚪 Выйти", callback_data="admin_exit")
    )
    
    await callback_query.message.edit_text(admin_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())
    await log_admin_action(user_id, "refresh_admin_panel")

# ================== АДМИН-ПАНЕЛЬ: ВЫХОД ==================
@dp.callback_query(F.data == "admin_exit")
async def admin_exit_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer()
    await callback_query.message.delete()
    await log_admin_action(user_id, "exit_admin_panel")

# ================== АДМИН-ПАНЕЛЬ: ГЛАВНОЕ МЕНЮ ==================
@dp.callback_query(F.data == "admin_panel")
async def admin_panel_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer()
    
    # Отправляем админ-панель
    admin_text = """
👑 *Админ-панель*

*Доступные функции:*
1. 📊 Просмотр статистики
2. 👥 Управление пользователями
3. 📢 Рассылка сообщений
4. 📝 Просмотр логов

Выберите действие:
    """
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
        InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")
    )
    keyboard.row(
        InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
        InlineKeyboardButton(text="📝 Логи", callback_data="admin_logs")
    )
    keyboard.row(
        InlineKeyboardButton(text="🔄 Обновить данные", callback_data="admin_refresh"),
        InlineKeyboardButton(text="🚪 Выйти", callback_data="admin_exit")
    )
    
    await callback_query.message.edit_text(admin_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())
    await log_admin_action(user_id, "open_admin_panel")

# ================== ОБРАБОТКА ПОИСКА ПОЛЬЗОВАТЕЛЯ ==================
@dp.message(AdminState.waiting_user_id)
async def process_user_search(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await state.clear()
        return
    
    search_term = message.text.strip()
    
    if not search_term.isdigit():
        await message.answer("❌ Введите числовой ID пользователя")
        return
    
    target_user_id = int(search_term)
    
    try:
        cursor.execute('''
        SELECT user_id, username, first_name, last_name, anon_link, created_at
        FROM users WHERE user_id = ?
        ''', (target_user_id,))
        
        user = cursor.fetchone()
        
        if not user:
            await message.answer(f"❌ Пользователь с ID `{target_user_id}` не найден.", parse_mode="Markdown")
            await state.clear()
            return
        
        # Пробуем получить is_blocked если колонка существует
        is_blocked = 0
        try:
            cursor.execute('SELECT is_blocked FROM users WHERE user_id = ?', (target_user_id,))
            blocked_result = cursor.fetchone()
            if blocked_result:
                is_blocked = blocked_result[0]
        except:
            pass
        
        # Получаем статистику сообщений пользователя
        cursor.execute('SELECT COUNT(*) FROM messages WHERE sender_id = ?', (target_user_id,))
        sent_messages = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(*) FROM messages WHERE receiver_id = ?', (target_user_id,))
        received_messages = cursor.fetchone()[0] or 0
        
        user_info = f"""
👤 *Информация о пользователе*

*Основные данные:*
• *ID:* `{user[0]}`
• *Username:* {f'@{user[1]}' if user[1] else 'Нет username'}
• *Имя:* {user[2] or 'Не указано'} {user[3] or ''}
• *Ссылка:* `{user[4]}`
• *Статус:* {'⛔ Заблокирован' if is_blocked else '✅ Активен'}
• *Дата регистрации:* {user[5]}

*Статистика сообщений:*
✉️ Отправлено: {sent_messages}
📨 Получено: {received_messages}
📊 Всего: {sent_messages + received_messages}
        """
        
        keyboard = InlineKeyboardBuilder()
        
        # Кнопки действий
        if is_blocked:  # Если заблокирован
            keyboard.row(InlineKeyboardButton(text="✅ Разблокировать", callback_data=f"admin_unblock_{target_user_id}"))
        else:
            keyboard.row(InlineKeyboardButton(text="⛔ Заблокировать", callback_data=f"admin_block_{target_user_id}"))
        
        keyboard.row(
            InlineKeyboardButton(text="✉️ Написать сообщение", callback_data=f"admin_message_{target_user_id}"),
        )
        keyboard.row(
            InlineKeyboardButton(text="🔍 Найти другого", callback_data="admin_search_user"),
            InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")
        )
        
        await message.answer(user_info, parse_mode="Markdown", reply_markup=keyboard.as_markup())
        
    except Exception as e:
        logger.error(f"Ошибка при поиске пользователя: {e}")
        await message.answer(f"❌ Ошибка при поиске пользователя: {e}")
    
    await state.clear()
    await log_admin_action(user_id, "view_user_info", f"User ID: {target_user_id}")

# ================== ОБРАБОТКА РАССЫЛКИ ==================
@dp.message(AdminState.waiting_broadcast_message)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await state.clear()
        return
    
    broadcast_text = message.text
    
    # Получаем всех пользователей
    cursor.execute('SELECT user_id FROM users')
    users = cursor.fetchall()
    
    if not users:
        await message.answer("❌ Нет пользователей для рассылки.")
        await state.clear()
        return
    
    total_users = len(users)
    
    # Создаем клавиатуру для подтверждения
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="✅ Начать рассылку", callback_data="admin_confirm_broadcast"),
        InlineKeyboardButton(text="❌ Отменить", callback_data="admin_panel")
    )
    
    await state.update_data(broadcast_text=broadcast_text, broadcast_users=users)
    
    await message.answer(
        f"📋 *Подтверждение рассылки*\n\n"
        f"*Получателей:* {total_users}\n"
        f"*Сообщение:*\n{broadcast_text[:100]}...\n\n"
        f"Подтвердите начало рассылки:",
        parse_mode="Markdown",
        reply_markup=keyboard.as_markup()
    )

# ================== ПОДТВЕРЖДЕНИЕ РАССЫЛКИ ==================
@dp.callback_query(F.data == "admin_confirm_broadcast")
async def admin_confirm_broadcast_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ У вас нет доступа к админ-панели.", show_alert=True)
        return
    
    await callback_query.answer()
    
    data = await state.get_data()
    broadcast_text = data.get('broadcast_text', '')
    users = data.get('broadcast_users', [])
    
    if not users:
        await callback_query.message.edit_text("❌ Ошибка: список пользователей пуст.")
        await state.clear()
        return
    
    total_users = len(users)
    success_count = 0
    failed_count = 0
    
    # Обновляем статус
    status_message = await callback_query.message.edit_text(
        f"🔄 *Рассылка началась...*\n\n"
        f"Отправлено: 0/{total_users}\n"
        f"Успешно: 0\n"
        f"Ошибок: 0",
        parse_mode="Markdown"
    )
    
    # Рассылаем сообщения
    for i, user in enumerate(users, 1):
        target_user_id = user[0]
        
        try:
            await bot.send_message(target_user_id, broadcast_text)
            success_count += 1
        except Exception as e:
            failed_count += 1
            logger.error(f"Broadcast error for user {target_user_id}: {e}")
        
        # Обновляем статус каждые 10 сообщений
        if i % 10 == 0 or i == total_users:
            try:
                await status_message.edit_text(
                    f"🔄 *Рассылка...*\n\n"
                    f"Отправлено: {i}/{total_users}\n"
                    f"Успешно: {success_count}\n"
                    f"Ошибок: {failed_count}",
                    parse_mode="Markdown"
                )
            except:
                pass
        
        # Небольшая задержка, чтобы не превысить лимиты
        await asyncio.sleep(0.1)
    
    # Итоговый отчет
    result_text = f"""
✅ *Рассылка завершена*

*Результаты:*
👥 Всего получателей: {total_users}
✅ Успешно отправлено: {success_count}
❌ Не доставлено: {failed_count}
📊 Успешность: {(success_count/total_users*100):.1f}%

*Время:* {datetime.now().strftime('%H:%M:%S')}
    """
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔄 Новая рассылка", callback_data="admin_broadcast"),
        InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_panel")
    )
    
    await status_message.edit_text(result_text, parse_mode="Markdown", reply_markup=keyboard.as_markup())
    
    await state.clear()
    await log_admin_action(
        user_id, 
        "broadcast_completed", 
        f"Total: {total_users}, Success: {success_count}, Failed: {failed_count}"
    )

# ================== ОБРАБОТКА ВСЕХ ОСТАЛЬНЫХ ТЕКСТОВЫХ СООБЩЕНИЙ ==================
@dp.message(F.text)
async def handle_text_message(message: types.Message, state: FSMContext):
    # Игнорируем команды
    if message.text.startswith('/'):
        return
    
    await message.answer(
        "👋 *Анонимные сообщения*\n\n"
        "Используйте команды для работы с ботом:\n\n"
        "/start - Начать работу\n"
        "/link - Получить вашу ссылку\n"
        "/send - Отправить сообщение\n"
        "/stop - Остановить бота\n"
        "/stats - Статистика",
        parse_mode="Markdown"
    )

# ================== ОСНОВНАЯ ФУНКЦИЯ ЗАПУСКА ==================
async def main():
    logger.info("Бот запущен...")
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
