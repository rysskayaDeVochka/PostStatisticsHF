import os
import logging
import json
import sqlite3
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackContext
from flask import Flask, request, jsonify
import asyncio

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== FLASK APP ====================
app = Flask(__name__)

# Конфигурация
TOKEN = os.getenv('BOT_TOKEN')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', 'your-secret-token')
WEBHOOK_PATH = '/webhook'
DATABASE_URL = os.getenv('DATABASE_URL')

# Глобальные переменные для БД
DB_TYPE = None
conn = None
cursor = None
connection_pool = None

# ==================== БАЗА ДАННЫХ ====================
def init_database():
    """Инициализация базы данных"""
    global DB_TYPE, conn, cursor, connection_pool
    
    if DATABASE_URL and DATABASE_URL.startswith('postgres'):
        try:
            import psycopg2
            from psycopg2 import pool
            
            connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 20, DATABASE_URL, sslmode='require'
            )
            
            conn = connection_pool.getconn()
            cursor = conn.cursor()
            DB_TYPE = 'postgres'
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    username TEXT,
                    character_name TEXT,
                    message_date TIMESTAMP,
                    char_count INTEGER DEFAULT 0,
                    points INTEGER DEFAULT 1
                )
            ''')
            conn.commit()
            connection_pool.putconn(conn)
            logger.info("✅ PostgreSQL база инициализирована")
            
        except Exception as e:
            logger.error(f"❌ Ошибка PostgreSQL: {e}")
            DB_TYPE = 'sqlite'
    else:
        DB_TYPE = 'sqlite'
        conn = sqlite3.connect('character_stats.db', check_same_thread=False)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                user_id INTEGER,
                username TEXT,
                character_name TEXT,
                message_date DATETIME,
                char_count INTEGER DEFAULT 0,
                points INTEGER DEFAULT 1
            )
        ''')
        conn.commit()
        logger.info("✅ SQLite база инициализирована")

# Инициализируем БД
init_database()

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def calculate_points(char_count):
    """Рассчитывает очки за длину поста"""
    if char_count < 500:
        return 1
    elif char_count < 1000:
        return 2
    elif char_count < 1500:
        return 3
    elif char_count < 2000:
        return 4
    elif char_count < 2500:
        return 6
    elif char_count < 3000:
        return 7
    elif char_count < 3500:
        return 8
    elif char_count < 4000:
        return 9
    elif char_count < 4500:
        return 10
    elif char_count < 5000:
        return 11
    else:
        return 12

def format_number(num):
    """Форматирует число с разделителями тысяч"""
    return f"{num:,}".replace(",", " ")

def decline_points(points):
    """Склоняет слово 'очко'"""
    if points % 10 == 1 and points % 100 != 11:
        return "очко"
    elif 2 <= points % 10 <= 4 and (points % 100 < 10 or points % 100 >= 20):
        return "очка"
    else:
        return "очков"

def decline_posts(posts):
    """Склоняет слово 'пост'"""
    if posts % 10 == 1 and posts % 100 != 11:
        return "пост"
    elif 2 <= posts % 10 <= 4 and (posts % 100 < 10 or posts % 100 >= 20):
        return "поста"
    else:
        return "постов"

# ==================== ОСНОВНЫЕ ФУНКЦИИ БОТА ====================
async def handle_message(update: Update, context: CallbackContext):
    """Обработка сообщений в ГРУППАХ"""
    try:
        # БЛОКИРУЕМ личные сообщения
        if update.message.chat.type == 'private':
            logger.info(f"🚫 Игнорируем ЛС от {update.effective_user.first_name}")
            return
        
        text = update.message.text
        
        # Берем первую строку
        lines = text.strip().split('\n')
        if not lines:
            return
        
        character_name = lines[0].strip().lower()
        
        if character_name and not character_name.startswith('/'):
            # Считаем символы и очки
            char_count = len(text)
            points = calculate_points(char_count)
            
            # СОХРАНЯЕМ С ДАТОЙ, СИМВОЛАМИ И ОЧКАМИ
            user = update.message.from_user
            display_name = f"@{user.username}" if user.username else user.first_name
            
            if DB_TYPE == 'postgres':
                # Для PostgreSQL
                import psycopg2
                temp_conn = connection_pool.getconn()
                temp_cursor = temp_conn.cursor()
                temp_cursor.execute(
                    """INSERT INTO posts 
                       (chat_id, user_id, username, character_name, message_date, char_count, points) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (update.message.chat_id, 
                     user.id,
                     display_name,
                     character_name,
                     update.message.date,
                     char_count,
                     points)
                )
                temp_conn.commit()
                connection_pool.putconn(temp_conn)
            else:
                # Для SQLite
                cursor.execute(
                    """INSERT INTO posts 
                       (chat_id, user_id, username, character_name, message_date, char_count, points) 
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (update.message.chat_id, 
                     user.id,
                     display_name,
                     character_name,
                     update.message.date,
                     char_count,
                     points)
                )
                conn.commit()
            
            logger.info(f"✅ Сохранено: {display_name} - '{character_name}' - {char_count} симв., {points} {decline_points(points)}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_message: {e}")

async def start_command(update: Update, context: CallbackContext):
    """Команда /start - работает только в группах"""
    if update.message.chat.type == 'private':
        logger.info(f"🚫 Игнорируем /start в ЛС от {update.effective_user.first_name}")
        return
    
    await update.message.reply_text(
        "🤖 Бот для подсчета постов персонажей с системой очков!\n\n"
        "📝 Как использовать:\n"
        "1. Пишите сообщение где в ПЕРВОЙ строке имя персонажа\n"
        "2. Бот автоматически сохраняет его\n"
        "3. Чем длиннее пост - тем больше очков!\n\n"
        "📌 Пример:\n"
        "```\n"
        "Гендальф\n"
        "Сегодня был в Ривенделле...\n"
        "```\n\n"
        "🎯 Система очков за длину:\n"
        "• <500 симв. = 1 очко\n"
        "• 500-1000 = 2 очка\n"
        "• 1000-1500 = 3 очка\n"
        "• 1500-2000 = 4 очка\n"
        "• 2000-2500 = 6 очков\n"
        "• 2500-3000 = 7 очков\n"
        "• 3000-3500 = 8 очков\n"
        "• 3500-4000 = 9 очков\n"
        "• 4000-4500 = 10 очков\n"
        "• 4500-5000 = 11 очков\n"
        "• >5000 = 12 очков\n\n"
        "📊 Доступные команды:\n"
        "/stats [period] - полная статистика (today/week/month/all)\n"
        "/top [period] - топ-10 пользователей (today/week/month/all)\n"
        "/mystats -ваши персонажи и статистика"
    )

async def get_user_stats(chat_id, period='month'):
    """Получает статистику по пользователям за период"""
    now = datetime.now()
    
    # Определяем дату начала периода
    if period == 'today':
        start_date = now.date()
        if DB_TYPE == 'postgres':
            condition = "AND DATE(message_date) = %s"
        else:
            condition = "AND DATE(message_date) = DATE(?)"
        params = (chat_id, start_date)
    elif period == 'week':
        start_date = now - timedelta(days=7)
        condition = "AND message_date >= ?"
        params = (chat_id, start_date)
    elif period == 'month':
        start_date = now - timedelta(days=30)
        condition = "AND message_date >= ?"
        params = (chat_id, start_date)
    else:  # all
        condition = ""
        params = (chat_id,)
    
    # Запрос для получения статистики
    query = f'''
        SELECT 
            p.user_id,
            p.username,
            p.character_name,
            COUNT(*) as post_count,
            COALESCE(SUM(p.char_count), 0) as char_count,
            COALESCE(SUM(p.points), 0) as points
        FROM posts p
        WHERE p.chat_id = ?
        {condition}
        GROUP BY p.user_id, p.character_name
        ORDER BY p.user_id, COALESCE(SUM(p.points), 0) DESC
    '''
    
    # Выполняем запрос
    if DB_TYPE == 'postgres':
        temp_conn = connection_pool.getconn()
        temp_cursor = temp_conn.cursor()
        temp_cursor.execute(query.replace('?', '%s'), params)
        rows = temp_cursor.fetchall()
        connection_pool.putconn(temp_conn)
    else:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    
    # Группируем вручную в Python
    user_stats = {}
    for user_id, username, character_name, post_count, char_count, points in rows:
        if user_id not in user_stats:
            user_stats[user_id] = {
                'username': username,
                'total_posts': 0,
                'total_chars': 0,
                'total_points': 0,
                'characters': [],
                'char_count': 0
            }
        
        # Добавляем персонажа как словарь
        user_stats[user_id]['characters'].append({
            'name': character_name,
            'posts': post_count,
            'chars': char_count,
            'points': points
        })
        
        # Обновляем общие счетчики
        user_stats[user_id]['total_posts'] += post_count
        user_stats[user_id]['total_chars'] += char_count
        user_stats[user_id]['total_points'] += points
        user_stats[user_id]['char_count'] = len(user_stats[user_id]['characters'])
    
    # Преобразуем в нужный формат
    result = []
    for user_id, data in user_stats.items():
        characters_json = json.dumps(data['characters'], ensure_ascii=False)
        
        result.append((
            user_id,
            data['username'],
            characters_json,
            data['total_posts'],
            data['total_chars'],
            data['total_points'],
            data['char_count']
        ))
    
    # Сортируем по общему количеству очков
    result.sort(key=lambda x: x[5], reverse=True)
    
    return result

async def stats_command(update: Update, context: CallbackContext):
    """Команда /stats - ДЕТАЛЬНАЯ статистика по пользователям"""
    if update.message.chat.type == 'private':
        logger.info(f"🚫 Игнорируем /stats в ЛС от {update.effective_user.first_name}")
        return
    
    chat_id = update.effective_chat.id
    args = context.args
    
    # По умолчанию - за месяц
    period = 'month'
    period_text = "за месяц"
    
    if args:
        arg = args[0].lower()
        if arg in ['сегодня', 'today']:
            period = 'today'
            period_text = "за сегодня"
        elif arg in ['неделя', 'week']:
            period = 'week'
            period_text = "за неделю"
        elif arg in ['месяц', 'month']:
            period = 'month'
            period_text = "за месяц"
        elif arg in ['все', 'all', 'всё']:
            period = 'all'
            period_text = "за всё время"
    
    # Получаем статистику
    results = await get_user_stats(chat_id, period)
    
    if not results:
        await update.message.reply_text(f"📭 Нет данных {period_text}!")
        return
    
    # Формируем сообщение
    text = f"📊 СТАТИСТИКА ПО ПОЛЬЗОВАТЕЛЯМ {period_text.upper()}:\n\n"
    
    # Показываем ВСЕХ пользователей
    for i, (user_id, username, characters_json, posts, chars, points, char_count) in enumerate(results, 1):
        posts_word = decline_posts(posts)
        points_word = decline_points(points)
        
        # Общая статистика пользователя
        text += f"{i}. {username}: {posts} {posts_word}, {format_number(chars)} симв., {points} {points_word}\n"
        
        # Обработка персонажей из JSON
        if characters_json:
            try:
                characters = json.loads(characters_json)
                
                text += "  Персонажи:\n"
                
                for char in characters:
                    char_name = char['name']
                    char_posts = char['posts']
                    char_chars = char['chars']
                    char_points = char['points']
                    
                    char_posts_word = decline_posts(char_posts)
                    char_points_word = decline_points(char_points)
                    
                    text += f"  • {char_name}: {char_posts} {char_posts_word}, {format_number(char_chars)} симв., {char_points} {char_points_word}\n"
                    
            except (json.JSONDecodeError, KeyError) as e:
                text += "  Персонажи: ошибка данных\n"
                logger.error(f"Ошибка разбора JSON: {e}")
        else:
            text += "  Персонажи: нет данных\n"
        
        text += "\n"
    
    # Если сообщение слишком длинное, разбиваем
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await update.message.reply_text(part)
    else:
        await update.message.reply_text(text)

async def top_command(update: Update, context: CallbackContext):
    """Команда /top - топ-10 пользователей за период"""
    if update.message.chat.type == 'private':
        logger.info(f"🚫 Игнорируем /top в ЛС от {update.effective_user.first_name}")
        return
    
    chat_id = update.effective_chat.id
    args = context.args
    
    # По умолчанию - за месяц
    period = 'month'
    period_text = "за месяц"
    
    if args:
        arg = args[0].lower()
        if arg in ['сегодня', 'today']:
            period = 'today'
            period_text = "за сегодня"
        elif arg in ['неделя', 'week']:
            period = 'week'
            period_text = "за неделю"
        elif arg in ['месяц', 'month']:
            period = 'month'
            period_text = "за месяц"
        elif arg in ['все', 'all', 'всё']:
            period = 'all'
            period_text = "за всё время"
    
    # Получаем топ пользователей за период
    results = await get_user_stats(chat_id, period)
    
    if not results:
        await update.message.reply_text(f"📭 Нет данных {period_text}!")
        return
    
    # Берем только топ-10
    top_users = results[:10]
    
    # Заголовок с периодом
    period_emojis = {
        'today': '📅',
        'week': '📆', 
        'month': '📊',
        'all': '🏆'
    }
    emoji = period_emojis.get(period, '🏆')
    
    text = f"{emoji} ТОП-10 ПОЛЬЗОВАТЕЛЕЙ {period_text.upper()}:\n\n"
    
    for i, (user_id, username, characters_json, posts, chars, points, char_count) in enumerate(top_users, 1):
        if i == 1: 
            medal = "👑 "
        elif i == 2: 
            medal = "🥈 "
        elif i == 3: 
            medal = "🥉 "
        else: 
            medal = f"{i}. "
        
        posts_word = decline_posts(posts)
        points_word = decline_points(points)
        
        text += f"{medal}{username}: {points} {points_word}\n"
        text += f"   📝 {posts} {posts_word}, {format_number(chars)} симв.\n"
        text += f"   🎭 Персонажей: {char_count}\n"
        
        # Самый успешный персонаж из JSON
        if characters_json:
            try:
                characters = json.loads(characters_json)
                if characters:
                    best_char = characters[0]  # Уже отсортированы по очкам
                    char_points_word = decline_points(best_char['points'])
                    text += f"   ⭐ Лучший: {best_char['name'].title()} ({best_char['points']} {char_points_word})\n"
            except (json.JSONDecodeError, KeyError):
                pass
        
        text += "\n"
    
    await update.message.reply_text(text)

async def mystats_command(update: Update, context: CallbackContext):
    """Команда /mystats - личная статистика пользователя"""
    if update.message.chat.type == 'private':
        logger.info(f"🚫 Игнорируем /mystats в ЛС от {update.effective_user.first_name}")
        return
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    username = update.effective_user.username or update.effective_user.first_name
    display_name = f"@{username}" if update.effective_user.username else username
    
    # Запрос для SQLite/PostgreSQL
    if DB_TYPE == 'postgres':
        temp_conn = connection_pool.getconn()
        temp_cursor = temp_conn.cursor()
        
        # Статистика пользователя за все время
        temp_cursor.execute('''
            SELECT 
                character_name,
                COUNT(*) as post_count,
                SUM(char_count) as char_count,
                SUM(points) as points
            FROM posts
            WHERE chat_id = %s AND user_id = %s
            GROUP BY character_name
            ORDER BY points DESC
        ''', (chat_id, user_id))
        
        character_stats = temp_cursor.fetchall()
        
        # Общая статистика пользователя
        temp_cursor.execute('''
            SELECT 
                COUNT(*) as total_posts,
                SUM(char_count) as total_chars,
                SUM(points) as total_points
            FROM posts 
            WHERE chat_id = %s AND user_id = %s
        ''', (chat_id, user_id))
        
        total_stats = temp_cursor.fetchone()
        connection_pool.putconn(temp_conn)
    else:
        # Статистика пользователя за все время
        cursor.execute('''
            SELECT 
                character_name,
                COUNT(*) as post_count,
                SUM(char_count) as char_count,
                SUM(points) as points
            FROM posts
            WHERE chat_id = ? AND user_id = ?
            GROUP BY character_name
            ORDER BY points DESC
        ''', (chat_id, user_id))
        
        character_stats = cursor.fetchall()
        
        # Общая статистика пользователя
        cursor.execute('''
            SELECT 
                COUNT(*) as total_posts,
                SUM(char_count) as total_chars,
                SUM(points) as total_points
            FROM posts 
            WHERE chat_id = ? AND user_id = ?
        ''', (chat_id, user_id))
        
        total_stats = cursor.fetchone()
    
    if not character_stats:
        await update.message.reply_text(f"📭 {display_name}, у вас пока нет постов!")
        return
    
    total_posts, total_chars, total_points = total_stats or (0, 0, 0)
    
    text = f"📊 ВАША СТАТИСТИКА {display_name.upper()}:\n\n"
    
    # Все персонажи пользователя
    for char_name, posts, chars, points in character_stats:
        posts_word = decline_posts(posts)
        points_word = decline_points(points)
        
        text += f"🎭 {char_name.title()}:\n"
        text += f"   📝 {posts} {posts_word}, {format_number(chars)} симв., {points} {points_word}\n\n"
    
    # Итоговая статистика
    total_posts_word = decline_posts(total_posts)
    total_points_word = decline_points(total_points)
    
    text += f"📈 ВАШИ ИТОГИ:\n"
    text += f"• Персонажей: {len(character_stats)}\n"
    text += f"• Постов: {total_posts} {total_posts_word}\n"
    text += f"• Символов: {format_number(total_chars)}\n"
    text += f"• Очков: {total_points} {total_points_word}"
    
    # Самый успешный персонаж
    if character_stats:
        best_char = character_stats[0]
        best_points_word = decline_points(best_char[3])
        text += f"\n\n🏆 ВАШ ЛУЧШИЙ ПЕРСОНАЖ:\n"
        text += f"{best_char[0].title()} - {best_char[3]} {best_points_word}"
    
    await update.message.reply_text(text)

# ==================== ТЕЛЕГРАМ ПРИЛОЖЕНИЕ ====================
# Создаем Telegram приложение
telegram_app = Application.builder().token(TOKEN).build()

# Регистрация ВСЕХ обработчиков
telegram_app.add_handler(CommandHandler("start", start_command))
telegram_app.add_handler(CommandHandler("stats", stats_command))
telegram_app.add_handler(CommandHandler("top", top_command))
telegram_app.add_handler(CommandHandler("mystats", mystats_command))
telegram_app.add_handler(MessageHandler(
    filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
    handle_message
))

# ==================== FLASK WEBHOOK ENDPOINTS ====================
@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "service": "telegram-character-counter-bot",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy"}), 200

@app.route('/ping')
def ping():
    """Для поддержания активности на Render"""
    return "pong", 200

@app.route(WEBHOOK_PATH, methods=['POST'])
async def webhook():
    """Основной endpoint для вебхука Telegram"""
    if WEBHOOK_SECRET and request.headers.get('X-Telegram-Bot-Api-Secret-Token') != WEBHOOK_SECRET:
        return 'Unauthorized', 403
    
    try:
        update = Update.de_json(request.get_json(), telegram_app.bot)
        await telegram_app.initialize()
        await telegram_app.process_update(update)
        return 'OK', 200
    except Exception as e:
        logger.error(f"Error processing update: {e}")
        return 'Internal Server Error', 500

@app.route('/set_webhook', methods=['GET', 'POST'])
async def set_webhook_route():
    """Ручка для установки вебхука (можно открыть в браузере)"""
    try:
        webhook_url = f"{request.host_url.rstrip('/')}{WEBHOOK_PATH}"
        await telegram_app.bot.set_webhook(
            url=webhook_url,
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True
        )
        logger.info(f"✅ Webhook set to: {webhook_url}")
        return jsonify({
            "success": True,
            "webhook_url": webhook_url,
            "message": "Webhook установлен успешно! Бот готов к работе."
        })
    except Exception as e:
        logger.error(f"❌ Failed to set webhook: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/delete_webhook', methods=['POST'])
async def delete_webhook_route():
    """Удаление вебхука"""
    try:
        await telegram_app.bot.delete_webhook(drop_pending_updates=True)
        return jsonify({"success": True, "message": "Webhook удален"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ==================== ЗАПУСК ====================
def setup_webhook_on_startup():
    """Автоматическая установка вебхука при старте"""
    import threading
    import time
    
    def set_webhook_thread():
        time.sleep(5)  # Ждем запуска Flask
        try:
            # Получаем URL из переменных окружения Render
            render_host = os.getenv('RENDER_EXTERNAL_HOSTNAME')
            if render_host:
                webhook_url = f"https://{render_host}{WEBHOOK_PATH}"
                asyncio.run(telegram_app.bot.set_webhook(
                    url=webhook_url,
                    secret_token=WEBHOOK_SECRET,
                    drop_pending_updates=True
                ))
                logger.info(f"✅ Webhook auto-set to: {webhook_url}")
            else:
                logger.warning("⚠️ RENDER_EXTERNAL_HOSTNAME не найден, вебхук не установлен")
        except Exception as e:
            logger.error(f"⚠️ Auto webhook setup failed: {e}. Set manually via /set_webhook")

    if os.getenv('RENDER') or os.getenv('AUTO_SET_WEBHOOK'):
        thread = threading.Thread(target=set_webhook_thread, daemon=True)
        thread.start()

# Запускаем автоматическую настройку вебхука
setup_webhook_on_startup()

if __name__ == '__main__':
    port = int(os.getenv('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)


