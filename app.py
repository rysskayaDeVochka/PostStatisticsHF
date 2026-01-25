import os
import logging
import asyncio
import sys
import threading
from pymysql.cursors import DictCursor
from flask import Flask, jsonify, request
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
import asyncio
import threading
import json
from datetime import datetime, timedelta
import nest_asyncio
nest_asyncio.apply()


# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ==================== FLASK APP ====================
app = Flask(__name__)

# Конфигурация
TOKEN = os.getenv('BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')  # MySQL строка от TiDB
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', 'secret123')
WEBHOOK_PATH = '/webhook'

# ==================== TIDB (MySQL) БАЗА ====================
def parse_tidb_url(url):
    """Парсим строку подключения TiDB: mysql://user:pass@host:port/dbname"""
    try:
        # Формат: mysql://username:password@host:port/database
        if url.startswith('mysql://'):
            url = url[8:]  # Убираем mysql://
        
        # Разбираем части
        auth_part, host_part = url.split('@')
        username, password = auth_part.split(':')
        host_port, database = host_part.split('/')
        
        if ':' in host_port:
            host, port = host_port.split(':')
            port = int(port)
        else:
            host = host_port
            port = 4000  # Стандартный порт TiDB
        
        return {
            'host': host,
            'port': port,
            'user': username,
            'password': password,
            'database': database,
            'ssl': {'ssl': {'ca': ''}}  # TiDB требует SSL
        }
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга DATABASE_URL: {e}")
        return None

def init_tidb():
    """Инициализация TiDB Cloud (MySQL-совместимая)"""
    try:
        if not DATABASE_URL:
            logger.warning("⚠️ DATABASE_URL не задан")
            return None
        
        # Парсим строку подключения
        db_config = parse_tidb_url(DATABASE_URL)
        if not db_config:
            return None
        
        # Тестовое подключение
        test_conn = pymysql.connect(**db_config)
        cursor = test_conn.cursor()
        
        # Создаем таблицу если нет
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username VARCHAR(255),
                character_name VARCHAR(255) NOT NULL,
                message_date DATETIME NOT NULL,
                char_count INT DEFAULT 0,
                points INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                INDEX idx_chat_user (chat_id, user_id),
                INDEX idx_character (character_name),
                INDEX idx_date (message_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        ''')
        
        test_conn.commit()
        test_conn.close()
        
        logger.info("✅ TiDB Cloud инициализирована (5 ГБ бесплатно!)")
        
        # Создаем пул соединений
        return pool.ConnectionPool(
            size=5,
            maxsize=20,
            **db_config
        )
        
    except Exception as e:
        logger.error(f"❌ Ошибка TiDB: {e}")
        return None

# Инициализация TiDB
db_pool = init_tidb()

# ==================== ТЕЛЕГРАМ БОТ ====================
try:
    telegram_app = Application.builder().token(TOKEN).build()
    logger.info("✅ Telegram приложение создано")
except Exception as e:
    logger.error(f"❌ Ошибка Telegram: {e}")
    telegram_app = None

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def calculate_points(char_count):
    if char_count < 500: return 1
    elif char_count < 1000:return 2
    elif char_count < 1500: return 3
    elif char_count < 2000: return 4
    elif char_count < 2500: return 6
    elif char_count < 3000: return 7
    elif char_count < 3500: return 8
    elif char_count < 4000: return 9
    elif char_count < 4500: return 10
    elif char_count < 5000: return 11
    else: return 12

def format_number(num):
    return f"{num:,}".replace(",", " ")

def decline_points(points):
    if points % 10 == 1 and points % 100 != 11:
        return "очко"
    elif 2 <= points % 10 <= 4 and (points % 100 < 10 or points % 100 >= 20):
        return "очка"
    else:
        return "очков"

def decline_posts(posts):
    if posts % 10 == 1 and posts % 100 != 11:
        return "пост"
    elif 2 <= posts % 10 <= 4 and (posts % 100 < 10 or posts % 100 >= 20):
        return "поста"
    else:
        return "постов"

# ==================== ФУНКЦИИ ДЛЯ TIDB ====================
def save_to_tidb(chat_id, user_id, username, character_name, message_date, char_count, points):
    """Сохранение поста в TiDB"""
    if not db_pool:
        logger.error("❌ TiDB пул не инициализирован")
        return False
    
    try:
        conn = db_pool.connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO posts 
            (chat_id, user_id, username, character_name, message_date, char_count, points)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (chat_id, user_id, username, character_name, message_date, char_count, points))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Сохранено в TiDB: {character_name} - {points} очков")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения в TiDB: {e}")
        return False

async def get_user_stats_tidb(chat_id, period='month'):
    """Получение статистики из TiDB"""
    if not db_pool:
        return []
    
    now = datetime.now()
    
    # Условие для периода
    if period == 'today':
        start_date = now.date()
        condition = "AND DATE(message_date) = %s"
        params = (chat_id, start_date)
    elif period == 'week':
        start_date = now - timedelta(days=7)
        condition = "AND message_date >= %s"
        params = (chat_id, start_date)
    elif period == 'month':
        start_date = now - timedelta(days=30)
        condition = "AND message_date >= %s"
        params = (chat_id, start_date)
    else:  # all
        condition = ""
        params = (chat_id,)
    
    try:
        conn = db_pool.connection()
        cursor = conn.cursor(DictCursor)
        
        query = f'''
            SELECT 
                user_id,
                username,
                character_name,
                COUNT(*) as post_count,
                SUM(char_count) as char_count,
                SUM(points) as points
            FROM posts
            WHERE chat_id = %s
            {condition}
            GROUP BY user_id, character_name
            ORDER BY user_id, SUM(points) DESC
        '''
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Группируем результаты
        user_stats = {}
        for row in rows:
            user_id = row['user_id']
            if user_id not in user_stats:
                user_stats[user_id] = {
                    'username': row['username'],
                    'total_posts': 0,
                    'total_chars': 0,
                    'total_points': 0,
                    'characters': []
                }
            
            user_stats[user_id]['characters'].append({
                'name': row['character_name'],
                'posts': row['post_count'],
                'chars': row['char_count'],
                'points': row['points']
            })
            
            user_stats[user_id]['total_posts'] += row['post_count']
            user_stats[user_id]['total_chars'] += row['char_count']
            user_stats[user_id]['total_points'] += row['points']
        
        # Преобразуем в список
        result = []
        for user_id, data in user_stats.items():
            result.append((
                user_id,
                data['username'],
                json.dumps(data['characters'], ensure_ascii=False),
                data['total_posts'],
                data['total_chars'],
                data['total_points'],
                len(data['characters'])
            ))
        
        # Сортируем по очкам
        result.sort(key=lambda x: x[5], reverse=True)
        return result
        
    except Exception as e:
        logger.error(f"❌ Ошибка запроса к TiDB: {e}")
        return []

# ==================== ОБРАБОТЧИКИ БОТА ====================
async def handle_message(update: Update, context: CallbackContext):
    """Сохранение сообщения в TiDB"""
    try:
        if update.message.chat.type == 'private':
            return
        
        text = update.message.text.strip()
        lines = text.split('\n')
        if not lines:
            return
        
        character_name = lines[0].strip().lower()
        if not character_name or character_name.startswith('/'):
            return
        
        char_count = len(text)
        points = calculate_points(char_count)
        user = update.message.from_user
        display_name = f"@{user.username}" if user.username else user.first_name
        
        # Сохраняем в TiDB
        saved = save_to_tidb(
            update.message.chat_id,
            user.id,
            display_name,
            character_name,
            update.message.date,
            char_count,
            points
        )
        
        if saved:
            logger.info(f"✅ Сохранено в TiDB: {character_name}")
        else:
            logger.error("❌ Не удалось сохранить в TiDB")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_message: {e}")

async def start_command(update: Update, context: CallbackContext):
    await update.message.reply_text(
        "🤖 Бот с TiDB Cloud (5 ГБ бесплатно!)\n\n"
        "📝 Как использовать:\n"
        "1. Пиши сообщение где ПЕРВАЯ строка - имя персонажа\n"
        "2. Бот автоматически сохраняет в TiDB\n"
        "3. Чем длиннее пост - тем больше очков!\n\n"
        "📊 Команды:\n"
        "/stats [period] - статистика\n"
        "/top [period] - топ-10\n"
        "/mystats - личная статистика"
    )

async def stats_command(update: Update, context: CallbackContext):
    if update.message.chat.type == 'private':
        return
    
    chat_id = update.effective_chat.id
    args = context.args if context.args else []
    
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
    
    results = await get_user_stats_tidb(chat_id, period)
    
    if not results:
        await update.message.reply_text(f"📭 Нет данных {period_text}!")
        return
    
    text = f"📊 СТАТИСТИКА {period_text.upper()} (TiDB):\n\n"
    
    for i, (user_id, username, characters_json, posts, chars, points, char_count) in enumerate(results, 1):
        posts_word = decline_posts(posts)
        points_word = decline_points(points)
        
        text += f"{i}. {username}: {posts} {posts_word}, {format_number(chars)} симв., {points} {points_word}\n"
        
        if characters_json and characters_json != 'null':
            try:
                characters = json.loads(characters_json)
                if characters:text += "  Персонажи:\n"
                for char in characters[:3]:  # Показываем только топ-3
                    char_name = char.get('name', 'Неизвестно')
                    char_posts = char.get('posts', 0)
                    char_chars = char.get('chars', 0)
                    char_points = char.get('points', 0)
        
                    char_posts_word = decline_posts(char_posts)
                    char_points_word = decline_points(char_points)
                        
                    text += f"  • {char_name}: {char_posts} {char_posts_word}, {format_number(char_chars)} симв., {char_points} {char_points_word}\n"
            except Exception:
                pass
        
    text += "\n"
    
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await update.message.reply_text(part)
    else:
        await update.message.reply_text(text)

async def top_command(update: Update, context: CallbackContext):
    if update.message.chat.type == 'private':
        return
    
    chat_id = update.effective_chat.id
    args = context.args if context.args else []
    
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
    
    results = await get_user_stats_tidb(chat_id, period)
    
    if not results:
        await update.message.reply_text(f"📭 Нет данных {period_text}!")
        return
    
    top_users = results[:10]
    
    emoji = {'today': '📅', 'week': '📆', 'month': '📊', 'all': '🏆'}.get(period, '🏆')
    
    text = f"{emoji} ТОП-10 {period_text.upper()} (TiDB):\n\n"
    
    for i, (user_id, username, characters_json, posts, chars, points, char_count) in enumerate(top_users, 1):
        if i == 1: medal = "👑 "
        elif i == 2: medal = "🥈 "
        elif i == 3: medal = "🥉 "
        else: medal = f"{i}. "
        
        posts_word = decline_posts(posts)
        points_word = decline_points(points)
        
        text += f"{medal}{username}: {points} {points_word}\n"
        text += f"   📝 {posts} {posts_word}, {format_number(chars)} симв.\n"
        text += f"   🎭 Персонажей: {char_count}\n"
        
        if characters_json and characters_json != 'null':
            try:
                characters = json.loads(characters_json)
                if characters:
                    best_char = characters[0]
                    char_points_word = decline_points(best_char.get('points', 0))
                    text += f"   ⭐ Лучший: {best_char.get('name', 'Неизвестно').title()} ({best_char.get('points', 0)} {char_points_word})\n"
            except Exception:
                pass
        
        text += "\n"
    
    await update.message.reply_text(text)

async def mystats_command(update: Update, context: CallbackContext):
    if update.message.chat.type == 'private':
        return
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    username = update.effective_user.username or update.effective_user.first_name
    display_name = f"@{username}" if update.effective_user.username else username
    
    if not db_pool:
        await update.message.reply_text("❌ База данных не доступна")
        return
    
    try:
        conn = db_pool.connection()
        cursor = conn.cursor(DictCursor)
        
        # Персонажи пользователя
        cursor.execute('''
            SELECT 
                character_name,
                COUNT(*) as post_count,SUM(char_count) as char_count,
                SUM(points) as points
            FROM posts
            WHERE chat_id = %s AND user_id = %s
            GROUP BY character_name
            ORDER BY points DESC
        ''', (chat_id, user_id))
        
        character_stats = cursor.fetchall()
        
        # Общая статистика
        cursor.execute('''
            SELECT 
                COUNT(*) as total_posts,
                SUM(char_count) as total_chars,
                SUM(points) as total_points
            FROM posts 
            WHERE chat_id = %s AND user_id = %s
        ''', (chat_id, user_id))
        
        total_stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not character_stats:
            await update.message.reply_text(f"📭 {display_name}, у вас пока нет постов в TiDB!")
            return
        
        total_posts = total_stats['total_posts'] if total_stats and total_stats['total_posts'] else 0
        total_chars = total_stats['total_chars'] if total_stats and total_stats['total_chars'] else 0
        total_points = total_stats['total_points'] if total_stats and total_stats['total_points'] else 0
        
        text = f"📊 ВАША СТАТИСТИКА {display_name.upper()} (TiDB):\n\n"
        
        for char in character_stats:
            posts = char['post_count']
            chars = char['char_count']
            points = char['points']
            
            posts_word = decline_posts(posts)
            points_word = decline_points(points)
            
            text += f"🎭 {char['character_name'].title()}:\n"
            text += f"   📝 {posts} {posts_word}, {format_number(chars)} симв., {points} {points_word}\n\n"
        
        total_posts_word = decline_posts(total_posts)
        total_points_word = decline_points(total_points)
        
        text += f"📈 ВАШИ ИТОГИ:\n"
        text += f"• Персонажей: {len(character_stats)}\n"
        text += f"• Постов: {total_posts} {total_posts_word}\n"
        text += f"• Символов: {format_number(total_chars)}\n"
        text += f"• Очков: {total_points} {total_points_word}"
        
        if character_stats:
            best_char = character_stats[0]
            best_points_word = decline_points(best_char['points'])
            text += f"\n\n🏆 ВАШ ЛУЧШИЙ ПЕРСОНАЖ:\n"
            text += f"{best_char['character_name'].title()} - {best_char['points']} {best_points_word}"
        
        await update.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"❌ Ошибка mystats: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")

# Регистрация обработчиков
if telegram_app:
    telegram_app.add_handler(CommandHandler("start", start_command))
    telegram_app.add_handler(CommandHandler("stats", stats_command))
    telegram_app.add_handler(CommandHandler("top", top_command))
    telegram_app.add_handler(CommandHandler("mystats", mystats_command))
    telegram_app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
        handle_message
    ))

# ==================== FLASK ENDPOINTS ====================
@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "database": "TiDB Cloud" if db_pool else "not_connected",
        "free_storage": "5 GB",
        "bot": "ready" if telegram_app else "not_ready"
    })

@app.route('/health')
def health():
    # Проверяем подключение к TiDB
    db_healthy = False
    if db_pool:
        try:
            conn = db_pool.connection()
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            cursor.fetchone()
            cursor.close()
            conn.close()
            db_healthy = True
        except Exception:
            db_healthy = False
    
    return jsonify({
        "status": "healthy",
        "database": "connected" if db_healthy else "disconnected",
        "bot": "ready" if telegram_app else "not_ready"
    }), 200 if db_healthy and telegram_app else 500

@app.route('/ping')
def ping():
    return "pong", 200

@app.route('/db_stats')
def db_stats():
    """Статистика TiDB"""
    if not db_pool:
        return jsonify({"error": "TiDB not connected"}), 500
    
    try:
        conn = db_pool.connection()
        cursor = conn.cursor(DictCursor)
        
        cursor.execute('SELECT COUNT(*) as total FROM posts')
        total = cursor.fetchone()['total']
        
        cursor.execute('SELECT COUNT(DISTINCT user_id) as users FROM posts')
        users = cursor.fetchone()['users']
        
        cursor.execute('SELECT COUNT(DISTINCT character_name) as characters FROM posts')
        characters = cursor.fetchone()['characters']
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "total_posts": total,
            "unique_users": users,
            "unique_characters": characters,
            "database": "TiDB Cloud"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/set_webhook', methods=['GET'])
def set_webhook():
    if not telegram_app:
        return jsonify({"error": "Bot not ready"}), 500
    
    try:
        render_host = os.getenv('RENDER_EXTERNAL_HOSTNAME')
        webhook_url = f"https://{render_host}{WEBHOOK_PATH}"
        
        # Новый event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        loop.run_until_complete(
            telegram_app.bot.set_webhook(
                url=webhook_url,
                secret_token=WEBHOOK_SECRET,
                drop_pending_updates=True
            )
        )
        
        loop.close()
        
        return jsonify({
            "success": True,
            "webhook_url": webhook_url,
            "message": "Вебхук установлен!"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        

@app.route(WEBHOOK_PATH, methods=['POST'])
async def webhook():
    if request.headers.get('X-Telegram-Bot-Api-Secret-Token') != WEBHOOK_SECRET:
        return 'Unauthorized', 403
    
    try:
        data = request.get_json()   
        update = Update.de_json(data, telegram_app.bot)

        await telegram_app.initialize()
        await telegram_app.process_update(update)
           
        return 'OK', 200
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        return 'Internal Server Error', 500
        
    
    def set_webhook_thread():
        time.sleep(5)
        try:
            render_host = os.getenv('RENDER_EXTERNAL_HOSTNAME')
            if render_host and telegram_app:
                webhook_url = f"https://{render_host}{WEBHOOK_PATH}"
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(
                    telegram_app.bot.set_webhook(
                        url=webhook_url,
                        secret_token=WEBHOOK_SECRET,
                        drop_pending_updates=True
                    )
                )
                loop.close()
                logger.info(f"✅ Auto webhook to TiDB bot: {webhook_url}")
        except Exception as e:
            logger.error(f"⚠️ Auto webhook failed: {e}")
    
    if os.getenv('RENDER'):
        thread = threading.Thread(target=set_webhook_thread, daemon=True)
        thread.start()

@app.route('/test_tidb')
def test_tidb():
    """"Тест подключения к TiDB"""
    try:
        conn = pymysql.connect(
            host='gateway01.eu-central-1.prod.aws.tidbcloud.com',
            port=4000,
            user='root',
            password='ok0N4vZrAvHrhWL8',
            database='test',
            ssl={'ssl': {'ca': ''}}
        )

        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        result = cursor.fetchone()

        conn.close()

        return jsonify({
            "success": True,
            "message": f"TiDB подключена! Результат: {result}"
        })

except Exception as e:
    return jsonify({
        "success": False,
        "error": str(e)
    }), 500

@app.route('/test_tidb_connection')
def test_tidb_connection():
    """Тест подключения к TiDB из Render"""
    try:
        # Парсим DATABASE_URL
        import re
        import urllib.parse
        
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            return jsonify({"error": "DATABASE_URL не найден"}), 500
        
        # Упрощённый парсинг
        if db_url.startswith('mysql://'):
            db_url = db_url[8:]  # Убираем mysql://
        
        # Разбираем: user:pass@host:port/db
        auth_part, rest = db_url.split('@')
        user, password = auth_part.split(':')
        host_port, database = rest.split('/')
        
        if ':' in host_port:
            host, port = host_port.split(':')
            port = int(port)
        else:
            host = host_port
            port = 4000
        
        # Пробуем подключиться
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            ssl={'ssl': {'ca': ''}},
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        cursor.execute('SELECT VERSION()')
        version = cursor.fetchone()[0]
        
        cursor.execute('SELECT DATABASE()')
        db_name = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            "success": True,
            "message": "✅ TiDB подключена!",
            "version": version,
            "database": db_name,
            "host": host,
            "port": port,
            "user": user
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "database_url": os.getenv('DATABASE_URL', 'не найден')
        }), 500
@app.route('/simple_test')
def simple_test():
    return jsonify({"test": "OK"})


if __name__ == '__main__':
    port = int(os.getenv('PORT', 10000))
    logger.info(f"🚀 TiDB Cloud Bot starting on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
















