import pymysql
from pymysql.cursors import DictCursor
import os
import logging
import asyncio
import sys
import threading
import urllib.parse
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
    """Инициализация TiDB Cloud"""
    try:
        if not DATABASE_URL:
            logger.warning("⚠️ DATABASE_URL не задан")
            return None
        
        db_config = parse_tidb_url(DATABASE_URL)
        if not db_config:
            return None
        
        # Тестовое подключение
        test_conn = pymysql.connect(**db_config)
        cursor = test_conn.cursor()
        
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
        
    
        # Используем pymysql.pool вместо pool
        return pymysql.pool.ConnectionPool(  
            size=5,
            maxsize=20,
            **db_config
        )

        
    except Exception as e:
        logger.error(f"❌ Ошибка TiDB: {e}")
        return None

# Инициализация TiDB
db_pool = None

def get_db():
    """Получаем пул соединений (инициализируем при первом вызове)"""
    global db_pool
    if db_pool is None:
        db_pool = init_tidb()
    return db_pool
    
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
    """Сохраняем в таблицу posts"""
    try:
        logger.info(f"🔄 Сохранение в TiDB: {character_name}")
        
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            logger.error("❌ DATABASE_URL не найден")
            return False
        
        parsed = urllib.parse.urlparse(db_url)
        
        conn = pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 4000,
            user=parsed.username,
            password=parsed.password,
            database='test',
            ssl={'ssl': {'ca': ''}},
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        
        # Создаем таблицу posts если нет
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chat_id BIGINT,
                user_id BIGINT,
                username VARCHAR(255),
                character_name VARCHAR(255),
                message_date DATETIME,
                char_count INT,
                points INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Вставляем в posts
        cursor.execute('''
            INSERT INTO posts 
            (chat_id, user_id, username, character_name, message_date, char_count, points)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (chat_id, user_id, username, character_name, message_date, char_count, points))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Успешно сохранено в posts: {character_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения в posts: {e}")
        return False

async def get_stats_from_db_async(chat_id=None, user_id=None, date_filter=None):
    """Асинхронная версия чтения из posts таблицы"""
    try:
        # Делаем синхронный вызов в отдельном потоке
        loop = asyncio.get_event_loop()
        
        # Синхронная функция для базы
        def sync_get_stats():
            return get_stats_from_db(chat_id, user_id, date_filter)
        
        # Выполняем в потоке
        stats = await loop.run_in_executor(None, sync_get_stats)
        return stats
        
    except Exception as e:
        print(f"❌ Ошибка в get_stats_from_db_async: {e}")
        return None


def get_stats_from_db(chat_id=None, user_id=None, date_filter=None):
    """Читаем из таблицы posts"""
    try:
        logger.info(f"📊 Чтение из posts: chat={chat_id}, user={user_id}")
        
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            logger.error("❌ DATABASE_URL не найден")
            return None
        
        parsed = urllib.parse.urlparse(db_url)
        
        conn = pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 4000,
            user=parsed.username,
            password=parsed.password,
            database='test',
            ssl={'ssl': {'ca': ''}},
            connect_timeout=10
        )
        
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        query = "SELECT * FROM posts WHERE 1=1"
        params = []
        
        if chat_id:
            query += " AND chat_id = %s"
            params.append(chat_id)
        
        if user_id:
            query += " AND user_id = %s"
            params.append(user_id)
        
        if date_filter == "today":
            query += " AND DATE(message_date) = CURDATE()"
        
        query += " ORDER BY message_date DESC"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        conn.close()
        
        logger.info(f"📊 Найдено в posts: {len(results)} записей")
        return results
        
    except Exception as e:
        logger.error(f"❌ Ошибка чтения из posts: {e}")
        return None

def convert_posts_to_old_format(raw_stats):
    """Преобразует сырые данные в старый формат"""
    if not raw_stats:
        return []
    
    from collections import defaultdict
    import json
    
    user_data = defaultdict(lambda: {
        'username': '',
        'posts': 0,
        'chars': 0,
        'points': 0,
        'characters': {}
    })
    
    for stat in raw_stats:
        user_id = stat['user_id']
        username = stat.get('username', f'user_{user_id}')
        char_name = stat.get('character_name', 'Неизвестно')
        char_count = stat.get('char_count', 0)
        points = stat.get('points', 0)
        
        user_data[user_id]['username'] = username
        user_data[user_id]['posts'] += 1
        user_data[user_id]['chars'] += char_count
        user_data[user_id]['points'] += points
        
        if char_name not in user_data[user_id]['characters']:
            user_data[user_id]['characters'][char_name] = {
                'posts': 0, 'chars': 0, 'points': 0
            }
        
        user_data[user_id]['characters'][char_name]['posts'] += 1
        user_data[user_id]['characters'][char_name]['chars'] += char_count
        user_data[user_id]['characters'][char_name]['points'] += points
    
    results = []
    for user_id, data in user_data.items():
        characters_list = []
        for char_name, char_data in data['characters'].items():
            characters_list.append({
                'name': char_name,
                'posts': char_data['posts'],
                'chars': char_data['chars'],
                'points': char_data['points']
            })
        
        results.append((
            user_id,
            data['username'],
            json.dumps(characters_list, ensure_ascii=False),
            data['posts'],
            data['chars'],
            data['points'],
            len(characters_list)
        ))
    
    results.sort(key=lambda x: x[5], reverse=True)
    return results


async def get_user_stats_tidb(chat_id, period='month'):
    """ОБНОВЛЕННАЯ версия - работает с таблицей posts"""
    try:
        print(f"🔍 DEBUG get_user_stats_tidb: начал, chat_id={chat_id}, period={period}")
        
        # Получаем все посты для этого чата
        all_posts = await get_stats_from_db_async(chat_id=chat_id)
        print(f"🔍 DEBUG: all_posts получено: {len(all_posts) if all_posts else 0}")
        
        if not all_posts:
            print(f"🔍 DEBUG: all_posts пустой, возвращаем []")
            return []
        
        # Фильтруем по периоду
        from datetime import datetime, timedelta
        now = datetime.now()
        
        filtered_posts = []
        for post in all_posts:
            post_date = post.get('message_date')
            
            if not post_date:
                print(f"🔍 DEBUG: у поста нет message_date: {post}")
                continue
                
            # Преобразуем строку в datetime если нужно
            if isinstance(post_date, str):
                try:
                    post_date = datetime.fromisoformat(post_date.replace('Z', '+00:00'))
                except Exception as e:
                    print(f"🔍 DEBUG: ошибка преобразования даты: {e}")
                    continue
            
            # Применяем фильтр по периоду
            if period == 'today':
                if post_date.date() != now.date():
                    continue
            elif period == 'week':
                week_ago = now - timedelta(days=7)
                if post_date < week_ago:
                    continue
            elif period == 'month':
                month_ago = now - timedelta(days=30)
                if post_date < month_ago:
                    continue
            # Для 'all' не фильтруем
            
            filtered_posts.append(post)
        
        print(f"🔍 DEBUG: после фильтрации filtered_posts: {len(filtered_posts)}")
        
        if filtered_posts:
            print(f"🔍 DEBUG: первый пост для конвертации: {filtered_posts[0]}")
        
        # Используем нашу функцию преобразования
        result = convert_posts_to_old_format(filtered_posts)
        
        print(f"🔍 DEBUG: convert_posts_to_old_format вернул: {len(result) if result else 0} записей")
        
        if result:
            print(f"🔍 DEBUG: первый результат: {result[0]}")
            print(f"🔍 DEBUG: тип первого результата: {type(result[0])}")
            print(f"🔍 DEBUG: длина первого результата: {len(result[0])}")
        
        return result
        
    except Exception as e:
        print(f"❌ Ошибка get_user_stats_tidb: {e}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
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
        "🤖 Бот со статистикой!\n\n"
        "📝 Как использовать:\n"
        "1. Пиши пост, где ПЕРВАЯ строка - имя персонажа\n"
        "2. Бот сохранит пост\n"
        "3. Используй команды\n\n"
        "📊 Команды:\n"
        "/stats [period] - статистика\n"
        "/top [period] - топ-10\n"
        "/mystats - личная статистика\n"
        "[period] - today, week, month, all"
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
    """Исправленная версия БЕЗ db_pool"""
    try:
        print(f"🚨 mystats_command вызвана от {update.effective_user.id}")
        
        if update.message.chat.type == 'private':
            await update.message.reply_text("ℹ️ Эта команда работает только в группах!")
            return
        
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        username = update.effective_user.username or update.effective_user.first_name
        display_name = f"@{username}" if update.effective_user.username else username
        
        # Используем НОВУЮ функцию вместо db_pool
        print(f"🚨 Получаю посты для user_id={user_id}, chat_id={chat_id}")
        all_posts = await get_stats_from_db_async(chat_id=chat_id, user_id=user_id)
        
        print(f"🚨 Найдено постов: {len(all_posts) if all_posts else 0}")
        
        if not all_posts:
            await update.message.reply_text(
                f"📊 ВАША СТАТИСТИКА {display_name.upper()}\n\n"
                f"📭 У вас пока нет постов в базе данных!"
            )
            return
        
        # Преобразуем в формат статистики
        user_stats = convert_posts_to_old_format(all_posts)
        
        if not user_stats or len(user_stats) == 0:
            await update.message.reply_text("❌ Ошибка обработки данных")
            return
        
        # Берем статистику текущего пользователя
        if len(user_stats) > 0:
            _, _, characters_json, posts, chars, points, char_count = user_stats[0]
            
            posts_word = decline_posts(posts)
            points_word = decline_points(points)
            
            text = f"📊 ВАША СТАТИСТИКА {display_name.upper()} (TiDB):\n\n"
            
            # Парсим персонажей
            if characters_json and characters_json != 'null':
                try:
                    characters = json.loads(characters_json)
                    if characters:
                        # Сортируем персонажей по очкам
                        characters.sort(key=lambda x: x.get('points', 0), reverse=True)
                        
                        for char in characters:
                            char_name = char.get('name', 'Неизвестно').title()
                            char_posts = char.get('posts', 0)
                            char_chars = char.get('chars', 0)
                            char_points = char.get('points', 0)
                            
                            char_posts_word = decline_posts(char_posts)
                            char_points_word = decline_points(char_points)
                            
                            text += f"🎭 {char_name}:\n"
                            text += f"   📝 {char_posts} {char_posts_word}, {format_number(char_chars)} симв., {char_points} {char_points_word}\n\n"
                except Exception as e:
                    print(f"❌ Ошибка парсинга персонажей: {e}")
                    text += "🎭 Персонажи: данные не доступны\n\n"
            
            total_posts_word = decline_posts(posts)
            total_points_word = decline_points(points)
            
            text += f"📈 ВАШИ ИТОГИ:\n"
            text += f"• Персонажей: {char_count}\n"
            text += f"• Постов: {posts} {total_posts_word}\n"
            text += f"• Символов: {format_number(chars)}\n"
            text += f"• Очков: {points} {total_points_word}"
            
            # Лучший персонаж
            if characters and len(characters) > 0:
                best_char = characters[0]
                best_points_word = decline_points(best_char['points'])
                text += f"\n\n🏆 ВАШ ЛУЧШИЙ ПЕРСОНАЖ:\n"
                text += f"{best_char['name'].title()} - {best_char['points']} {best_points_word}"
            
            await update.message.reply_text(text)
        else:
            await update.message.reply_text("❌ Не удалось получить статистику")
            
    except Exception as e:
        print(f"❌ Ошибка в mystats_command: {e}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        await update.message.reply_text(f"❌ Ошибка получения статистики")

async def clear_posts_from_db(chat_id, period='all'):
    """Удаляет посты из базы данных"""
    try:
        print(f"🗑️ Очистка постов: chat={chat_id}, period={period}")
        
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print("❌ DATABASE_URL не найден")
            return -1
        
        parsed = urllib.parse.urlparse(db_url)
        
        conn = pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 4000,
            user=parsed.username,
            password=parsed.password,
            database='test',
            ssl={'ssl': {'ca': ''}},
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        
        # Строим условие WHERE
        where_clause = "WHERE chat_id = %s"
        params = [chat_id]
        
        if period == 'today':
            where_clause += " AND DATE(message_date) = CURDATE()"
        elif period == 'week':
            where_clause += " AND message_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)"
        elif period == 'month':
            where_clause += " AND message_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)"
        # Для 'all', 'да', 'yes', 'confirm' - удаляем все
        
        # Сначала считаем сколько будет удалено
        count_query = f"SELECT COUNT(*) FROM posts {where_clause}"
        cursor.execute(count_query, params)
        count_to_delete = cursor.fetchone()[0]
        
        if count_to_delete == 0:
            conn.close()
            print(f"🗑️ Нет постов для удаления")
            return 0
        
        # Удаляем посты
        delete_query = f"DELETE FROM posts {where_clause}"
        cursor.execute(delete_query, params)
        
        conn.commit()
        conn.close()
        
        print(f"🗑️ Удалено {count_to_delete} постов")
        return count_to_delete
        
    except Exception as e:
        print(f"❌ Ошибка очистки постов: {e}")
        return -1

async def clear_stats_command(update: Update, context: CallbackContext):
    """Очистка статистики (только для админов)"""
    try:
        print(f"🚨 clear_stats вызвана от {update.effective_user.id}")
        
        # Проверяем что пользователь админ
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # Получаем информацию о пользователе в чате
        chat_member = await update.effective_chat.get_member(user_id)
        
        # Разрешаем только создателям и админам
        if chat_member.status not in ['creator', 'administrator']:
            await update.message.reply_text(
                "⛔ Эта команда только для администраторов чата!"
            )
            return
        
        # Запрашиваем подтверждение
        args = context.args if context.args else []

        if not args:
            # Нет аргументов - показываем предупреждение
            await update.message.reply_text( 
                "⚠️ **ВНИМАНИЕ: Очистка статистики**\n\n"
                "Эта команда УДАЛИТ ВСЕ данные статистики из базы данных.\n"
                "Действие необратимо!\n\n"
                "Для подтверждения напишите:\n"
                "`/clearstats да`\n\n"
                "Или укажите период:\n"
                "`/clearstats today` - удалить только сегодняшние посты\n"
                "`/clearstats week` - удалить посты за неделю\n"
                "`/clearstats month` - удалить посты за месяц"
            )
            return
            
        # Получаем первый аргумент
        arg = args[0].lower()
        
        # Если это команда подтверждения или период - пропускаем предупреждение
        if arg in ['да', 'yes', 'confirm', 'today', 'week', 'month']:
            # Это валидная команда очистки, не показываем предупреждение
            pass
        else:
            # Неизвестная команда - показываем предупреждение
            await update.message.reply_text( 
                "⚠️ **ВНИМАНИЕ: Очистка статистики**\n\n"
                "Эта команда УДАЛИТ ВСЕ данные статистики из базы данных.\n"
                "Действие необратимо!\n\n"
                "Для подтверждения напишите:\n"
                "`/clearstats да`\n\n"
                "Или укажите период:\n"
                "`/clearstats today` - удалить только сегодняшние посты\n"
                "`/clearstats week` - удалить посты за неделю\n"
                "`/clearstats month` - удалить посты за месяц"
            )
            return
        
        # Получаем период очистки
        period = args[0].lower()
        
        # Функция очистки
        deleted_count = await clear_posts_from_db(chat_id, period)
        
        if deleted_count >= 0:
            period_text = {
                'да': 'все посты',
                'yes': 'все посты',
                'confirm': 'все посты',
                'today': 'посты за сегодня',
                'week': 'посты за неделю',
                'month': 'посты за месяц'
            }.get(period, period)
            
            await update.message.reply_text(
                f"✅ Статистика очищена!\n"
                f"🗑️ Удалено {deleted_count} {decline_posts(deleted_count)} ({period_text})."
            )
        else:
            await update.message.reply_text("❌ Ошибка при очистке статистики")
            
    except Exception as e:
        print(f"❌ Ошибка в clear_stats_command: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)[:100]}")

async def backup_command(update: Update, context: CallbackContext):
    """Создает и отправляет резервную копию статистики"""
    try:
        # Проверка прав админа
        chat_member = await update.effective_chat.get_member(update.effective_user.id)
        if chat_member.status not in ['creator', 'administrator']:
            await update.message.reply_text("⛔ Только для администраторов!")
            return
        
        chat_id = update.effective_chat.id
        chat_title = update.effective_chat.title or f"Chat_{chat_id}"
        
        await update.message.reply_text("📦 Создаю резервную копию...")
        
        # Создаем резервную копию
        backup_data = await create_backup_data(chat_id)
        
        if not backup_data:
            await update.message.reply_text("❌ Нет данных для резервного копирования")
            return
        
        # Сохраняем в файл JSON (лучше чем CSV для восстановления)
        import json
        from datetime import datetime
        
        filename = f"backup_{chat_title}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2, default=str)
        
        # Отправляем файл
        with open(filename, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=filename,
                caption=f"📦 Резервная копия статистики\n"
                       f"Чат: {chat_title}\n"
                       f"Записей: {len(backup_data.get('posts', []))}\n"
                       f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
        
        # Удаляем временный файл
        import os
        os.remove(filename)
        
        await update.message.reply_text(
            "✅ Резервная копия создана!\n\n"
            "📌 Для восстановления:\n"
            "1. Сохраните этот файл\n"
            "2. Отправьте его боту командой /restore"
        )
        
    except Exception as e:
        print(f"❌ Ошибка backup_command: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)[:100]}")

async def create_backup_data(chat_id):
    """Создает структуру данных для резервного копирования"""
    try:
        db_url = os.getenv('DATABASE_URL')
        parsed = urllib.parse.urlparse(db_url)
        
        conn = pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 4000,
            user=parsed.username,
            password=parsed.password,
            database='test',
            ssl={'ssl': {'ca': ''}}
        )
        
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Получаем все посты
        cursor.execute('''
            SELECT * FROM posts 
            WHERE chat_id = %s 
            ORDER BY id ASC
        ''', (chat_id,))
        
        posts = cursor.fetchall()
        conn.close()
        
        # Создаем структуру данных
        backup_data = {
            'chat_id': chat_id,
            'backup_date': datetime.now().isoformat(),
            'total_posts': len(posts),
            'posts': posts
        }
        
        return backup_data
        
    except Exception as e:
        print(f"❌ Ошибка create_backup_data: {e}")
        return None

async def restore_command(update: Update, context: CallbackContext):
    """Восстанавливает статистику из резервной копии"""
    try:
        # Проверка прав админа
        chat_member = await update.effective_chat.get_member(update.effective_user.id)
        if chat_member.status not in ['creator', 'administrator']:
            await update.message.reply_text("⛔ Только для администраторов!")
            return
        
        # Проверяем что есть документ
        if not update.message.document:
            await update.message.reply_text(
                "📤 Для восстановления:\n\n"
                "1. Создайте резервную копию командой /backup\n"
                "2. Сохраните файл\n"
                "3. Отправьте файл боту с командой /restore\n\n"
                "Или отправьте файл и напишите:\n"
                "`/restore`"
            )
            return
        
        document = update.message.document
        
        # Проверяем что это JSON файл
        if not document.file_name.endswith('.json'):
            await update.message.reply_text(
                "❌ Файл должен быть в формате JSON\n"
                "(создайте командой /backup)"
            )
            return
        
        await update.message.reply_text("🔄 Загружаю и проверяю файл...")
        
        # Скачиваем файл
        file = await document.get_file()
        temp_file = f"temp_restore_{document.file_id}.json"
        await file.download_to_drive(temp_file)
        
        # Читаем и проверяем файл
        import json
        with open(temp_file, 'r', encoding='utf-8') as f:
            try:
                backup_data = json.load(f)
            except json.JSONDecodeError:
                await update.message.reply_text("❌ Ошибка чтения файла. Неверный формат JSON")
                import os
                os.remove(temp_file)
                return
        
        # Проверяем структуру данных
        required_keys = ['chat_id', 'backup_date', 'posts']
        for key in required_keys:
            if key not in backup_data:
                await update.message.reply_text(f"❌ Неверный формат файла: нет ключа '{key}'")
                import os
                os.remove(temp_file)
                return
        
        # Показываем информацию о бэкапе
        chat_id = backup_data['chat_id']
        backup_date = backup_data.get('backup_date', 'неизвестно')
        total_posts = len(backup_data.get('posts', []))
        
        info_text = (
            f"📋 Информация о резервной копии:\n"
            f"• Чат ID: {chat_id}\n"
            f"• Дата создания: {backup_date}\n"
            f"• Записей: {total_posts}\n\n"
        )
        
        # Показываем пример данных
        if total_posts > 0:
            sample = backup_data['posts'][0]
            info_text += f"Пример записи:\n"
            info_text += f"• Пользователь: {sample.get('username', 'N/A')}\n"
            info_text += f"• Персонаж: {sample.get('character_name', 'N/A')}\n"
            info_text += f"• Дата: {sample.get('message_date', 'N/A')}\n"
        
        # Запрашиваем подтверждение
        await update.message.reply_text(
            info_text + "\n" +
            "⚠️ **ВНИМАНИЕ:**\n"
            "При восстановлении СУЩЕСТВУЮЩИЕ данные будут:\n"
            "1. УДАЛЕНЫ (для этого чата)\n"
            "2. ЗАМЕНЕНЫ на данные из резервной копии\n\n"
            "Для подтверждения напишите:\n"
            "`/dorestore confirm`"
        )
        
        # Сохраняем данные в контексте для следующего шага
        context.user_data['restore_data'] = backup_data
        context.user_data['restore_file'] = temp_file
        
        import os
        os.remove(temp_file)
        
    except Exception as e:
        print(f"❌ Ошибка restore_command: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)[:100]}")

async def restore_command(update: Update, context: CallbackContext):
    """Восстанавливает статистику из резервной копии"""
    try:
        print(f"🔄 restore_command вызвана от {update.effective_user.id}")
        
        # Проверка прав админа
        chat_member = await update.effective_chat.get_member(update.effective_user.id)
        if chat_member.status not in ['creator', 'administrator']:
            await update.message.reply_text("⛔ Только для администраторов!")
            return
        
        # Проверяем есть ли сохраненный файл
        if 'pending_restore_file' not in context.user_data:
            await update.message.reply_text(
                "📤 Для восстановления:\n\n"
                "1. Создайте резервную копию командой `/backup`\n"
                "2. Сохраните файл\n"
                "3. Отправьте файл боту\n"
                "4. Напишите `/restore`\n\n"
                "Или отправьте файл и напишите:\n"
                "`/restore`"
            )
            return
        
        file_info = context.user_data['pending_restore_file']
        
        await update.message.reply_text("🔄 Загружаю и проверяю файл...")
        
        # Скачиваем файл
        document = await context.bot.get_file(file_info['file_id'])
        temp_file = f"temp_restore_{file_info['file_id']}.json"
        await document.download_to_drive(temp_file)
        
        # Читаем файл
        import json
        with open(temp_file, 'r', encoding='utf-8') as f:
            try:
                backup_data = json.load(f)
            except json.JSONDecodeError as e:
                await update.message.reply_text(f"❌ Ошибка чтения JSON: {e}")
                import os
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                context.user_data.pop('pending_restore_file', None)
                return
        
        # Проверяем структуру
        required_keys = ['chat_id', 'backup_date', 'posts']
        for key in required_keys:
            if key not in backup_data:
                await update.message.reply_text(f"❌ Неверный формат файла: нет ключа '{key}'")
                import os
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                context.user_data.pop('pending_restore_file', None)
                return
        
        # Показываем информацию
        chat_id = backup_data['chat_id']
        backup_date = backup_data.get('backup_date', 'неизвестно')
        total_posts = len(backup_data.get('posts', []))
        
        from datetime import datetime
        try:
            backup_dt = datetime.fromisoformat(backup_date.replace('Z', '+00:00'))
            backup_date_str = backup_dt.strftime('%d.%m.%Y %H:%M')
        except:
            backup_date_str = backup_date
        
        info_text = (
            f"📋 Информация о резервной копии:\n"
            f"• Чат ID: {chat_id}\n"
            f"• Дата создания: {backup_date_str}\n"
            f"• Записей: {total_posts}\n\n"
        )
        
        # Показываем пример
        if total_posts > 0:
            sample = backup_data['posts'][0]
            info_text += f"Пример записи:\n"
            info_text += f"• Пользователь: {sample.get('username', 'N/A')}\n"
            info_text += f"• Персонаж: {sample.get('character_name', 'N/A')}\n"
            info_text += f"• Дата: {sample.get('message_date', 'N/A')[:10]}\n"
        
        # Сохраняем данные
        context.user_data['restore_data'] = backup_data
        
        # Удаляем временный файл
        import os
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        # Удаляем информацию о файле
        context.user_data.pop('pending_restore_file', None)
        
        # Запрашиваем подтверждение
        await update.message.reply_text(
            info_text + "\n" +
            "⚠️ **ВНИМАНИЕ:**\n"
            "При восстановлении СУЩЕСТВУЮЩИЕ данные будут:\n"
            "1. УДАЛЕНЫ (для этого чата)\n"
            "2. ЗАМЕНЕНЫ на данные из резервной копии\n\n"
            "Для подтверждения напишите:\n"
            "`/dorestore confirm`"
        )
        
    except Exception as e:
        print(f"❌ Ошибка restore_command: {e}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)[:100]}")

async def do_restore_command(update: Update, context: CallbackContext):
    """Выполняет восстановление после подтверждения"""
    try:
        # Проверка прав админа
        chat_member = await update.effective_chat.get_member(update.effective_user.id)
        if chat_member.status not in ['creator', 'administrator']:
            await update.message.reply_text("⛔ Только для администраторов!")
            return
        
        # Проверяем подтверждение
        args = context.args if context.args else []
        if not args or args[0].lower() != 'confirm':
            await update.message.reply_text(
                "❌ Требуется подтверждение!\n"
                "Напишите: `/dorestore confirm`"
            )
            return
        
        # Проверяем что есть данные для восстановления
        if 'restore_data' not in context.user_data:
            await update.message.reply_text(
                "❌ Нет данных для восстановления\n"
                "Сначала отправьте файл командой /restore"
            )
            return
        
        backup_data = context.user_data['restore_data']
        chat_id = backup_data['chat_id']
        
        await update.message.reply_text("🔄 Начинаю восстановление...")
        
        # Выполняем восстановление
        result = await restore_from_backup(backup_data)
        
        if result['success']:
            restored = result['restored_count']
            errors = result['error_count']
            
            message = (
                f"✅ Восстановление завершено!\n\n"
                f"📊 Результаты:\n"
                f"• Успешно восстановлено: {restored} записей\n"
                f"• Ошибок: {errors}\n"
                f"• Удалено старых записей: {result.get('deleted_count', 0)}\n\n"
            )
            
            if errors > 0:
                message += f"⚠️ {errors} записей не восстановлено (см. логи)\n"
            
            message += f"🔄 Проверьте статистику командой /stats all"
            
            await update.message.reply_text(message)
            
            # Очищаем данные
            context.user_data.pop('restore_data', None)
            context.user_data.pop('restore_file', None)
            
        else:
            await update.message.reply_text(
                f"❌ Ошибка восстановления:\n{result.get('error', 'Неизвестная ошибка')}"
            )
        
    except Exception as e:
        print(f"❌ Ошибка do_restore_command: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)[:100]}")

async def restore_from_backup(backup_data):
    """Восстанавливает данные из резервной копии в базу"""
    try:
        chat_id = backup_data['chat_id']
        posts = backup_data.get('posts', [])
        
        if not posts:
            return {'success': False, 'error': 'Нет данных для восстановления'}
        
        db_url = os.getenv('DATABASE_URL')
        parsed = urllib.parse.urlparse(db_url)
        
        conn = pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 4000,
            user=parsed.username,
            password=parsed.password,
            database='test',
            ssl={'ssl': {'ca': ''}}
        )
        
        cursor = conn.cursor()
        
        # 1. Удаляем старые данные для этого чата
        cursor.execute("DELETE FROM posts WHERE chat_id = %s", (chat_id,))
        deleted_count = cursor.rowcount
        
        # 2. Восстанавливаем новые данные
        restored_count = 0
        error_count = 0
        
        for post in posts:
            try:
                cursor.execute('''
                    INSERT INTO posts 
                    (chat_id, user_id, username, character_name, message_date, char_count, points, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    post.get('chat_id'),
                    post.get('user_id'),
                    post.get('username'),
                    post.get('character_name'),
                    post.get('message_date'),
                    post.get('char_count', 0),
                    post.get('points', 0),
                    post.get('created_at')
                ))
                restored_count += 1
            except Exception as e:
                print(f"❌ Ошибка восстановления записи: {e}")
                error_count += 1
        
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'restored_count': restored_count,
            'error_count': error_count,
            'deleted_count': deleted_count,
            'total_in_backup': len(posts)
        }
        
    except Exception as e:
        print(f"❌ Ошибка restore_from_backup: {e}")
        return {'success': False, 'error': str(e)}

async def handle_document(update: Update, context: CallbackContext):
    """Обработка отправленных документов (для восстановления)"""
    try:
        print(f"📄 Документ получен: {update.message.document.file_name}")
        
        # Проверяем что это JSON файл для восстановления
        if update.message.document.file_name.endswith('.json'):
            # Сохраняем информацию о файле в контекст
            context.user_data['pending_restore_file'] = {
                'file_id': update.message.document.file_id,
                'file_name': update.message.document.file_name,
                'chat_id': update.effective_chat.id,
                'user_id': update.effective_user.id
            }
            
            await update.message.reply_text(
                f"📦 Файл '{update.message.document.file_name}' получен!\n\n"
                f"Для восстановления статистики напишите:\n"
                f"`/restore`"
            )
        else:
            await update.message.reply_text(
                "❌ Файл должен быть в формате JSON\n"
                "(создайте командой /backup)"
            )
            
    except Exception as e:
        print(f"❌ Ошибка handle_document: {e}")
        await update.message.reply_text(f"❌ Ошибка обработки файла: {str(e)[:100]}")

@app.route('/debug')
def debug_info():
    """Показать диагностическую информацию"""
    try:
        # Проверяем DATABASE_URL
        db_url = os.getenv('DATABASE_URL')
        
        info = {
            "bot_status": "ready",
            "database_connection": "not_connected",
            "free_storage": "5 GB",
            "status": "online",
            "debug_details": {
                "DATABASE_URL_exists": bool(db_url),
                "DATABASE_URL_preview": db_url[:50] + "..." if db_url and len(db_url) > 50 else db_url,
                "python_version": os.sys.version,
                "current_time": datetime.now().isoformat()
            }
        }
        
        # Пробуем подключиться к TiDB
        if db_url:
            try:
                parsed = urllib.parse.urlparse(db_url)
                
                # Формируем данные для теста
                connection_params = {
                    'host': parsed.hostname,
                    'port': parsed.port or 4000,
                    'user': parsed.username,
                    'password': '****' if parsed.password else None,
                    'database': parsed.path[1:] if parsed.path else 'test'
                }
                
                # Тестируем подключение
                test_conn = pymysql.connect(
                    host=parsed.hostname,
                    port=parsed.port or 4000,
                    user=parsed.username,
                    password=parsed.password,
                    database=parsed.path[1:] if parsed.path else 'test',
                    ssl={'ssl': {'ca': ''}},
                    connect_timeout=5
                )
                
                test_conn.close()
                info["database_connection"] = "connected"
                info["debug_details"]["connection_test"] = "success"
                
            except Exception as e:
                info["debug_details"]["connection_error"] = str(e)
                info["debug_details"]["error_type"] = type(e).__name__
        
        return jsonify(info)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
# Регистрация обработчиков
if telegram_app:
    telegram_app.add_handler(CommandHandler("start", start_command))
    telegram_app.add_handler(CommandHandler("stats", stats_command))
    telegram_app.add_handler(CommandHandler("top", top_command))
    telegram_app.add_handler(CommandHandler("mystats", mystats_command))
    telegram_app.add_handler(CommandHandler("clearstats", clear_stats_command))
    telegram_app.add_handler(CommandHandler("backup", backup_command))
    telegram_app.add_handler(CommandHandler("restore", restore_command)) 
    telegram_app.add_handler(CommandHandler("dorestore", do_restore_command))
    telegram_app.add_handler(MessageHandler(filters.Document.ALL & ~filters.COMMAND, handle_document))
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









































