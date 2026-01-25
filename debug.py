from flask import Flask, jsonify
import os

app = Flask(__name__)

@app.route('/')
def home():
    return "Debug server"

@app.route('/test')
def test():
    return jsonify({
        "DATABASE_URL_exists": bool(os.getenv('DATABASE_URL')),
        "DATABASE_URL": os.getenv('DATABASE_URL', 'не найден')
    })

@app.route('/test_tidb')
def test_tidb():
    return "Test endpoint works"

if __name__ == '__main__':
    app.run(debug=True)
