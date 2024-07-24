from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

def get_db_connection():
    # Membuat koneksi ke database SQLite
    conn = sqlite3.connect('data_lake.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/data', methods=['GET'])
def get_data():
    # Membuat koneksi ke database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Implementasi pagination
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    offset = (page - 1) * per_page
    
    # Mengambil data dari tabel processed_data
    cursor.execute('SELECT * FROM processed_data LIMIT ? OFFSET ?', (per_page, offset))
    rows = cursor.fetchall()
    
    # Mengonversi data ke format dictionary
    data = [dict(row) for row in rows]
    
    # Menutup koneksi
    conn.close()
    
    # Mengembalikan data dalam format JSON
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
