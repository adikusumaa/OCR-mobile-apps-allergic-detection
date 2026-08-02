import os
from flask import Flask, send_from_directory, jsonify, request
from flask_cors import CORS

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
FRONTEND_DIR = os.path.abspath(os.path.join(BASE_DIR, '..', 'frontend'))

app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path='')
CORS(app)

@app.route('/', methods=['GET'])
def index():
    return send_from_directory(FRONTEND_DIR, 'index.html')

@app.route('/<path:path>', methods=['GET'])
def static_files(path):
    return send_from_directory(FRONTEND_DIR, path)

@app.route('/api/detect', methods=['POST'])
def detect():
    payload = request.get_json(silent=True) or {}
    input_type = payload.get('inputType', 'Tidak diketahui')
    response = {
        'inputType': input_type,
        'status': 'Selesai',
        'allergens': ['Susu', 'Kacang', 'Gluten'],
        'cause': 'Terdeteksi istilah "casein" dan "whey protein" pada komposisi.',
        'recommendation': 'Periksa daftar bahan dan hindari produk ini jika memiliki alergi terhadap susu atau kacang.',
        'detectedAt': 'backend'
    }
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
