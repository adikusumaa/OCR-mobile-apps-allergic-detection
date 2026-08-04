import base64
import os
from flask import Flask, send_from_directory, jsonify, request
from flask_cors import CORS
from comvis import analyze_image_from_base64

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
FRONTEND_DIR = os.path.abspath(os.path.join(BASE_DIR, '..', 'frontend'))
DIST_DIR = os.path.join(FRONTEND_DIR, 'dist')

app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path='')
CORS(app)

@app.route('/', methods=['GET'])
def index():
    if os.path.exists(os.path.join(DIST_DIR, 'index.html')):
        return send_from_directory(DIST_DIR, 'index.html')
    return send_from_directory(FRONTEND_DIR, 'index.html')

@app.route('/<path:path>', methods=['GET'])
def static_files(path):
    if os.path.exists(os.path.join(DIST_DIR, path)):
        return send_from_directory(DIST_DIR, path)
    if os.path.exists(os.path.join(FRONTEND_DIR, path)):
        return send_from_directory(FRONTEND_DIR, path)
    return index()

@app.route('/api/analyze', methods=['POST'])
def analyze():
    payload = request.get_json(silent=True) or {}
    image_data = payload.get('imageData')
    input_type = payload.get('inputType', 'Tidak diketahui')

    if not image_data:
        return jsonify({
            'status': 'error',
            'message': 'Tidak ada data gambar yang dikirim.',
            'inputType': input_type
        }), 400

    try:
        analysis = analyze_image_from_base64(image_data)
        analysis['inputType'] = input_type
        return jsonify(analysis)
    except ValueError as error:
        return jsonify({
            'status': 'error',
            'message': str(error),
            'inputType': input_type
        }), 400

@app.route('/api/detect', methods=['POST'])
def detect():
    return analyze()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
