# ARCHITECTURE

## Tujuan
Dokumen ini menjelaskan arsitektur sistem prototipe web dan rencana transisi ke Android offline. Fokus pada pemisahan modul, aliran data, dan teknologi yang digunakan.

## Arsitektur Tingkat Tinggi
Aplikasi terdiri dari tiga lapisan utama:

1. Kamera / Input
   - Sumber: kamera HP atau upload gambar dari user
   - Hasil: frame mentah

2. Processing Pipeline
   - Computer Vision (comvis)
   - OCR
   - AI Alergen

3. Presentasi Output
   - Preview frame enhanced
   - Teks OCR
   - Daftar alergen dan uraian risiko

---

## 1. Modul Computer Vision (comvis)
### Tanggung jawab
- Memeriksa kualitas frame
- Mendeteksi blur, glare, dan distorsi perspektif
- Meningkatkan visibilitas teks
- Menolak frame yang buruk

### Komponen
- `frame_quality_analyzer`
- `perspective_corrector`
- `contrast_enhancer`
- `output_feedback`

### Algoritme utama
- varians Laplacian untuk blur
- histogram clipping untuk overexposure/underexposure
- deteksi garis tepi untuk perspektif
- CLAHE / adaptive thresholding

### Data flow
1. frame mentah -> quality analyzer
2. quality pass -> enhancer
3. enhancer -> OCR pipeline
4. quality fail -> UI feedback

---

## 2. Modul OCR
### Tanggung jawab
- Menangkap teks pada kemasan makanan
- Mendukung teks multibahasa dan karakter non-Latin
- Menormalisasi teks hasil OCR

### Komponen
- `ocr_engine`
- `text_postprocessor`
- `language_detector`
- `line_joiner`

### Teknologi
- Prototipe web: PaddleOCR, OpenCV
- Mobile: ONNX Runtime untuk model OCR

### Data flow
1. frame enhanced -> OCR engine
2. raw OCR output -> postprocessor
3. postprocessor -> normalized text JSON
4. normalized text -> AI alergen

---

## 3. Modul AI Alergen
### Tanggung jawab
- Mendeteksi alergen dari teks
- Mengidentifikasi istilah ilmiah dan variasi bahasa
- Menghasilkan penjelasan sebab dan risiko
- Menyediakan output terstruktur

### Komponen
- `allergen_database`
- `rule_engine`
- `model_inference`
- `result_validator`

### Pendekatan
- Hybrid: rule-based matching + model inference kecil
- Validasi hasil menggunakan schema JSON
- Offline lokal pada perangkat target

### Data flow
1. normalized text -> rule_engine
2. candidates -> model_inference
3. hasil -> result_validator
4. final JSON -> frontend/UI

---

## 4. Prototipe Web vs Mobile
### Prototipe Web
- Bahasa: Python
- Interface: Streamlit
- Vision: OpenCV
- OCR: PaddleOCR
- AI: Ollama / local SLM
- Tujuan: validasi pipeline, UI demo, uji bahasa dan OCR

### Mobile Offline
- Framework: Flutter
- OCR runtime: ONNX
- Model AI: llama.cpp atau PyTorch Mobile
- Preprocessing: plugin OpenCV/Flutter
- Tujuan: eksekusi offline, realtime kamera, optimalisasi perangkat

---

## 5. Infrastruktur Data Alergen
### Basis data offline
- Daftar alergen utama: kacang, susu, telur, gluten, ikan, kerang, kedelai, dll.
- Sinonim ilmiah: casein, albumin, gliadin, tropomyosin, dll.
- Variasi bahasa: Melayu, Inggris, Mandarin, Jepang, Korea, Latin, dan typo umum

### Format data
- `allergen_id`
- `name`
- `aliases`
- `language_tags`
- `risk_level`
- `explanation_template`

---

## 6. Interfaces dan Schema Data
### Input layer
- `camera_frame`:
  - `frame_id`
  - `timestamp`
  - `image_data`
  - `sensor_metadata`

### OCR output
- `raw_text`
- `normalized_text`
- `languages_detected`
- `confidence_score`
- `frame_id`

### AI output
- `allergens_detected`
- `matched_terms`
- `risk_score`
- `cause_description`
- `validation_status`

---

## 7. Asumsi dan Batasan
- OCR hanya memproses teks dari label kemasan, bukan teks dari gambar latar belakang kompleks.
- Pengguna dapat menerima prompt untuk memperbaiki posisi kamera.
- Aplikasi berjalan offline; model dan data harus disimpan lokal.
- False negative harus diminimalkan, validasi strict diperlukan.

---

## 8. Migrasi dari Web ke Android
1. Validasi pipeline data dan logika di web.
2. Ekspor model OCR ke format ONNX.
3. Implementasi preprocessing di Flutter.
4. Integrasi OCR dan AI secara lokal.
5. Uji end-to-end pada perangkat Android.
