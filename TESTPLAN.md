# TESTPLAN

## Tujuan
Menetapkan strategi pengujian untuk memastikan setiap modul berfungsi, pipeline terintegrasi stabil, dan hasil deteksi alergen valid serta dapat diandalkan.

## 1. Testing Scope
- Unit test untuk modul `comvis`, `OCR`, dan `AI`
- Integrasi end-to-end pipeline
- Validasi output JSON schema
- Pengujian kondisi offline
- Pengujian multilingual, typo, dan istilah ilmiah

---

## 2. Modul Testing
### 2.1 COMVIS
#### Tujuan
Memastikan frame input diolah dengan benar dan frame buruk ditolak.

#### Test Cases
- `TC-COMVIS-01`: Frame jelas dengan teks kontras tinggi -> status `ready`
- `TC-COMVIS-02`: Frame blur berat -> status `retry`
- `TC-COMVIS-03`: Glare tinggi terdeteksi -> status `glare_detected`
- `TC-COMVIS-04`: Permukaan melengkung dikoreksi tanpa artefak berat

#### Validation
- Output metadata kualitas frame lengkap
- Frame hasil enhancement tersedia
- Log `COMVIS` mencatat setiap tindakan

### 2.2 OCR
#### Tujuan
Memastikan teks dapat diekstrak dari hasil frame enhanced, termasuk teks multilanguage dan kata terpecah.

#### Test Cases
- `TC-OCR-01`: Teks campuran Latin dan Mandarin dibaca
- `TC-OCR-02`: Potongan kata akhir baris digabungkan
- `TC-OCR-03`: Noise karakter diabaikan
- `TC-OCR-04`: Confidence OCR disediakan per baris

#### Validation
- Field `normalized_text` berisi hasil normalisasi
- `language_hints` benar
- Log `OCR` mencatat koreksi line-break/hyphenation

### 2.3 AI Alergen
#### Tujuan
Memastikan deteksi alergen tepat, multi-alergen, dan penjelasan sebab dibuat.

#### Test Cases
- `TC-AI-01`: "casein" dan "soya" dikenali sebagai dua alergen
- `TC-AI-02`: Istilah ilmiah "albumin" mengarah ke alergen telur
- `TC-AI-03`: Hasil JSON valid menurut schema
- `TC-AI-04`: False positive dicegah dengan validasi rule

#### Validation
- Semua field output tersedia
- `risk_score` dalam rentang 0.0-1.0
- `validation_status` = `pass`

---

## 3. Integrasi End-to-End
### Tujuan
Memastikan alur lengkap dari input kamera hingga output alergen bekerja tanpa koneksi internet.

### Test Cases
- `TC-E2E-01`: Kamera -> comvis -> OCR -> AI menghasilkan output lengkap
- `TC-E2E-02`: Frame kualitas buruk menghasilkan prompt perbaikan tanpa memproses OCR
- `TC-E2E-03`: Teks bahasa campur diproses end-to-end
- `TC-E2E-04`: Aplikasi offline tanpa koneksi external tetap berjalan

---

## 4. Pengujian Bahasa & Tipografi
### Tujuan
Memastikan sistem memperlakukan berbagai bahasa, alfabet non-Latin, dan teks ilmiah.

### Test Cases
- `TC-LANG-01`: Mandarin / Jepang / Korea terbaca dalam OCR
- `TC-LANG-02`: Bahasa Melayu dan Inggris bersama-sama diidentifikasi
- `TC-LANG-03`: Typo umum pada istilah alergen disesuaikan atau dikenali
- `TC-LANG-04`: Istilah ilmiah disambungkan ke alergen relevan

---

## 5. Validasi Output & Log
### Tujuan
Memastikan setiap proses menghasilkan output terstruktur dan log yang konsisten.

### Validation Points
- JSON schema untuk AI output
- Log memiliki format `{LOG} [timestamp] [module] event metadata`
- Kesalahan ditangani dengan status `error` dan pesan deskriptif
- Output UI dapat ditelusuri kembali ke frame_id

---

## 6. Data Test Sample
### Kategori Data
- Label kemasan makanan impor dengan teks campuran
- Label dengan glare dan low-light
- Label dengan istilah ilmiah alergen
- Label dengan pemenggalan kata

### Contoh Kasus
- `sample_01`: Label susu, kacang, dan gluten
- `sample_02`: Label produk impor dengan Mandarin dan Inggris
- `sample_03`: Label dengan komposisi kecil dan font tipis
- `sample_04`: Label dengan glare reflektif

---

## 7. Pelaporan Bug dan Tindak Lanjut
### Ketentuan
- Setiap bug harus mencantumkan modul terkait dan input sample
- Termasuk log trace dari `COMVIS`, `OCR`, dan `AI`
- Prioritas bug berdasarkan risiko kesehatan dan stabilitas pipeline

### Contoh Laporan
- `BUG-001`: False negative alergen "casein" dari istilah ilmiah yang salah normalisasi
- `BUG-002`: OCR gagal menggabungkan kata terpotong pada baris multiline
- `BUG-003`: Output AI menghasilkan status `error` meskipun `normalized_text` valid

---

## 8. Rencana Pengujian Mobile
### Tujuan
Menjamin komponen Flutter dan model offline bekerja di Android.

### Test Cases
- `TC-MOB-01`: Kamera realtime terhubung dan frame dikirim ke comvis
- `TC-MOB-02`: ONNX OCR model memproses frame lokal
- `TC-MOB-03`: AI inference bekerja offline pada perangkat
- `TC-MOB-04`: UI menampilkan daftar alergen dan cause

---

## 9. Checklist Implementasi
- [ ] Modul `comvis` siap dan diuji
- [ ] Modul `OCR` siap dan diuji
- [ ] Modul `AI` siap dan diuji
- [ ] Pipeline end-to-end diuji
- [ ] Dokumentasi schema output selesai
- [ ] Demo web siap
- [ ] Rencana migrasi Android siap
