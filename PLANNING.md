# PLANNING

## Tujuan Umum
Membuat aplikasi prototipe berbasis web untuk mendeteksi alergen pada bahan produk makanan secara offline. Hasil prototipe ini akan menjadi dasar untuk migrasi ke aplikasi Android native yang berjalan lokal di perangkat.

## Ringkasan Sistem
Aplikasi terdiri dari tiga modul utama:
1. Computer Vision (comvis)
2. OCR
3. AI Alergen

Data mengalir dari kamera ke preprocessing citra, kemudian ke OCR untuk ekstraksi teks, lalu ke modul AI untuk identifikasi alergen.

## Status Saat Ini
- [x] UI prototipe web minimum viable sudah selesai: live preview kamera, upload overlay, dan hasil deteksi yang muncul setelah proses selesai.
- [ ] Modul Computer Vision (comvis) masih dalam perancangan; belum diimplementasikan secara penuh.
- [ ] Modul OCR belum diintegrasikan dengan pipeline frontend.
- [ ] Modul AI Alergen masih berupa stub respons backend dan perlu detail pemetaan istilah serta validasi.

---

## 1. Modul Computer Vision (comvis)
### Deskripsi
Modul ini berfungsi untuk memperbaiki kualitas frame kamera secara realtime sehingga OCR mendapatkan input teks yang ter-enchance.

### Fitur
- Deteksi blur / motion blur
- Deteksi glare / overexposure
- Koreksi perspektif pada permukaan melengkung
- Optimasi kontras dan thresholding lokal
- Umpan balik realtime ke pengguna

### Input
- Frame kamera asli dari kamera HP
- Metadata cahaya dan fokus

### Output
- Frame yang sudah di-enhance untuk OCR
- Status kualitas frame: `ready`, `retry`, `bad_lighting`, `glare_detected`

### Alur Proses
1. Terima stream frame dari kamera.
2. Evaluasi kualitas frame:
   - varians Laplacian untuk blur
   - histogram untuk overexposure / underexposure
   - deteksi tepi untuk tekstur permukaan melengkung
3. Terapkan koreksi:
   - unwarp perspektif jika diperlukan
   - adaptive threshold atau CLAHE
4. Berikan hasil frame ke pipeline OCR.

### Log Profesional Contoh
- `{LOG} COMVIS: frame received, timestamp=..., quality=0.82`
- `{LOG} COMVIS: blur detected, variance=12.3, action=reject_frame`
- `{LOG} COMVIS: glare detected, region_percentage=28, action=prompt_user_adjust_angle`
- `{LOG} COMVIS: perspective correction applied, method=unwarp, success=true`
- `{LOG} COMVIS: frame enhanced and passed to OCR`

### Test Case Utama
- `TC-COMVIS-01`: Frame jelas, tidak blur, OCR-ready.
- `TC-COMVIS-02`: Frame blur, tolak dan minta ulang.
- `TC-COMVIS-03`: Glare kuat, tampilkan rekomendasi sudut.
- `TC-COMVIS-04`: Permukaan melengkung dikoreksi, teks dapat dibaca.

---

## 2. Modul OCR
### Deskripsi
Modul OCR membaca teks pada komposisi bahan makanan yang sudah diolah oleh modul comvis. Fokus pada teks multilingual, typo, istilah ilmiah, dan karakter non-alfabet seperti Mandarin, Jepang, Korea.

### Fitur
- OCR multilingual (Latin, Cyrillic, Mandarin, Jepang, Korea, lain-lain)
- Deteksi dan normalisasi line-break/hyphenation
- Penggabungan hasil OCR multi-baris
- Filter noise karakter non-relevan
- Output teks terstruktur untuk AI

### Input
- Frame hasil comvis
- Metadata pipeline kualitas OCR

### Output
- Hasil teks OCR mentah
- Hasil teks OCR terstruktur
- Confidence per baris/per karakter

### Alur Proses
1. Terima frame dari comvis.
2. Jalankan mesin OCR multi-bahasa.
3. Lakukan pre-processing hasil teks:
   - hapus karakter garbage
   - normalisasi pemenggalan kata
   - gabungkan baris yang terputus
4. Masukkan teks ke format JSON untuk AI:
   - `raw_text`
   - `normalized_text`
   - `language_hints`
   - `source_frame_id`

### Log Profesional Contoh
- `{LOG} OCR: OCR started, frame_id=..., model=PaddleOCR`
- `{LOG} OCR: language detected=[Latin, Mandarin, Hangul], confidence=0.88`
- `{LOG} OCR: hyphenation corrected, original="Maltodex-", corrected="Maltodextrin"`
- `{LOG} OCR: normalized_text prepared, length=432 chars`
- `{LOG} OCR: OCR output sent to AI module`

### Test Case Utama
- `TC-OCR-01`: Komposisi bahasa campur Latin dan Mandarin terbaca.
- `TC-OCR-02`: Kata terpotong di akhir baris disatukan.
- `TC-OCR-03`: Noise karakter diabaikan tanpa merusak konteks.
- `TC-OCR-04`: Output teks disimpan dalam field `normalized_text`.

---

## 3. Modul AI Alergen
### Deskripsi
Modul AI menilai teks komposisi bahan makanan, mendeteksi berbagai jenis alergen, dan menjelaskan penyebab serta kontekstualnya. Model berjalan offline pada perangkat target.

### Fitur
- Identifikasi multi-alergen
- Klasifikasi berdasarkan daftar alergen umum dan istilah ilmiah
- Penjelasan sebab masuknya alergen
- Validasi deterministik hasil inference
- Output JSON terstruktur

### Input
- `normalized_text` dari OCR
- daftar alergen referensi lokal
- metadata confidence OCR

### Output
- Daftar alergen terdeteksi
- Komposisi atau istilah yang menyebabkan alergi
- Penjelasan sebab (cause)
- Level risiko / confidence
- Rekomendasi tindakan (opsional)

### Alur Proses
1. Terima teks terstruktur dari OCR.
2. Jalankan matching terhadap database alergen offline:
   - alergen umum (kacang, susu, gluten, telur, ikan, kerang, dll.)
   - istilah ilmiah (casein, albumin, gliadin, dextrin, dll.)
   - nama lokal dan variasi bahasa.
3. Jalankan model inferensi kecil atau rule-based overlay.
4. Validasi hasil menggunakan aturan deterministic.
5. Susun output final.

### Log Profesional Contoh
- `{LOG} AI: starting allergen detection, ocr_text_length=432`
- `{LOG} AI: found candidates=[casein, soya, peanut], method=hybrid_rule_model`
- `{LOG} AI: risk_assessment computed, score=0.92`
- `{LOG} AI: final output validated against schema, status=pass`
- `{LOG} AI: result packaged for UI response`

### Test Case Utama
- `TC-AI-01`: Teks berisi "casein" dan "soya" menghasilkan dua alergen.
- `TC-AI-02`: Istilah ilmiah "albumin" diidentifikasi sebagai alergen telur.
- `TC-AI-03`: Output JSON valid, semua field tersedia.
- `TC-AI-04`: False positive dicegah oleh validasi rule.

---

## 4. Arsitektur Prototipe Web
### Stack yang direkomendasikan
- Frontend: Streamlit
- Computer Vision: OpenCV
- OCR: PaddleOCR (PP-OCRv4)
- AI/Inferensi offline: Ollama atau local SLM

### Alur Aplikasi Web
1. User mengunggah gambar atau video stream.
2. Modul comvis memperbaiki citra.
3. Modul OCR mengekstrak teks.
4. Modul AI menilai alergen.
5. Tampilan hasil: frame final, daftar alergen, sebab, confidence.

### Input & Output pada Web
- Input: file gambar/video dari user
- Output: preview frame enhanced, teks OCR, daftar alergen, deskripsi penyebab

---

## 5. Transisi ke Android Offline
### Teknologi Mobile
- Framework: Flutter
- OCR inference: ONNX Runtime (model PP-OCR dikonversi ke ONNX)
- AI inference: llama.cpp dengan model kuantisasi `.gguf` atau PyTorch Mobile

### Komponen Android
- Kamera realtime
- Preprocessing native (OpenCV / custom Flutter plugin)
- OCR lokal ONNX
- AI lokal dengan model kecil
- UI hasil deteksi dan rekomendasi

### Alur Migrasi
1. Validasi pipeline di prototipe web.
2. Ekstrak model dan aturan komposisi alergen.
3. Konversi model OCR ke ONNX.
4. Implementasi modul comvis di Flutter.
5. Validasi offline end-to-end pada Android.

---

## 6. Rencana Testing & Validasi
### Pengujian Modular
- Unit test setiap komponen comvis, OCR, AI
- Validasi output tekstual dan JSON schema
- Simulasi frame buruk dan frame baik

### Pengujian Integrasi
- Uji alur lengkap: kamera -> comvis -> OCR -> AI -> hasil
- Uji bahasa campur dan typo
- Uji kondisi offline tanpa koneksi internet

### Log & Monitoring
- Semua modul harus menghasilkan log terstruktur
- Log mencakup `timestamp`, `module`, `event`, `status`, `metadata`
- Contoh: `{LOG} [timestamp] [OCR] process completed, engine=PaddleOCR, score=0.89`

---

## 7. Daftar Fitur Utama dan Prioritas
1. Deteksi kualitas frame realtime
2. OCR multibahasa dan penggabungan line-break
3. Deteksi multi-alergen dari teks komposisi
4. Penjelasan sebab alergen
5. Validasi offline dan schema-based output
6. UI prototipe web untuk demo sebelum Android

## 8. Catatan Khusus
- Model AI harus dirancang untuk meminimalkan false negative karena berhubungan dengan kesehatan.
- Semua alergen dan istilah ilmiah disimpan dalam database offline.
- Sistem harus dapat memproses bahasa non-Latin seperti Mandarin, Jepang, Korea, dan variasi typo.

---

## 9. Format File dan Dokumentasi
Simpan file `PLANNING.md` ini sebagai referensi perencanaan awal. Dokumentasi lebih lanjut dapat dibuat sebagai `ARCHITECTURE.md`, `TESTPLAN.md`, dan `DATA-SPEC.md` seiring perkembangan aplikasi.
