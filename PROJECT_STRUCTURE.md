# PROJECT STRUCTURE

## Tujuan
Dokumen ini menjelaskan struktur folder proyek untuk prototipe web dan pipeline deteksi alergen.

## Root Folder
- `ARCHITECTURE.md` : Arsitektur sistem.
- `MUSTREAD.md` : Referensi analisis tantangan teknis dan teknologi yang direkomendasikan.
- `PLANNING.md` : Perencanaan fitur dan alur proses.
- `TESTPLAN.md` : Rencana pengujian.
- `PROJECT_STRUCTURE.md` : Dokumentasi struktur folder.

## Folder `web`
Folder untuk prototipe website.

- `web/frontend/`
  - `assets/` : Asset statis seperti gambar, logo, dan ikon.
  - `components/` : Komponen UI reusable.
  - `pages/` : Halaman utama aplikasi web.
  - `public/` : File publik yang diakses langsung oleh browser.
  - `styles/` : Berkas CSS atau styling global.
  - `utils/` : Utilitas JavaScript/TypeScript untuk helper, format teks, dan helpers UI.

- `web/backend/`
  - `api/` : Endpoint API untuk menghubungkan frontend ke pipeline.
  - `services/` : Logika bisnis backend, orchestrasi pipeline, handler request.
  - `models/` : Skema data atau tipe model yang dipakai backend.
  - `utils/` : Utilitas backend seperti logging, validasi, dan helper fungsi.

## Folder `pipeline`
Folder untuk modul pemrosesan utama.

- `pipeline/comvis/`
  - Modul Computer Vision preprocessing.
  - File yang menangani deteksi kualitas frame, glare, blur, dan koreksi perspektif.

- `pipeline/ocr/`
  - Modul OCR untuk ekstraksi teks.
  - File yang menangani integrasi PaddleOCR, post-processing, dan normalisasi teks.

- `pipeline/ai/`
  - Modul AI untuk deteksi alergen.
  - File yang menangani inferensi offline, database alergen, validasi hasil, dan pembuatan output.

## Folder dan file tambahan yang direkomendasikan
- `data/` : Dataset sample, daftar alergen, glossary istilah ilmiah.
- `docs/` : Dokumen tambahan seperti `DATA-SPEC.md`, `DEPLOYMENT.md`, dan `API.md`.
- `scripts/` : Skrip bantuan untuk setup environment, konversi model, atau data pipeline.

## Catatan
Folder ini dirancang untuk memisahkan frontend dari backend dan membagi pipeline deteksi ke modul yang jelas.
Dokumentasi tambahan seperti `DATA-SPEC.md` dan `API.md` bisa dibuat selanjutnya untuk menjelaskan skema data dan endpoint API.
