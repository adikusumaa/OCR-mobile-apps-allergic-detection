# MUSTREAD: Arsitektur & Real-World Edge Cases Sistem Deteksi Alergen (Offline Mobile)

Dokumen ini memuat analisis komprehensif mengenai tantangan teknis di lapangan (worst-case scenarios) serta daftar teknologi open-source yang direkomendasikan untuk pengembangan sistem deteksi alergen berbasis Computer Vision, OCR, dan GenAI secara offline.

## 1. Tantangan Real-World (Worst-Case Scenarios)

### A. Akuisisi Citra & Preprocessing (Computer Vision)
*   **Distorsi Permukaan Melengkung:** Kemasan seperti kaleng atau botol silinder mendistorsi teks secara perspektif (perspective distortion). Pipeline preprocessing memerlukan algoritma unwarping atau konfigurasi OCR yang toleran terhadap rotasi multidemensi.
*   **Cahaya Silau (Glare) & Refleksi:** Material plastik atau metalik kemasan sering memantulkan cahaya lingkungan, menutupi sebagian blok teks komposisi. Sistem memerlukan deteksi overexposure dan harus mampu memberikan umpan balik (feedback) real-time agar pengguna menyesuaikan sudut pengambilan gambar.
*   **Kondisi Low-Light & Motion Blur:** Pengambilan gambar menggunakan tangan (handheld) pada pencahayaan minimal menghasilkan teks blur. Implementasi deteksi tingkat blur (misal: perhitungan varians Laplacian) wajib dilakukan untuk menolak frame yang tidak memenuhi standar kualitas sebelum diproses oleh OCR, sehingga meminimalkan beban komputasi yang tidak perlu.
*   **Latar Belakang Kompleks & Kontras Rendah:** Teks sering kali dicetak dengan warna yang tidak kontras terhadap latar belakang (contoh: teks merah marun di atas latar belakang coklat tua).

### B. Optical Character Recognition (OCR)
*   **Tipografi Tidak Standar & Ukuran Font Ekstrem:** Blok komposisi sering menggunakan ukuran font yang sangat kecil (4pt - 6pt) dan kerapatan spasi antar karakter (kerning) yang padat untuk menghemat ruang kemasan.
*   **Line-Break pada Kata Kunci (Hyphenation):** Kata bahan kimia atau alergen sering terpotong di akhir baris. Contoh: "Maltodex-" di baris pertama dan "trin" di baris kedua. Jika OCR dan Agent mengevaluasi hasil per baris tanpa penggabungan kontekstual, ekstraksi entitas akan gagal.
*   **Karakter Multilingual & Noise:** Produk impor sering mencantumkan komposisi dalam multi-bahasa dengan susunan karakter yang saling beririsan. Kesalahan deteksi bounding box OCR dapat menghasilkan karakter non-alfanumerik (garbage output) yang merusak konteks saat diteruskan ke LLM.

### C. GenAI / Edge LLM (Offline Inference)
*   **Limitasi Hardware & Thermal Throttling:** Eksekusi Small Language Model (SLM) secara lokal memakan memori (RAM) intensif. Pemrosesan berkelanjutan dapat memicu peningkatan suhu prosesor perangkat mobile, mengakibatkan penurunan performa otomatis (thermal throttling). Manajemen kuantisasi secara presisi adalah sebuah keharusan.
*   **Hallucination pada Keputusan Fatal:** Model LLM memiliki risiko menghasilkan false positive (mendeteksi alergen yang tidak ada) atau false negative (gagal mendeteksi alergen kritis). Karena ini menyangkut keamanan kesehatan, output harus dikunci dengan ketat menggunakan parsing deterministik dan validasi berbasis skema (JSON Schema validation).
*   **Overhead Token:** Hasil OCR mentah yang terlalu panjang atau mengandung terlalu banyak noise dapat melampaui context window dari SLM atau memperlambat waktu inferensi lokal hingga melampaui batas toleransi User Experience.

## 2. Daftar Teknologi Open-Source

Berikut adalah arsitektur teknologi yang dipetakan dari fase prototipe berbasis web hingga konversi untuk deployment mobile secara offline.

### A. Fase Prototipe (Web Interface)
*   **Frontend:** Streamlit 
*   **Computer Vision / Image Processing:** OpenCV
*   **OCR Pipeline:** PaddleOCR (PP-OCRv4)
*   **LLM Inference Engine:** Ollama
*   **SLM Models:** Llama-3.2 (1B/3B) atau Phi-3.5-mini

### B. Fase Mobile (Offline Deployment)
*   **Framework Aplikasi:** Flutter
*   **Inference Engine (OCR):** ONNX Runtime. Model PP-OCRv4 dikonversi ke format ONNX untuk menjamin efisiensi eksekusi inference pada environment Flutter.
*   **Inference Engine (AI Agent):** PyTorch Mobile (untuk integrasi model dalam format `.ptl`) atau llama.cpp (untuk model terkuantisasi `.gguf`). Konversi ke `.ptl` atau penggunaan ekosistem ONNX Runtime direkomendasikan untuk menjaga konsistensi eksekusi dari tahap preprocessing hingga klasifikasi.

