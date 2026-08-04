import React, { useRef, useState } from 'react';
import axios from 'axios';
import { Upload, Sparkles, Camera } from 'lucide-react';
import './styles.css';

export default function App() {
  const [image, setImage] = useState(null);
  const [result, setResult] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const fileInputRef = useRef(null);

  const handleImageUpload = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onloadend = async () => {
      const base64String = reader.result;
      setImage(base64String);
      setIsLoading(true);
      setResult(null);

      try {
        const response = await axios.post('/api/analyze', {
          imageData: base64String,
          inputType: 'upload'
        });
        setResult(response.data);
      } catch (error) {
        console.error('Image analysis failed.', error);
        setResult({
          status: 'error',
          message: 'Gagal menganalisis gambar. Coba lagi.',
          inputType: 'upload'
        });
      } finally {
        setIsLoading(false);
      }
    };

    reader.readAsDataURL(file);
  };

  const resetState = () => {
    setImage(null);
    setResult(null);
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  return (
    <div className="app-container">
      <nav className="nav-bar">
        <div className="nav-content">
          <div className="nav-logo">
            <span className="logo-text">Alergio</span>
          </div>
          <div className="nav-links">
            <a href="#beranda" className="nav-link">Beranda</a>
            <a href="#fitur" className="nav-link">Fitur</a>
            <button type="button" className="nav-cta" onClick={() => fileInputRef.current?.click()}>
              Unggah Foto
            </button>
          </div>
        </div>
      </nav>

      <section className="hero">
        <h1 className="hero-title">Deteksi alergi lebih cerdas, cepat, dan elegan.</h1>
        <p className="hero-subtitle">
          Unggah foto dan biarkan Alergio memindai potensi alergi secara real-time dengan tampilan modern ala Apple.
        </p>
      </section>

      <section className="input-section" id="fitur">
        <div className="input-card">
          <div className="input-header">
            <h2 className="section-title">Unggah Foto Alergi Anda</h2>
            <p className="section-subtitle">Alergio akan menganalisis alergi dengan cepat dan menampilkan hasil yang mudah dipahami.</p>
          </div>

          <div className="input-body">
            <div className="preview-card">
              <div className="preview-card-header">
                <span className="preview-card-title">Live Preview</span>
                <span className="preview-card-badge">Upload</span>
              </div>
              <div className="camera-card">
                {isLoading && (
                  <div className="loader-overlay">
                    <div className="loader-spinner" />
                    <span>Menganalisis...</span>
                  </div>
                )}

                {image ? (
                  <img src={image} alt="Preview upload" />
                ) : (
                  <div className="camera-placeholder">
                    <Camera size={36} />
                    <p>Unggah foto untuk memulai analisis alergi.</p>
                  </div>
                )}

                <input
                  ref={fileInputRef}
                  type="file"
                  accept="image/*"
                  className="hidden"
                  onChange={handleImageUpload}
                />

                <button
                  type="button"
                  className="upload-overlay-btn"
                  onClick={() => fileInputRef.current?.click()}
                >
                  <Upload size={20} />
                </button>
              </div>
            </div>

            <div className="hero-result">
              <div className="hero-result-header">
                <span className="hero-result-step">Kontrol</span>
                <span className="hero-result-state">{result?.status ? result.status : 'Siap memproses'}</span>
              </div>
              <div className="hero-result-body">
                <p>
                  Klik tombol upload untuk memulai. Setelah gambar diproses, hasil deteksi alergi akan muncul di bawah.
                </p>
                <button type="button" className="button-action secondary" onClick={resetState}>
                  <Sparkles size={16} />
                  Reset
                </button>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="results-section">
        {result ? (
          <div className="result-card">
            <div className="result-card-image">
              <img src={result.resultImageUrl || image} alt="Hasil analisis" />
              <div className="result-card-badge">Hasil</div>
            </div>
            <div className="result-card-body">
              <div className="result-card-header">
                <span className="result-card-style">Ringkasan</span>
                <span className="result-card-score">{result.status === 'success' ? 'Sukses' : 'Error'}</span>
              </div>
              <div className="result-card-content">
                <p><strong>Input:</strong> {result.inputType}</p>
                <p><strong>Pesan:</strong> {result.reasoning || result.message}</p>
                {result.predictions && (
                  <p><strong>Prediksi:</strong> {result.predictions.join(', ')}</p>
                )}
              </div>
            </div>
          </div>
        ) : (
          <div className="placeholder-panel">
            <h3>Hasil akan muncul di sini setelah gambar dianalisis.</h3>
            <p>Unggah foto alergi untuk menerima penilaian dan rekomendasi.</p>
          </div>
        )}
      </section>
    </div>
  );
}
