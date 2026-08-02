const videoPreview = document.getElementById('videoPreview');
const startCameraBtn = document.getElementById('startCameraBtn');
const stopCameraBtn = document.getElementById('stopCameraBtn');
const detectBtn = document.getElementById('detectBtn');
const imageInput = document.getElementById('imageInput');
const hiddenUpload = document.getElementById('hiddenUpload');
const cameraStatus = document.getElementById('cameraStatus');
const resultCard = document.getElementById('resultCard');
const resultSection = document.getElementById('resultSection');
const loaderOverlay = document.getElementById('loaderOverlay');

let mediaStream = null;
let selectedImageFile = null;

async function startCamera() {
  try {
    mediaStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' }, audio: false });
    videoPreview.srcObject = mediaStream;
    cameraStatus.textContent = 'Kamera aktif, siap mendeteksi.';
    startCameraBtn.disabled = true;
    stopCameraBtn.disabled = false;
  } catch (error) {
    cameraStatus.textContent = 'Gagal mengaktifkan kamera. Izinkan akses kamera di browser.';
    console.error('Camera error:', error);
  }
}

function stopCamera() {
  if (mediaStream) {
    mediaStream.getTracks().forEach((track) => track.stop());
    mediaStream = null;
    videoPreview.srcObject = null;
    cameraStatus.textContent = 'Kamera dihentikan.';
    startCameraBtn.disabled = false;
    stopCameraBtn.disabled = true;
  }
}

function updateResult(content) {
  resultCard.innerHTML = content;
  resultSection.classList.remove('hidden');
  resultCard.classList.add('visible');
}

function createResultCard(data) {
  return `
    <div class="result-summary">
      <p><strong>Input:</strong> ${data.inputType}</p>
      <p><strong>Status:</strong> ${data.status}</p>
      <p><strong>Alergen terdeteksi:</strong> ${Array.isArray(data.allergens) ? data.allergens.join(', ') : data.allergens}</p>
      <p><strong>Penyebab:</strong> ${data.cause}</p>
      ${data.recommendation ? `<p><strong>Rekomendasi:</strong> ${data.recommendation}</p>` : ''}
    </div>
  `;
}

function setLoading(isLoading) {
  if (isLoading) {
    detectBtn.disabled = true;
    startCameraBtn.disabled = true;
    loaderOverlay.classList.remove('hidden');
    detectBtn.textContent = 'Memproses...';
  } else {
    detectBtn.disabled = false;
    startCameraBtn.disabled = false;
    loaderOverlay.classList.add('hidden');
    detectBtn.textContent = 'Mulai Deteksi';
  }
}

async function detectAllergen(inputType) {
  setLoading(true);

  try {
    const response = await fetch('/api/detect', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ inputType })
    });

    if (!response.ok) {
      throw new Error(`HTTP error ${response.status}`);
    }

    const data = await response.json();
    updateResult(createResultCard(data));
  } catch (error) {
    updateResult(`<p class="result-empty">Terjadi kesalahan saat memproses deteksi: ${error.message}</p>`);
    console.error('Detection error:', error);
  } finally {
    setLoading(false);
  }
}

hiddenUpload.addEventListener('change', (event) => {
  selectedImageFile = event.target.files[0] || null;
  if (!selectedImageFile) {
    return;
  }

  cameraStatus.textContent = 'Gambar terunggah, memproses...';
  detectAllergen('Foto Unggahan');
});

imageInput.addEventListener('click', (event) => {
  event.preventDefault();
  hiddenUpload.click();
});

startCameraBtn.addEventListener('click', startCamera);
stopCameraBtn.addEventListener('click', stopCamera);

detectBtn.addEventListener('click', () => {
  const inputType = mediaStream ? 'Kamera Realtime' : selectedImageFile ? 'Foto Unggahan' : null;

  if (!inputType) {
    updateResult('<p class="result-empty">Pilih foto atau aktifkan kamera terlebih dahulu.</p>');
    return;
  }

  detectAllergen(inputType);
});
