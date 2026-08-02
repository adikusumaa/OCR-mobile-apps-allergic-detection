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
let lastImageData = null;

async function startCamera() {
  try {
    mediaStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' }, audio: false });
    videoPreview.srcObject = mediaStream;
    cameraStatus.textContent = 'Kamera aktif, siap mendeteksi.';
    startCameraBtn.disabled = true;
    stopCameraBtn.disabled = false;
    selectedImageFile = null;
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
  const metrics = data.metrics ? `
      <p><strong>Blur Score:</strong> ${data.metrics.blur.toFixed(1)}</p>
      <p><strong>Exposure:</strong> mean=${data.metrics.exposure.mean.toFixed(1)}, over=${(data.metrics.exposure.overExpPct * 100).toFixed(1)}%, under=${(data.metrics.exposure.underExpPct * 100).toFixed(1)}%</p>
      <p><strong>Glare:</strong> ${(data.metrics.glare.glarePct * 100).toFixed(1)}%</p>
    ` : '';

  const suggestions = Array.isArray(data.suggestions) && data.suggestions.length > 0
    ? `<p><strong>Saran:</strong> ${data.suggestions.join(' ')}</p>`
    : '';

  const inputPreview = lastImageData ? `
      <div class="result-image-block">
        <p><strong>Input Asli:</strong></p>
        <img src="${lastImageData}" alt="Preview input image" />
      </div>
    ` : '';

  const processedPreview = data.processedImage ? `
      <div class="result-image-block">
        <p><strong>Hasil Preprocessing:</strong></p>
        <img src="${data.processedImage}" alt="Preview processed image" />
      </div>
    ` : '';

  const imageSection = data.processedImage ? `
      <div class="result-image-compare">
        ${inputPreview}
        ${processedPreview}
      </div>
    ` : inputPreview;

  return `
    ${imageSection}
    <div class="result-summary">
      <p><strong>Input:</strong> ${data.inputType}</p>
      <p><strong>Status:</strong> ${data.status}</p>
      ${data.message ? `<p><strong>Pesan:</strong> ${data.message}</p>` : ''}
      ${metrics}
      ${suggestions}
    </div>
  `;
}

function setLoading(isLoading) {
  if (isLoading) {
    detectBtn.disabled = true;
    startCameraBtn.disabled = true;
    imageInput.disabled = true;
    loaderOverlay.classList.remove('hidden');
    detectBtn.textContent = 'Memproses...';
  } else {
    detectBtn.disabled = false;
    startCameraBtn.disabled = false;
    imageInput.disabled = false;
    loaderOverlay.classList.add('hidden');
    detectBtn.textContent = 'Mulai Deteksi';
  }
}

function getDataURLFromVideo() {
  const canvas = document.createElement('canvas');
  canvas.width = videoPreview.videoWidth || 640;
  canvas.height = videoPreview.videoHeight || 480;
  const ctx = canvas.getContext('2d');
  ctx.drawImage(videoPreview, 0, 0, canvas.width, canvas.height);
  return canvas.toDataURL('image/jpeg', 0.85);
}

function fileToDataURL(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

async function sendToAnalyze(imageData, inputType) {
  setLoading(true);
  lastImageData = imageData;

  try {
    const response = await fetch('/api/analyze', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ imageData, inputType })
    });

    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.message || `HTTP error ${response.status}`);
    }

    updateResult(createResultCard(data));
  } catch (error) {
    updateResult(`<p class="result-empty">Terjadi kesalahan saat memproses analisis: ${error.message}</p>`);
    console.error('Analyze error:', error);
  } finally {
    setLoading(false);
  }
}

async function analyzeCurrentFrame() {
  if (!mediaStream) {
    updateResult('<p class="result-empty">Aktifkan kamera terlebih dahulu sebelum mendeteksi.</p>');
    return;
  }

  const imageData = getDataURLFromVideo();
  cameraStatus.textContent = 'Menganalisis frame kamera...';
  await sendToAnalyze(imageData, 'Kamera Realtime');
}

async function analyzeUploadedImage(file) {
  const imageData = await fileToDataURL(file);
  cameraStatus.textContent = 'Menganalisis gambar unggahan...';
  await sendToAnalyze(imageData, 'Foto Unggahan');
}

hiddenUpload.addEventListener('change', async (event) => {
  selectedImageFile = event.target.files[0] || null;
  if (!selectedImageFile) {
    return;
  }

  cameraStatus.textContent = 'Gambar terunggah, siap dianalisis.';
  await analyzeUploadedImage(selectedImageFile);
});

imageInput.addEventListener('click', (event) => {
  event.preventDefault();
  hiddenUpload.click();
});

startCameraBtn.addEventListener('click', startCamera);
stopCameraBtn.addEventListener('click', stopCamera);

detectBtn.addEventListener('click', async () => {
  if (mediaStream) {
    await analyzeCurrentFrame();
    return;
  }

  if (selectedImageFile) {
    await analyzeUploadedImage(selectedImageFile);
    return;
  }

  updateResult('<p class="result-empty">Pilih foto atau aktifkan kamera terlebih dahulu.</p>');
});
