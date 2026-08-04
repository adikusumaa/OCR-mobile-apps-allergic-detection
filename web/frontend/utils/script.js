const videoPreview = document.getElementById('videoPreview');
const snapshotPreview = document.getElementById('snapshotPreview');
const cameraCard = document.querySelector('.camera-card');
const imageInput = document.getElementById('imageInput');
const hiddenUpload = document.getElementById('hiddenUpload');
const cameraStatus = document.getElementById('cameraStatus');
const heroResult = document.getElementById('heroResult');
const heroResultStep = document.getElementById('heroResultStep');
const heroResultState = document.getElementById('heroResultState');
const heroResultImage = document.getElementById('heroResultImage');
const heroResultText = document.getElementById('heroResultText');
const heroResultContent = document.getElementById('heroResultContent');
const heroCardTitle = document.querySelector('.hero-card-title');
const resultCard = document.getElementById('resultCard');
const resultSection = document.getElementById('resultSection');
const loaderOverlay = document.getElementById('loaderOverlay');
const heroCopy = document.querySelector('.hero-copy');
const heroBadge = document.querySelector('.hero-card-badge');

let mediaStream = null;
let selectedImageFile = null;
let lastImageData = null;
let autoAnalyzeInterval = null;
let isAnalyzing = false;
let livePreviewTimeout = null;

async function startCamera() {
  try {
    mediaStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' }, audio: false });
    videoPreview.srcObject = mediaStream;
    cameraStatus.textContent = 'Live stream kamera dimulai. Menunggu proses...';
    selectedImageFile = null;
    startAutoAnalyze();
  } catch (error) {
    cameraStatus.textContent = 'Gagal mengaktifkan kamera. Izinkan akses kamera di browser.';
    console.error('Camera error:', error);
  }
}

function startAutoAnalyze() {
  if (autoAnalyzeInterval) return;
  autoAnalyzeInterval = setInterval(async () => {
    if (!mediaStream || isAnalyzing) return;
    await analyzeCurrentFrame();
  }, 3000);
}

function updateResult(data) {
  const answerText = data.message || data.status || 'Hasil tersedia.';
  heroResultStep.textContent = 'Tahap: Selesai';
  heroResultState.textContent = 'COMVIS dan OCR selesai. Menampilkan output awal.';
  heroResultImage.src = data.processedImage || lastImageData || '';
  heroResultText.innerHTML = answerText;
  heroResultContent.innerHTML = renderHeroResultContent(data);

  heroResult.classList.remove('hidden');
  heroResult.classList.add('visible');
  resultSection.classList.add('hidden');
  heroBadge.textContent = 'Hasil Deteksi';
  heroCardTitle.textContent = 'Hasil Deteksi';
  heroCardTitle.classList.add('active-title');
  heroCopy.classList.add('active-result');

  // set hero to result-mode to change layout/proportions
  const heroEl = document.querySelector('.hero');
  if (heroEl) heroEl.classList.add('result-mode');
}

function showError(message) {
  heroResultStep.textContent = 'Tahap: Error';
  heroResultState.textContent = 'Terjadi kesalahan saat memproses pipeline.';
  heroResultImage.src = lastImageData || '';
  heroResultText.innerHTML = message;
  heroResultContent.innerHTML = '';

  heroResult.classList.remove('hidden');
  heroResult.classList.add('visible');
  resultSection.classList.add('hidden');
  heroBadge.textContent = 'Hasil Deteksi';
  heroCardTitle.textContent = 'Hasil Deteksi';
  heroCardTitle.classList.add('active-title');
  heroCopy.classList.add('active-result');

  // layout change to result-mode
  const heroEl = document.querySelector('.hero');
  if (heroEl) heroEl.classList.add('result-mode');
}

function showPreviewImage(imageSrc) {
  if (!snapshotPreview) {
    return;
  }

  if (imageSrc) {
    snapshotPreview.src = imageSrc;
    snapshotPreview.classList.add('visible');
    snapshotPreview.classList.remove('hidden');
    videoPreview.classList.add('hidden');
  } else {
    snapshotPreview.classList.remove('visible');
    snapshotPreview.classList.add('hidden');
    videoPreview.classList.remove('hidden');
  }
}

function resetPreviewToVideo(delay = 1200) {
  if (livePreviewTimeout) {
    clearTimeout(livePreviewTimeout);
  }

  livePreviewTimeout = setTimeout(() => {
    showPreviewImage(null);
  }, delay);
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
  imageInput.disabled = isLoading;
  hiddenUpload.disabled = isLoading;
  if (isLoading) {
    loaderOverlay.classList.remove('hidden');
  } else {
    loaderOverlay.classList.add('hidden');
  }
}

function renderHeroResultContent(data) {
  const metrics = data.metrics ? `
      <p><strong>Blur Score:</strong> ${data.metrics.blur.toFixed(1)}</p>
      <p><strong>Exposure:</strong> mean=${data.metrics.exposure.mean.toFixed(1)}, over=${(data.metrics.exposure.overExpPct * 100).toFixed(1)}%, under=${(data.metrics.exposure.underExpPct * 100).toFixed(1)}%</p>
      <p><strong>Glare:</strong> ${(data.metrics.glare.glarePct * 100).toFixed(1)}%</p>
    ` : '';

  const suggestions = Array.isArray(data.suggestions) && data.suggestions.length > 0
    ? `<p><strong>Saran:</strong> ${data.suggestions.join(' ')}</p>`
    : '';

  return `
    <div class="hero-result-info">
      <p><strong>Input:</strong> ${data.inputType}</p>
      <p><strong>Status:</strong> ${data.status}</p>
      ${data.message ? `<p><strong>Pesan:</strong> ${data.message}</p>` : ''}
      ${metrics}
      ${suggestions}
    </div>
  `;
}

function updateProcessingStage(stage, stateText, imageSrc = null) {
  heroResultStep.textContent = `Tahap: ${stage}`;
  heroResultState.textContent = stateText;
  heroResultText.innerHTML = 'Sedang dalam proses -- menampilkan ringkasan saat selesai.';
  if (imageSrc) {
    heroResultImage.src = imageSrc;
  }
  heroResultContent.innerHTML = '';
  heroResult.classList.remove('hidden');
  heroResult.classList.add('visible');
  heroBadge.textContent = 'Hasil Deteksi';
  heroCardTitle.textContent = 'Hasil Deteksi';
  heroCardTitle.classList.add('active-title');
  heroCopy.classList.add('active-result');

  // switch hero to result-mode immediately so layout becomes horizontal
  const heroEl = document.querySelector('.hero');
  if (heroEl) heroEl.classList.add('result-mode');
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
    updateProcessingStage('OCR', 'Sedang menjalankan OCR dan pemrosesan teks...');
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

    updateResult(data);
  } catch (error) {
    showError(`Terjadi kesalahan saat memproses analisis: ${error.message}`);
    console.error('Analyze error:', error);
  } finally {
    setLoading(false);
  }
}

async function analyzeCurrentFrame() {
  if (!mediaStream) {
    return;
  }

  isAnalyzing = true;
  const imageData = getDataURLFromVideo();
  cameraStatus.textContent = 'Menganalisis frame kamera...';
  updateProcessingStage('COMVIS', 'Sedang memproses frame realtime melalui COMVIS...', imageData);
  showPreviewImage(imageData);
  resetPreviewToVideo(1400);
  await sendToAnalyze(imageData, 'Kamera Realtime');
  isAnalyzing = false;
}

async function analyzeUploadedImage(file) {
  const imageData = await fileToDataURL(file);
  cameraStatus.textContent = 'Menganalisis gambar unggahan...';
  updateProcessingStage('COMVIS', 'Sedang memproses gambar unggahan melalui COMVIS...', imageData);
  showPreviewImage(imageData);
  await sendToAnalyze(imageData, 'Foto Unggahan');
  resetPreviewToVideo(8000);
}

hiddenUpload.addEventListener('change', async (event) => {
  selectedImageFile = event.target.files[0] || null;
  if (!selectedImageFile) {
    return;
  }

  cameraStatus.textContent = 'Gambar terunggah, memproses...';
  await analyzeUploadedImage(selectedImageFile);
});

imageInput.addEventListener('click', (event) => {
  event.preventDefault();
  hiddenUpload.click();
});

startCamera().catch((error) => {
  console.error('Kesalahan saat membuka kamera otomatis:', error);
});
