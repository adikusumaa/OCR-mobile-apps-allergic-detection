import base64
import datetime
import json

import cv2
import numpy as np

QUALITY_THRESHOLDS = {
    'blur': 100.0,
    'extreme_blur': 50.0,
    'glare_pct': 0.08,
    'over_exposure_pct': 0.18,
    'under_exposure_pct': 0.25,
    'contrast_low': 20.0,
}


def _log_event(message: str, **metadata) -> str:
    timestamp = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    if metadata:
        payload = json.dumps(metadata, ensure_ascii=False)
        return f"{timestamp} [COMVIS] {message} | {payload}"
    return f"{timestamp} [COMVIS] {message}"


def decode_base64_image(data_uri: str) -> np.ndarray:
    if not data_uri.startswith('data:image/'):
        raise ValueError('Format data image tidak didukung.')

    _, encoded = data_uri.split(',', 1)
    image_data = base64.b64decode(encoded)
    np_arr = np.frombuffer(image_data, np.uint8)
    image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    if image is None:
        raise ValueError('Gagal mendekode data gambar.')

    return image


def encode_image_to_base64(image: np.ndarray) -> str:
    success, buffer = cv2.imencode('.jpg', image, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
    if not success:
        raise ValueError('Gagal mengkodekan gambar hasil preprocessing.')
    return 'data:image/jpeg;base64,' + base64.b64encode(buffer).decode('ascii')


def calculate_blur_score(image: np.ndarray) -> float:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


def calculate_exposure_metrics(image: np.ndarray):
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    histogram = cv2.calcHist([gray], [0], None, [256], [0, 256]).flatten()
    total_pixels = gray.size

    over_exp = np.sum(histogram[245:])
    under_exp = np.sum(histogram[:10])

    return {
        'mean': float(gray.mean()),
        'overExpPct': float(over_exp / total_pixels),
        'underExpPct': float(under_exp / total_pixels),
    }


def calculate_glare_pct(image: np.ndarray):
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, bright_mask = cv2.threshold(gray, 245, 255, cv2.THRESH_BINARY)
    glare_area = np.count_nonzero(bright_mask)
    return float(glare_area / gray.size)


def calculate_contrast_score(image: np.ndarray) -> float:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return float(np.std(gray))


def order_points(pts: np.ndarray) -> np.ndarray:
    rect = np.zeros((4, 2), dtype='float32')
    s = pts.sum(axis=1)
    diff = np.diff(pts, axis=1)
    rect[0] = pts[np.argmin(s)]
    rect[2] = pts[np.argmax(s)]
    rect[1] = pts[np.argmin(diff)]
    rect[3] = pts[np.argmax(diff)]
    return rect


def correct_perspective(image: np.ndarray) -> tuple[np.ndarray, bool]:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    edges = cv2.Canny(blurred, 50, 150)
    contours, _ = cv2.findContours(edges, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)

    if not contours:
        return image, False

    contours = sorted(contours, key=cv2.contourArea, reverse=True)

    for contour in contours[:5]:
        peri = cv2.arcLength(contour, True)
        approx = cv2.approxPolyDP(contour, 0.02 * peri, True)
        if len(approx) == 4 and cv2.contourArea(approx) > 0.2 * image.shape[0] * image.shape[1]:
            pts = approx.reshape(4, 2)
            rect = order_points(pts)
            (tl, tr, br, bl) = rect
            widthA = np.linalg.norm(br - bl)
            widthB = np.linalg.norm(tr - tl)
            maxWidth = int(max(widthA, widthB))
            heightA = np.linalg.norm(tr - br)
            heightB = np.linalg.norm(tl - bl)
            maxHeight = int(max(heightA, heightB))
            if maxWidth == 0 or maxHeight == 0:
                continue
            dst = np.array([
                [0, 0],
                [maxWidth - 1, 0],
                [maxWidth - 1, maxHeight - 1],
                [0, maxHeight - 1]
            ], dtype='float32')
            M = cv2.getPerspectiveTransform(rect, dst)
            warped = cv2.warpPerspective(image, M, (maxWidth, maxHeight))
            return warped, True

    return image, False


def unsharp_mask(image: np.ndarray, amount: float = 1.5, threshold: int = 10) -> np.ndarray:
    blurred = cv2.GaussianBlur(image, (0, 0), 3)
    sharpened = cv2.addWeighted(image, 1 + amount, blurred, -amount, 0)
    if threshold > 0:
        low_contrast_mask = np.absolute(image - blurred) < threshold
        np.copyto(sharpened, image, where=low_contrast_mask)
    return sharpened


def enhance_image(image: np.ndarray) -> np.ndarray:
    lab = cv2.cvtColor(image, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=2.2, tileGridSize=(8, 8))
    l = clahe.apply(l)
    lab = cv2.merge((l, a, b))
    enhanced = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)
    return unsharp_mask(enhanced)


def preprocess_image(image: np.ndarray) -> tuple[np.ndarray, bool]:
    image = cv2.fastNlMeansDenoisingColored(image, None, 10, 10, 7, 21)
    image, perspective_applied = correct_perspective(image)
    image = enhance_image(image)
    return image, perspective_applied


def analyze_image_from_base64(data_uri: str) -> dict:
    image = decode_base64_image(data_uri)
    logs = []
    process_steps = []

    logs.append(_log_event('frame received', source='upload'))
    blur_score = calculate_blur_score(image)
    exposure = calculate_exposure_metrics(image)
    glare_pct = calculate_glare_pct(image)
    contrast_score = calculate_contrast_score(image)

    logs.append(_log_event('quality metrics calculated', blur_score=blur_score, exposure=exposure, glare_pct=glare_pct, contrast_score=contrast_score))
    process_steps.append('Menganalisis kualitas gambar: blur, pencahayaan, kontras, dan glare.')

    processed_image, perspective_applied = preprocess_image(image)
    processed_image_data = encode_image_to_base64(processed_image)

    if perspective_applied:
        process_steps.append('Koreksi perspektif diterapkan untuk permukaan terdistorsi.')
        logs.append(_log_event('perspective correction applied', method='unwarp'))
    else:
        logs.append(_log_event('perspective correction not needed'))

    process_steps.append('Denoising dan enhancement citra diterapkan untuk OCR.')
    logs.append(_log_event('image enhancement completed', method='CLAHE + sharpening'))

    status = 'ready'
    message = 'Gambar telah diproses dan siap untuk OCR.'
    suggestions = []

    if blur_score < QUALITY_THRESHOLDS['extreme_blur']:
        status = 'retry'
        message = 'Gambar sangat blur. Preprocessing tidak dapat memperbaikinya sepenuhnya.'
        suggestions.append('Tangkap ulang gambar dengan lebih stabil dan fokus pada teks.')
        process_steps.append('Frame ditolak karena blur sangat tinggi.')
        logs.append(_log_event('blur detected', blur_score=blur_score, action='retry'))
    elif blur_score < QUALITY_THRESHOLDS['blur']:
        status = 'processed'
        message = 'Gambar blur terdeteksi, tetapi telah diproses untuk OCR.'
        suggestions.append('Periksa hasil praproses dan ulangi jika teks masih sulit dibaca.')
        process_steps.append('Blur rendah terdeteksi, frame diproses dengan peringatan.')
        logs.append(_log_event('blur detected', blur_score=blur_score, action='processed'))
    else:
        process_steps.append('Blur aman; frame dapat dilanjutkan ke OCR.')
        logs.append(_log_event('blur OK', blur_score=blur_score))

    if exposure['overExpPct'] > QUALITY_THRESHOLDS['over_exposure_pct']:
        if status == 'ready':
            status = 'processed'
            message = 'Glare terdeteksi, tetapi gambar telah diproses untuk OCR.'
        suggestions.append('Periksa hasil praproses dan kurangi pantulan jika perlu.')
        process_steps.append('Overexposure atau glare terdeteksi; hasil diproses tapi direkomendasikan ulang.')
        logs.append(_log_event('overexposure detected', overExpPct=exposure['overExpPct']))
    elif exposure['underExpPct'] > QUALITY_THRESHOLDS['under_exposure_pct']:
        if status == 'ready':
            status = 'processed'
            message = 'Pencahayaan rendah, tetapi gambar telah diproses untuk OCR.'
        suggestions.append('Periksa hasil praproses dan tambahkan cahaya jika masih kurang jelas.')
        process_steps.append('Pencahayaan rendah terdeteksi; hasil diproses dengan peringatan.')
        logs.append(_log_event('underexposure detected', underExpPct=exposure['underExpPct']))
    else:
        process_steps.append('Pencahayaan baik; frame siap dilanjutkan.')
        logs.append(_log_event('exposure OK', exposure=exposure))

    if glare_pct > QUALITY_THRESHOLDS['glare_pct']:
        if status == 'ready':
            status = 'processed'
            message = 'Glare terdeteksi, namun gambar tetap diproses untuk OCR.'
        suggestions.append('Periksa hasil praproses dan ubah sudut jika pantulan masih mengganggu.')
        process_steps.append('Glare tinggi terdeteksi; saran pengguna diberikan.')
        logs.append(_log_event('glare detected', glare_pct=glare_pct))

    if contrast_score < QUALITY_THRESHOLDS['contrast_low']:
        if status == 'ready':
            status = 'processed'
            message = 'Kontras rendah terdeteksi; hasil diproses dengan peringatan.'
        suggestions.append('Tingkatkan kontras dan gunakan latar belakang yang lebih seragam.')
        process_steps.append('Kontras rendah dideteksi; hasil diproses dengan peringatan.')
        logs.append(_log_event('low contrast detected', contrast_score=contrast_score))
    else:
        logs.append(_log_event('contrast OK', contrast_score=contrast_score))

    if status == 'retry':
        process_steps.append('Akan minta pengambilan ulang karena kualitas frame tidak memenuhi standar OCR.')
    else:
        process_steps.append('Frame dipersiapkan untuk tahap OCR selanjutnya.')

    return {
        'status': status,
        'message': message,
        'metrics': {
            'blur': blur_score,
            'exposure': exposure,
            'glare': {
                'glarePct': glare_pct,
            },
            'contrast': contrast_score,
        },
        'suggestions': suggestions,
        'readyForOCR': status in ['ready', 'processed'],
        'processedImage': processed_image_data,
        'processSteps': process_steps,
        'logs': logs,
    }
