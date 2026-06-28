---
title: CryptoWaveBackend
emoji: 🚀
colorFrom: green
colorTo: gray
sdk: docker
app_port: 8000
pinned: false
---

# CryptoWave Backend — Hugging Face Spaces

TradingView-uyumlu LRC-300 crossover motoru, Wyckoff/Elliot wave framework ve Smart Radar API'si.
Cyber Carbon frontend ile birlikte çalışmak üzere tasarlanmış **7/24 FastAPI** backend.

## Canlı API

Space ayağa kalktıktan sonra:

| Endpoint | Açıklama |
|----------|----------|
| `GET /health` | WS durumu, sembol sayısı |
| `GET /signals` | Confluence özetleri |
| `GET /groups` | large / mid / small gruplar |
| `GET /alpha-history` | Wave onaylı alpha havuzu |
| `GET /docs` | Swagger UI |

## Hugging Face Secrets (zorunlu)

Space → **Settings → Repository secrets** altına ekleyin:

| Secret | Açıklama |
|--------|----------|
| `BINANCE_API_KEY` | Binance API key (opsiyonel ama rate limit için önerilir) |
| `BINANCE_SECRET_KEY` | Binance secret |
| `CORS_ALLOW_ALL` | `1` (varsayılan — Vercel frontend için) |
| `FRONTEND_URL` | Örn. `https://your-app.vercel.app` (isteğe bağlı ek CORS) |

## Yerel Docker testi

```bash
# Proje kökünden
docker build -t cryptowave-backend .
docker run --rm -p 8000:8000 --env-file backend/.env cryptowave-backend
```

Tarayıcı: http://localhost:8000/health

## Mimari notlar

- **Port:** `8000` (HF Spaces zorunluluğu)
- **Kullanıcı:** Docker içinde `uid=1000`
- **CORS:** Varsayılan `allow_origins=["*"]` — Vercel frontend engeline takılmaz
- **Motor:** `backend/indicators.py` TV-uyumlu LRC matematiği — dokunulmaz katman

## Klasör yapısı

```
botty/
├── Dockerfile              # HF Spaces Docker
├── README.md               # Bu dosya (HF meta YAML üstte)
├── backend/
│   ├── main.py             # FastAPI orkestrasyon
│   ├── indicators.py       # LRC + crossover motoru
│   ├── wave_framework.py   # Wyckoff / Elliot / OI
│   ├── config.py
│   └── requirements.txt
└── frontend/               # Ayrı deploy (Vercel)
```

## Frontend bağlantısı

Vercel (veya başka host) frontend `.env`:

```env
VITE_BACKEND_URL=https://<kullanici>-cryptowavebackend.hf.space
```

---

## Yerel geliştirme (Docker dışı)

```powershell
cd backend
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m uvicorn main:app --host 127.0.0.1 --port 8001 --reload
```

Frontend: `cd frontend && npm run dev` → http://localhost:5173
