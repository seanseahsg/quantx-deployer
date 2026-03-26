# QuantX Deployer

Deploy automated trading strategies on LongPort without writing code.

## Deploy to Railway (Web Hosted)

### Step 1 — Fork this repo to your GitHub

### Step 2 — Deploy to Railway
1. Go to [railway.app](https://railway.app) > New Project
2. Deploy from GitHub > select your fork
3. Add a Volume > mount at `/data`
4. Set environment variables:
   ```
   FERNET_KEY=<generate with command below>
   CENTRAL_API_URL=https://your-central.up.railway.app
   DATA_DIR=/data
   HOSTING=railway
   ```

Generate FERNET_KEY:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Step 3 — Share the URL with students
Students open: `https://your-deployer.up.railway.app`

No installation needed.

## Run Locally (VPS Mode)

```bash
pip install -r requirements.txt
uvicorn api.main:app --host 0.0.0.0 --port 8080
```

Open http://localhost:8080

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Server port |
| `CENTRAL_API_URL` | `http://localhost:8001` | QuantX Central server URL |
| `DATA_DIR` | `.` (app root) | Where to store DB, bots, logs |
| `FERNET_KEY` | auto-generated | Encryption key for credentials |
| `HOSTING` | `vps` | `vps` or `railway` |
| `ADMIN_PIN` | `quantx2025` | Admin PIN |
