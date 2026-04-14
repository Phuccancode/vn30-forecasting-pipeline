# Deploy VN30 Pipeline on Azure VM

Tài liệu này hướng dẫn triển khai dự án lên Azure Virtual Machine theo kiến trúc đã chốt:
- Ubuntu 22.04 LTS
- Docker + Docker Compose
- Streamlit public qua Nginx + HTTPS
- Airflow private (không public 8080)
- Power BI đọc dữ liệu trực tiếp từ Azure SQL

## 1) Chuẩn bị Azure VM

Khuyến nghị:
- VM size: `Standard_B2s` (demo)
- OS: `Ubuntu 22.04 LTS`
- Disk: >= 30GB

NSG inbound khuyến nghị:
- `22/tcp`: chỉ cho IP của bạn
- `80/tcp`: mở internet
- `443/tcp`: mở internet

Không mở public:
- `8080` (Airflow)
- `8501` (Streamlit internal)

## 2) SSH vào VM và cài Docker

```bash
ssh azureuser@<VM_PUBLIC_IP>
```

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg lsb-release

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

sudo usermod -aG docker $USER
newgrp docker
docker --version
docker compose version
```

## 3) Pull code và cấu hình môi trường

```bash
git clone <YOUR_REPO_URL> vn30-stock-project
cd vn30-stock-project
cp .env.example .env
```

Chỉnh `.env`:
- `AZURE_STORAGE_ACCOUNT_NAME`
- `AZURE_STORAGE_ACCOUNT_KEY`
- `AZURE_SQL_CONNECTION_STRING`
- `FERNET_KEY` (bắt buộc đặt giá trị cố định, không để trống)
- `_AIRFLOW_WWW_USER_USERNAME`
- `_AIRFLOW_WWW_USER_PASSWORD`

Tạo `FERNET_KEY`:
```bash
python3 - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
```

Set permission cho file secrets:
```bash
chmod 600 .env
```

## 4) Start stack bằng Docker Compose

```bash
docker compose up -d
docker compose ps
```

Kiểm tra logs nhanh:
```bash
docker compose logs --tail=100 airflow-scheduler
docker compose logs --tail=100 airflow-apiserver
```

## 5) Cấu hình Nginx reverse proxy cho Streamlit

Cài Nginx:
```bash
sudo apt install -y nginx
sudo systemctl enable nginx
```

Tạo file `/etc/nginx/sites-available/vn30`:
```nginx
server {
    listen 80;
    server_name <YOUR_DOMAIN>;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }
}
```

Enable site:
```bash
sudo ln -sf /etc/nginx/sites-available/vn30 /etc/nginx/sites-enabled/vn30
sudo nginx -t
sudo systemctl reload nginx
```

## 6) Bật HTTPS (Let's Encrypt)

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d <YOUR_DOMAIN>
```

Kiểm tra auto-renew:
```bash
sudo systemctl status certbot.timer
```

## 7) Giữ Airflow private

- Không mở `8080` ở NSG.
- Nếu cần truy cập Airflow UI, dùng SSH tunnel:

```bash
ssh -L 8080:localhost:8080 azureuser@<VM_PUBLIC_IP>
```

Sau đó mở local browser:
- `http://localhost:8080`

## 8) Auto-start sau reboot (systemd)

Tạo file `/etc/systemd/system/vn30-compose.service`:
```ini
[Unit]
Description=VN30 Docker Compose Stack
Requires=docker.service
After=docker.service network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/azureuser/vn30-stock-project
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
```

Enable service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable vn30-compose.service
sudo systemctl start vn30-compose.service
sudo systemctl status vn30-compose.service
```

## 9) Power BI Integration

Nguồn dữ liệu:
- Azure SQL Database (không qua Streamlit)

Bảng/view khuyến nghị:
- `fact_prices`
- `dim_ticker`
- `dim_date`
- `fact_price_predictions`
- `ml_metrics_history`

Khuyến nghị bảo mật:
- Tạo SQL login read-only riêng cho Power BI
- Chỉ cấp quyền `SELECT` cho schema/view BI

Refresh:
- Scheduled refresh sau khi DAG daily chạy xong
- Nếu muốn ổn định, đặt refresh chậm hơn giờ DAG 15-30 phút

## 10) Checklist vận hành sau deploy

- [ ] `docker compose ps` tất cả service healthy/running
- [ ] Airflow DAG `vn30_daily_pipeline` trigger thành công
- [ ] `gold_load` và `ml_*` tasks chạy thành công
- [ ] Streamlit truy cập được qua `https://<YOUR_DOMAIN>`
- [ ] Airflow không public internet
- [ ] Power BI refresh thành công từ Azure SQL

## 11) Lệnh xử lý nhanh

Restart stack:
```bash
cd /home/azureuser/vn30-stock-project
docker compose down
docker compose up -d
```

Xem log task:
```bash
docker compose logs -f airflow-scheduler
```

Check mounts trong container Airflow:
```bash
docker exec -it vn30-stock-project-airflow-scheduler-1 ls -la /opt/airflow/sql
```

