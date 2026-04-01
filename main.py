"""
OPC UA Proxy Server - Entry point.
Khởi động FastAPI web server, phục vụ REST API và giao diện web.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from opcua_proxy import OPCUAProxy

# ── Cấu hình logging ──────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")

# ── Đường dẫn file ────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
STATIC_DIR = BASE_DIR / "static"

# ── Config mặc định ───────────────────────────────────────

DEFAULT_CONFIG = {
    "sourceUrl": "opc.tcp://localhost:4840",
    "proxyPort": 49321,
    "autoConnect": True,
    "autoStartProxy": True,
    "tagConfig": {},
    "excludeFolders": [],
    "excludeVariables": [],
}


def load_config() -> dict:
    """Đọc config từ file, tạo mới nếu chưa tồn tại."""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    save_config(DEFAULT_CONFIG)
    return DEFAULT_CONFIG.copy()


def save_config(cfg: dict):
    """Lưu config ra file."""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


# ── OPC UA Proxy instance ─────────────────────────────────

proxy = OPCUAProxy()


# ── FastAPI lifespan ───────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown logic."""
    cfg = load_config()
    apply_config_to_proxy(cfg)
    proxy.add_log("Ứng dụng OPC UA Proxy Server đã khởi động")

    # Tự động kết nối và bật proxy nếu cấu hình cho phép
    if cfg.get("autoConnect", True):
        asyncio.create_task(_auto_startup(cfg))

    yield
    # Shutdown: dừng proxy và ngắt kết nối
    try:
        await proxy.stop_server()
    except Exception:
        pass
    try:
        await proxy.disconnect()
    except Exception:
        pass
    proxy.add_log("Ứng dụng đã dừng")


async def _auto_startup(cfg: dict):
    """Tự động kết nối source và bật proxy server khi khởi động."""
    await asyncio.sleep(1)  # Chờ FastAPI khởi động xong
    try:
        proxy.add_log("Tự động kết nối tới source server...")
        await proxy.connect()
    except Exception as e:
        proxy.add_log(f"Tự động kết nối thất bại: {e}", "ERROR")
        return
    if cfg.get("autoStartProxy", True):
        try:
            proxy.add_log("Tự động bật Proxy Server...")
            await proxy.start_server()
        except Exception as e:
            proxy.add_log(f"Tự động bật proxy thất bại: {e}", "ERROR")


app = FastAPI(title="OPC UA Proxy Server", lifespan=lifespan)

# Phục vụ file tĩnh
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── Pydantic models ───────────────────────────────────────

class ConfigModel(BaseModel):
    sourceUrl: str
    proxyPort: int
    autoConnect: bool = True
    autoStartProxy: bool = True
    tagConfig: dict[str, str] = {}
    excludeFolders: list[str] = []
    excludeVariables: list[str] = []


def apply_config_to_proxy(cfg: dict):
    """Áp dụng cấu hình từ dict vào proxy instance."""
    proxy.source_url = cfg.get("sourceUrl", DEFAULT_CONFIG["sourceUrl"])
    proxy.proxy_port = cfg.get("proxyPort", DEFAULT_CONFIG["proxyPort"])
    proxy.tag_config = cfg.get("tagConfig", {})
    proxy.exclude_folders = cfg.get("excludeFolders", [])
    proxy.exclude_variables = cfg.get("excludeVariables", [])


# ── REST API ───────────────────────────────────────────────

@app.get("/")
async def index():
    """Trả về trang giao diện chính."""
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/api/status")
async def get_status():
    """Trạng thái hiện tại của proxy."""
    return JSONResponse(proxy.get_status())


@app.get("/api/variables")
async def get_variables():
    """Danh sách tất cả biến đang proxy."""
    return JSONResponse(proxy.get_variables())


@app.get("/api/logs")
async def get_logs():
    """Log gần nhất (max 200 dòng)."""
    return JSONResponse(proxy.logs)


@app.get("/api/config")
async def get_config():
    """Trả về cấu hình hiện tại."""
    return JSONResponse(load_config())


@app.post("/api/config")
async def post_config(cfg: ConfigModel):
    """Lưu cấu hình mới và áp dụng."""
    data = cfg.model_dump()
    # Validate port range
    if not (1024 <= data["proxyPort"] <= 65535):
        raise HTTPException(status_code=400, detail="Port phải trong khoảng 1024-65535")
    save_config(data)
    apply_config_to_proxy(data)
    proxy.add_log(f"Cấu hình đã cập nhật: source={data['sourceUrl']}, port={data['proxyPort']}")
    return JSONResponse({"message": "Đã lưu cấu hình", "config": data})


@app.post("/api/connect")
async def api_connect():
    """Kết nối tới source server."""
    try:
        await proxy.connect()
        return JSONResponse({"message": "Đã kết nối thành công"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/disconnect")
async def api_disconnect():
    """Ngắt kết nối source server."""
    try:
        await proxy.disconnect()
        return JSONResponse({"message": "Đã ngắt kết nối"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/start")
async def api_start():
    """Bật Proxy Server."""
    try:
        await proxy.start_server()
        return JSONResponse({"message": f"Proxy Server đã chạy trên port {proxy.proxy_port}"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/stop")
async def api_stop():
    """Tắt Proxy Server."""
    try:
        await proxy.stop_server()
        return JSONResponse({"message": "Đã dừng Proxy Server"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/logs/clear")
async def api_clear_logs():
    """Xóa toàn bộ log."""
    proxy.clear_logs()
    return JSONResponse({"message": "Đã xóa log"})


# ── Main ───────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  OPC UA Proxy Server đang chạy tại http://localhost:8080")
    print("=" * 55)
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        log_level="info",
    )
