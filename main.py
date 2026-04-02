"""
OPC UA Proxy Server - Entry point.
Khởi động FastAPI web server, phục vụ REST API và giao diện web.
"""

import asyncio
import csv
import json
import logging
import sys
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

# Giảm log từ thư viện asyncua (quá verbose khi runtime)
logging.getLogger("asyncua").setLevel(logging.WARNING)
# Giảm log truy cập HTTP lặp lại
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# ── Đường dẫn file ────────────────────────────────────────
# Khi đóng gói thành exe bằng PyInstaller:
#   - sys.frozen = True
#   - sys._MEIPASS = thư mục tạm chứa các file bundled (static/, ...)
#   - sys.executable = đường dẫn tới file .exe
# Khi chạy bình thường bằng Python:
#   - BASE_DIR = thư mục chứa main.py

if getattr(sys, "frozen", False):
    # Chạy trong môi trường PyInstaller exe
    _BUNDLE_DIR = Path(sys._MEIPASS)          # chứa static/ đã bundle
    _RUNTIME_DIR = Path(sys.executable).parent  # thư mục cạnh file .exe
else:
    # Chạy bình thường bằng Python
    _BUNDLE_DIR = Path(__file__).resolve().parent
    _RUNTIME_DIR = Path(__file__).resolve().parent

STATIC_DIR = _BUNDLE_DIR / "static"
# config.json luôn nằm cạnh exe (để người dùng dễ chỉnh sửa)
CONFIG_PATH = _RUNTIME_DIR / "config.json"
# tags.csv: copy file CSV cạnh exe/main.py để bật Direct mode
TAG_CSV_PATH = _RUNTIME_DIR / "tags.csv"

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


def load_tag_list_from_csv(path: Path) -> list[dict]:
    """Phân tích file CSV export từ KEPServer/Ignition.

    Định dạng yêu cầu:
      Cột 'Tag Name' → tên tag expose trên proxy
      Cột 'Address'  → NodeId OPC UA phía source (ví dụ: ns=3;s=...)
      Cột 'Data Type'→ kiểu dữ liệu (Float, Boolean, Short, ...)
    """
    tags: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            raw_header = next(reader, None)
            if raw_header is None:
                return tags
            header = [h.strip().strip('"').lower() for h in raw_header]
            try:
                idx_name = header.index("tag name")
                idx_addr = header.index("address")
                idx_dtype = header.index("data type")
            except ValueError:
                logger.warning(
                    "File CSV không đúng định dạng "
                    "(thiếu cột 'Tag Name', 'Address' hoặc 'Data Type')"
                )
                return tags
            col_max = max(idx_name, idx_addr, idx_dtype)
            for row in reader:
                if len(row) <= col_max:
                    continue
                tag_name = row[idx_name].strip()
                address = row[idx_addr].strip()
                data_type = row[idx_dtype].strip()
                if not tag_name or not address:
                    continue
                tags.append({
                    "tagName": tag_name,
                    "address": address,
                    "dataType": data_type,
                })
    except Exception as e:
        logger.warning(f"Lỗi đọc file tags CSV: {e}")
    return tags


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

    # Tải danh sách tag từ CSV nếu có (kích hoạt Direct mode)
    if TAG_CSV_PATH.exists():
        tag_list = load_tag_list_from_csv(TAG_CSV_PATH)
        if tag_list:
            proxy.tag_list = tag_list
            proxy.add_log(
                f"Đã tải {len(tag_list)} tag từ '{TAG_CSV_PATH.name}' → Direct mode"
            )
        else:
            proxy.add_log(
                f"File '{TAG_CSV_PATH.name}' không có tag hợp lệ → Browse mode", "WARNING"
            )
    else:
        proxy.add_log("Không tìm thấy 'tags.csv' → Browse mode (fallback)")

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
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info",
    )
