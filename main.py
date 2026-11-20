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
# File CSV thực tế đã nạp lúc khởi động (nơi tab Browse sẽ ghi thêm tag vào)
ACTIVE_CSV_PATH: Path = TAG_CSV_PATH

# ── Config mặc định ───────────────────────────────────────

DEFAULT_CONFIG = {
    "sourceUrl": "opc.tcp://localhost:4840",
    "proxyPort": 49321,
    "autoConnect": True,
    "autoStartProxy": True,
    "tagConfig": {},
    "excludeFolders": [],
    "excludeVariables": [],
    # Timeout (giây). Sửa trong config.json rồi khởi động lại / kết nối lại.
    #   requestTimeout: mỗi request thường tới source server (connect, read, subscribe)
    #   browseTimeout : mỗi request browse ở tab Browse OPC Server
    "requestTimeout": 10,
    "browseTimeout": 60,
}


def load_tag_list_from_csv(path: Path) -> list[dict]:
    """Phân tích file CSV export từ KEPServer/Ignition.

    Hỗ trợ 2 định dạng:
      A) Có header: cột 'Tag Name', 'Address', 'Data Type'
      B) Không có header: cột 0=TagName, 1=Address, 2=DataType (KEPServer export mặc định)
    Hỗ trợ: dấu phân cách , hoặc ;, encoding utf-8/utf-8-sig/cp1252.
    """
    tags: list[dict] = []
    # Thử lần lượt các encoding phổ biến
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            with open(path, "r", encoding=encoding) as f:
                sample = f.read(4096)
            # Phát hiện delimiter: ; phổ biến ở locale EU/VN
            delimiter = ";" if sample.count(";") > sample.count(",") else ","
            break
        except UnicodeDecodeError:
            continue
    else:
        logger.warning("Không thể đọc file CSV với các encoding thử nghiệm (utf-8, cp1252)")
        return tags

    try:
        with open(path, "r", encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter)
            raw_header = next(reader, None)
            if raw_header is None:
                logger.warning("File CSV trống (không có dòng nào)")
                return tags

            header = [h.strip().strip('"').lower() for h in raw_header]
            logger.info(f"CSV delimiter='{delimiter}', encoding='{encoding}', first_row={header[:4]}")

            # Kiểm tra xem dòng đầu có phải header không
            has_header = "tag name" in header and "address" in header
            if has_header:
                idx_name = header.index("tag name")
                idx_addr = header.index("address")
                idx_dtype = header.index("data type") if "data type" in header else None
                rows = reader
            else:
                # Không có header — dùng vị trí cố định (KEPServer export mặc định)
                # col0=TagName, col1=Address, col2=DataType
                idx_name, idx_addr, idx_dtype = 0, 1, 2
                # Dòng đầu đã đọc là dữ liệu, đưa lại vào vòng lặp
                from itertools import chain
                rows = chain([raw_header], reader)

            col_max = max(idx_name, idx_addr, idx_dtype if idx_dtype is not None else 0)
            skipped = 0
            for row in rows:
                if len(row) <= col_max:
                    skipped += 1
                    continue
                tag_name = row[idx_name].strip().strip('"')
                address = row[idx_addr].strip().strip('"')
                data_type = row[idx_dtype].strip().strip('"') if idx_dtype is not None else "Unknown"
                if not tag_name or not address:
                    skipped += 1
                    continue
                tags.append({
                    "tagName": tag_name,
                    "address": address,
                    "dataType": data_type,
                })
            if skipped:
                logger.info(f"CSV: bỏ qua {skipped} dòng không hợp lệ")
    except Exception as e:
        logger.warning(f"Lỗi đọc file tags CSV: {e}")
    return tags


def append_tags_to_csv(path: Path, tags: list[dict]) -> None:
    """Ghi thêm tag vào CSV mà KHÔNG sửa các dòng đã có.

    Giữ nguyên định dạng file hiện tại (delimiter, encoding, có/không header).
    File chưa tồn tại → tạo mới theo định dạng KEPServer (không header)."""
    delimiter, encoding, needs_newline, header = ",", "utf-8", False, None
    if path.exists() and path.stat().st_size > 0:
        raw = path.read_bytes()
        for enc in ("utf-8-sig", "cp1252"):
            try:
                text = raw.decode(enc)
                encoding = "utf-8" if enc == "utf-8-sig" else enc
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError("Không đọc được encoding của file CSV hiện có")
        needs_newline = not raw.endswith(b"\n")
        first = text.splitlines()[0]
        delimiter = ";" if first.count(";") > first.count(",") else ","
        hdr = [h.strip().strip('"').lower() for h in next(csv.reader([first], delimiter=delimiter))]
        if "tag name" in hdr and "address" in hdr:
            header = hdr

    def q(s: str) -> str:
        return '"' + s.replace('"', '""') + '"'

    lines = []
    for t in tags:
        if header:
            cells = [""] * len(header)
            cells[header.index("tag name")] = q(t["tagName"])
            cells[header.index("address")] = q(t["address"])
            if "data type" in header:
                cells[header.index("data type")] = t["dataType"]
        else:
            # Định dạng KEPServer export: Name, Address, Type, 1, R/W, 100, ...
            cells = [q(t["tagName"]), q(t["address"]), t["dataType"], "1", "R/W", "100"]
            cells += [""] * 9 + ['""', ""]
        lines.append(delimiter.join(cells))

    with open(path, "ab") as f:
        data = ("\r\n" if needs_newline else "") + "\r\n".join(lines) + "\r\n"
        f.write(data.encode(encoding, errors="replace"))


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
    global ACTIVE_CSV_PATH
    cfg = load_config()
    apply_config_to_proxy(cfg)
    proxy.add_log("Ứng dụng OPC UA Proxy Server đã khởi động")

    # Tải danh sách tag từ CSV nếu có (kích hoạt Direct mode)
    # Tìm tags.csv: ưu tiên cạnh exe, fallback thư mục làm việc hiện tại
    _csv_path = TAG_CSV_PATH
    if not _csv_path.exists():
        _cwd_csv = Path.cwd() / "tags.csv"
        if _cwd_csv.exists():
            _csv_path = _cwd_csv
    ACTIVE_CSV_PATH = _csv_path
    proxy.add_log(f"Tìm tags.csv tại: {_csv_path}")
    if _csv_path.exists():
        tag_list = load_tag_list_from_csv(_csv_path)
        if tag_list:
            proxy.tag_list = tag_list
            proxy.add_log(
                f"Đã tải {len(tag_list)} tag từ '{_csv_path.name}' → Direct mode"
            )
        else:
            proxy.add_log(
                f"File '{_csv_path.name}' không có tag hợp lệ → Browse mode", "WARNING"
            )
    else:
        proxy.add_log(
            f"Không tìm thấy 'tags.csv' (đã kiểm tra: {TAG_CSV_PATH}, {Path.cwd() / 'tags.csv'}) → Browse mode (fallback)"
        )

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
    # Không bắt buộc: giao diện không gửi 2 trường này, giá trị trong file được giữ nguyên
    requestTimeout: float | None = None
    browseTimeout: float | None = None


def _valid_timeout(value, default: float) -> float:
    """Timeout hợp lệ: số trong khoảng 1–600 giây, sai thì dùng mặc định."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if 1 <= v <= 600 else default


def apply_config_to_proxy(cfg: dict):
    """Áp dụng cấu hình từ dict vào proxy instance."""
    proxy.request_timeout = _valid_timeout(cfg.get("requestTimeout"), DEFAULT_CONFIG["requestTimeout"])
    proxy.browse_timeout = _valid_timeout(cfg.get("browseTimeout"), DEFAULT_CONFIG["browseTimeout"])
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
    # Gộp lên cấu hình hiện có: các khóa giao diện không gửi (VD: requestTimeout,
    # browseTimeout) được giữ nguyên thay vì bị xóa/đặt lại về mặc định
    data = {**load_config(), **cfg.model_dump(exclude_unset=True)}
    for k in ("requestTimeout", "browseTimeout"):
        if data.get(k) is None:
            data[k] = DEFAULT_CONFIG[k]
        elif not 1 <= data[k] <= 600:
            raise HTTPException(status_code=400, detail=f"{k} phải trong khoảng 1-600 giây")
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


# ── Browse source server & thêm tag vào cấu hình ──────────

class ReadNodesModel(BaseModel):
    nodeIds: list[str]


class TagItem(BaseModel):
    tagName: str
    address: str
    dataType: str = "Unknown"


class AddTagsModel(BaseModel):
    tags: list[TagItem]


@app.get("/api/browse/children")
async def api_browse_children(nodeId: str = ""):
    """Trang đầu của con trực tiếp của 1 node (rỗng = thư mục Objects gốc).
    Gọi lười khi người dùng mở folder. Nếu `more` = true → gọi /browse/more."""
    try:
        return JSONResponse(await proxy.browse_children(nodeId or None))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e) or type(e).__name__)


@app.get("/api/browse/more")
async def api_browse_more(token: str):
    """Trang tiếp theo của một folder lớn đang tải dở."""
    try:
        return JSONResponse(await proxy.browse_more(token))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e) or type(e).__name__)


@app.post("/api/browse/cache/clear")
async def api_browse_cache_clear():
    """Xóa cache duyệt cây (nút "Tải lại")."""
    proxy.clear_browse_cache()
    return JSONResponse({"message": "Đã xóa cache"})


@app.post("/api/browse/read")
async def api_browse_read(body: ReadNodesModel):
    """Đọc giá trị hiện tại + kiểu dữ liệu của các node đã chọn (tối đa 500)."""
    if len(body.nodeIds) > 500:
        raise HTTPException(status_code=400, detail="Tối đa 500 node mỗi lần đọc")
    try:
        return JSONResponse(await proxy.read_nodes_preview(body.nodeIds))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e) or repr(e))


@app.post("/api/taglist/add")
async def api_taglist_add(body: AddTagsModel):
    """Thêm tag đã chọn vào tags.csv (append) và vào danh sách tag đang nạp."""
    names = {t["tagName"] for t in proxy.tag_list}
    addrs = {t["address"] for t in proxy.tag_list}
    to_add: list[dict] = []
    skipped: list[dict] = []
    for t in body.tags:
        name, addr = t.tagName.strip(), t.address.strip()
        dtype = (t.dataType or "Unknown").strip() or "Unknown"
        if not name or not addr:
            skipped.append({"tagName": name, "reason": "Tên hoặc địa chỉ trống"})
        elif name in names:
            skipped.append({"tagName": name, "reason": "Tên proxy đã tồn tại"})
        elif addr in addrs:
            skipped.append({"tagName": name, "reason": "Địa chỉ đã có trong cấu hình"})
        else:
            names.add(name)
            addrs.add(addr)
            to_add.append({"tagName": name, "address": addr, "dataType": dtype})
    if to_add:
        try:
            append_tags_to_csv(ACTIVE_CSV_PATH, to_add)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Không ghi được {ACTIVE_CSV_PATH}: {e}")
        proxy.tag_list.extend(to_add)
        proxy.add_log(f"Browse: đã thêm {len(to_add)} tag vào '{ACTIVE_CSV_PATH.name}'")
    return JSONResponse({
        "added": len(to_add),
        "skipped": skipped,
        "total": len(proxy.tag_list),
        "csvPath": str(ACTIVE_CSV_PATH),
        "restartRequired": bool(to_add) and proxy.source_connected,
    })


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
