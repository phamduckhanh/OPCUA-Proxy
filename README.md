# OPC UA Proxy Server

Ứng dụng đọc tất cả biến từ một OPC UA Server nguồn, rồi expose lại trên một OPC UA Server mới ở port khác. Người dùng cấu hình và giám sát toàn bộ qua giao diện web.

## Tính năng chính

- **Proxy OPC UA**: Đọc toàn bộ biến từ source server → tạo lại trên proxy server
- **Giao diện web**: Cấu hình, giám sát biến và log realtime
- **Tự động kết nối**: Khởi động là tự connect + bật proxy, không cần thao tác tay
- **Tự động reconnect**: Khi mất kết nối source, tự động thử lại mỗi 5 giây
- **Đổi tên biến**: Cấu hình mapping tên biến từ source sang proxy
- **Bỏ qua folder/biến**: Cấu hình loại trừ folder hoặc biến không cần proxy
- **Copy biến**: Nút copy thông tin biến (NodeId, BrowseName) theo định dạng OPC

## Cấu trúc project

```
Source code/
├── main.py              # FastAPI web server + REST API + entry point
├── opcua_proxy.py       # OPC UA client/server proxy logic
├── config.json          # File cấu hình (tự tạo nếu chưa có)
├── requirements.txt     # Danh sách thư viện Python
├── Dockerfile           # Build Docker image
├── docker-compose.yml   # Docker Compose cho production
├── README.md            # File này
└── static/
    └── index.html       # Giao diện web (HTML/CSS/JS thuần)
```

## Yêu cầu

- Python 3.12+
- Các thư viện: `asyncua`, `fastapi`, `uvicorn`

## Cài đặt & Chạy

### Chạy trực tiếp

```bash
# 1. Cài thư viện
pip install -r requirements.txt

# 2. Chạy ứng dụng
python main.py
```

Mở trình duyệt tại **http://localhost:8080**

### Chạy bằng Docker

```bash
# Build và chạy
docker compose up -d

# Xem log
docker compose logs -f

# Dừng
docker compose down
```

## Cấu hình (config.json)

File `config.json` sẽ tự tạo với giá trị mặc định khi chạy lần đầu. Có thể chỉnh sửa trực tiếp file hoặc qua giao diện web.

```json
{
  "sourceUrl": "opc.tcp://192.168.1.45:4840",
  "proxyPort": 49321,
  "autoConnect": true,
  "autoStartProxy": true,
  "tagConfig": {
    "SourceTag1": "ProxyTag1",
    "SourceTag2": "ProxyTag2"
  },
  "excludeFolders": ["FolderKhongCanProxy"],
  "excludeVariables": ["BienKhongCanProxy"]
}
```

### Giải thích các tham số

| Tham số | Kiểu | Mô tả |
|---------|------|-------|
| `sourceUrl` | string | URL kết nối tới OPC UA Server nguồn |
| `proxyPort` | int | Port của OPC UA Proxy Server (1024-65535) |
| `autoConnect` | bool | Tự động kết nối source khi khởi động ứng dụng |
| `autoStartProxy` | bool | Tự động bật proxy server sau khi kết nối thành công |
| `tagConfig` | object | Mapping đổi tên biến: `{"TênGốc": "TênMới"}`. Biến nào có trong danh sách sẽ được đổi tên trên proxy server |
| `excludeFolders` | array | Danh sách tên folder cần bỏ qua khi browse. Toàn bộ biến trong folder này sẽ không được proxy |
| `excludeVariables` | array | Danh sách tên biến cần bỏ qua. Biến có tên trùng khớp sẽ không được proxy |

## REST API

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| GET | `/` | Giao diện web |
| GET | `/api/status` | Trạng thái: source connected, proxy running, số biến, uptime |
| GET | `/api/variables` | Danh sách biến đang proxy (nodeId, browseName, value, dataType...) |
| GET | `/api/logs` | Log gần nhất (tối đa 200 dòng) |
| GET | `/api/config` | Cấu hình hiện tại |
| POST | `/api/config` | Lưu cấu hình mới (JSON body) |
| POST | `/api/connect` | Kết nối tới source server |
| POST | `/api/disconnect` | Ngắt kết nối source server |
| POST | `/api/start` | Bật proxy server |
| POST | `/api/stop` | Tắt proxy server |
| POST | `/api/logs/clear` | Xóa toàn bộ log |

## Giao diện web

Giao diện gồm 4 phần chính:

1. **Thanh trạng thái**: Hiển thị trạng thái kết nối source (xanh/đỏ) và proxy (xanh/đỏ)
2. **Cấu hình**: URL source, port proxy, checkbox tự động kết nối/bật proxy, danh sách loại trừ, đổi tên biến
3. **Bảng biến**: Hiển thị realtime tất cả biến đang proxy, có tìm kiếm, highlight khi giá trị thay đổi, nút copy
4. **Nhật ký**: Log hoạt động dạng console, auto-scroll

## Xử lý lỗi

- **Source server không kết nối được**: Hiển thị lỗi trên UI và log, ứng dụng không crash
- **Port proxy bị chiếm**: Thông báo lỗi rõ ràng
- **Source server ngắt giữa chừng**: Tự động reconnect mỗi 5 giây, log sự kiện
- **Cấu hình sai JSON tagConfig**: Thông báo lỗi trên UI khi lưu

## Lưu ý

- Proxy server cho phép **anonymous access** (không cần xác thực)
- Endpoint proxy server có dạng: `opc.tcp://<IP>:<PORT>/freeopcua/server/`
- Giao diện web tự động refresh: trạng thái + biến mỗi 1 giây, log mỗi 2 giây
