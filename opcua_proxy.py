"""
OPC UA Proxy - Module xử lý kết nối OPC UA Client/Server.
Đọc biến từ source server, expose lại trên proxy server.
"""

import asyncio
import logging
import math
from datetime import datetime, timezone
from typing import Any

from asyncua import Client, Server, ua
from asyncua.common.subscription import DataChangeNotificationHandler

logger = logging.getLogger("opcua_proxy")

# DIAG: hậu tố NodeId của tag cần chẩn đoán (xoá khi xong)
DIAG_SUFFIX = ".FI_101.Val"
DIAG_FILE = "diag_fi101.log"


def _diag(msg: str):
    """Ghi log chẩn đoán ra file riêng (kèm timestamp ms)."""
    try:
        with open(DIAG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {msg}\n")
    except Exception:
        pass

# ── Helpers kiểu dữ liệu OPC UA ──────────────────────────

_DTYPE_MAP: dict[str, ua.VariantType] = {
    "float":   ua.VariantType.Float,
    "double":  ua.VariantType.Double,
    "int16":   ua.VariantType.Int16,
    "short":   ua.VariantType.Int16,
    "int32":   ua.VariantType.Int32,
    "long":    ua.VariantType.Int32,
    "int64":   ua.VariantType.Int64,
    "llong":   ua.VariantType.Int64,
    "uint16":  ua.VariantType.UInt16,
    "word":    ua.VariantType.UInt16,
    "uint32":  ua.VariantType.UInt32,
    "dword":   ua.VariantType.UInt32,
    "uint64":  ua.VariantType.UInt64,
    "qword":   ua.VariantType.UInt64,
    "boolean": ua.VariantType.Boolean,
    "bool":    ua.VariantType.Boolean,
    "string":  ua.VariantType.String,
    "byte":    ua.VariantType.Byte,
    "sbyte":   ua.VariantType.SByte,
}


def _dtype_to_variant_type(dtype: str) -> ua.VariantType | None:
    """Chuyển chuỗi dataType (KEPServer/CSV) thành asyncua VariantType.
    Trả về None nếu không nhận ra kiểu → để asyncua tự suy luận."""
    return _DTYPE_MAP.get(dtype.strip().lower())


def _make_variant(val: Any, dtype_str: str) -> Any:
    """Tạo ua.Variant với kiểu dữ liệu đúng.

    - Nếu dtype_str không nhận ra → trả lại val nguyên (asyncua tự suy luận).
    - Nếu val là None → dùng giá trị mặc định phù hợp với kiểu.
    """
    vtype = _dtype_to_variant_type(dtype_str)
    if vtype is None:
        # Không biết kiểu — tránh dùng 0 (Int64) khi val=None, dùng 0.0 cho an toàn
        return val
    if val is None:
        if vtype == ua.VariantType.Boolean:
            val = False
        elif vtype == ua.VariantType.String:
            val = ""
        elif vtype in (ua.VariantType.Float, ua.VariantType.Double):
            val = 0.0
        else:
            val = 0
    return ua.Variant(val, vtype)


class SubscriptionHandler(DataChangeNotificationHandler):
    """Handler nhận giá trị realtime từ subscription của source server."""

    def __init__(self, proxy: "OPCUAProxy"):
        self._proxy = proxy

    def datachange_notification(self, node, val, data):
        """Callback khi giá trị biến thay đổi trên source server."""
        node_id_str = node.nodeid.to_string()
        now = datetime.now(timezone.utc).isoformat()
        # ── DIAG: log chi tiết notification của tag cần theo dõi ──
        if node_id_str.endswith(DIAG_SUFFIX):
            try:
                dv = data.monitored_item.Value
                src_ts = dv.SourceTimestamp
                status = dv.StatusCode.name if dv.StatusCode else None
            except Exception:
                src_ts, status = "?", "?"
            prev = self._proxy.variable_cache.get(node_id_str, {}).get("value")
            _diag(
                f"[DIAG-NOTIF] val={val!r} prev={prev!r} status={status} "
                f"srcTs={src_ts} sub={getattr(data, 'subscription_id', '?')} "
                f"handler={id(self):x} client={id(self._proxy.client):x}"
            )
        self._proxy.variable_cache[node_id_str] = {
            "value": self._serialize_value(val),
            "lastUpdate": now,
        }
        # Đánh dấu node cần cập nhật lên proxy server
        self._proxy._dirty_nodes.add(node_id_str)

    @staticmethod
    def _serialize_value(val: Any) -> Any:
        """Chuyển đổi giá trị OPC UA thành dạng JSON-serializable."""
        if val is None:
            return None
        if isinstance(val, (bool, int, float, str)):
            return val
        if isinstance(val, (bytes, bytearray)):
            return val.hex()
        if isinstance(val, datetime):
            return val.isoformat()
        if isinstance(val, (list, tuple)):
            return [SubscriptionHandler._serialize_value(v) for v in val]
        return str(val)


class OPCUAProxy:
    """
    Proxy OPC UA: kết nối tới source server, browse biến,
    subscribe realtime, và expose lại trên một server mới.
    """

    def __init__(self):
        self.source_url: str = ""
        self.proxy_port: int = 49321
        self.client: Client | None = None
        self.server: Server | None = None
        self.subscription = None
        self.monitored_items: list = []

        # Cache giá trị realtime: {node_id_string: {value, lastUpdate}}
        self.variable_cache: dict[str, dict] = {}
        # Thông tin biến đã browse: {node_id_string: {browseName, dataType}}
        self.variable_info: dict[str, dict] = {}
        # Mapping node_id source -> node proxy server
        self.proxy_nodes: dict[str, Any] = {}
        # Tập node_id cần ghi lên proxy server (chỉ khi giá trị thực sự thay đổi)
        self._dirty_nodes: set[str] = set()

        self.source_connected: bool = False
        self.proxy_running: bool = False
        self.start_time: datetime | None = None

        # Cấu hình đổi tên biến: {"SourceTag": "ProxyTag"}
        self.tag_config: dict[str, str] = {}
        # Danh sách folder cần bỏ qua khi browse
        self.exclude_folders: list[str] = []
        # Danh sách tên biến cần bỏ qua khi browse
        self.exclude_variables: list[str] = []

        # Log nội bộ (tối đa 200 dòng)
        self.logs: list[dict] = []
        self._max_logs = 200

        # Tag list cho Direct mode: [{"tagName", "address", "dataType"}]
        # Nếu có dữ liệu → dùng Direct mode, bỏ qua Browse mode
        self.tag_list: list[dict] = []

        # Task reconnect
        self._reconnect_task: asyncio.Task | None = None
        self._stop_reconnect = False

    # ── Logging ──────────────────────────────────────────────

    def add_log(self, message: str, level: str = "INFO"):
        """Thêm log vào danh sách nội bộ (hiển thị trên UI)."""
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
        }
        self.logs.append(entry)
        if len(self.logs) > self._max_logs:
            self.logs = self.logs[-self._max_logs:]
        log_fn = getattr(logger, level.lower(), logger.info)
        log_fn(message)

    def clear_logs(self):
        self.logs.clear()

    # ── Mode ──────────────────────────────────────────────────

    @property
    def _direct_mode(self) -> bool:
        """True nếu đang dùng Direct mode (có tag list từ CSV)."""
        return len(self.tag_list) > 0

    # ── Source Client ────────────────────────────────────────

    async def connect(self, url: str | None = None):
        """Kết nối tới OPC UA source server."""
        if url:
            self.source_url = url
        if not self.source_url:
            raise ValueError("Chưa cấu hình URL source server")

        self.add_log(
            f"[DIAG-CONNECT] connect() gọi; client cũ={self.client is not None}, "
            f"subscription cũ={self.subscription is not None}, "
            f"reconnect_task cũ chạy={bool(self._reconnect_task and not self._reconnect_task.done())}",
            "WARNING",
        )
        self.add_log(f"Đang kết nối tới {self.source_url}...")
        try:
            self.client = Client(url=self.source_url, timeout=10)
            await self.client.connect()
            self.source_connected = True
            self.add_log(f"Đã kết nối thành công tới {self.source_url}")
            await self.browse_and_subscribe()
        except Exception as e:
            self.source_connected = False
            self.client = None
            self.add_log(f"Lỗi kết nối: {e}", "ERROR")
            raise

    async def disconnect(self):
        """Ngắt kết nối khỏi source server."""
        self._stop_reconnect = True
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        try:
            if self.subscription:
                await self.subscription.delete()
                self.subscription = None
                self.monitored_items.clear()
        except Exception:
            pass

        try:
            if self.client:
                await self.client.disconnect()
        except Exception:
            pass

        self.client = None
        self.source_connected = False
        self.variable_cache.clear()
        self.variable_info.clear()
        self.add_log("Đã ngắt kết nối source server")

    # ── Browse & Subscribe ───────────────────────────────────

    async def browse_and_subscribe(self):
        """Dispatch tới Direct mode hoặc Browse mode tuỳ theo cấu hình."""
        if not self.client or not self.source_connected:
            return

        if self._direct_mode:
            await self._direct_subscribe()
        else:
            await self._browse_subscribe()

        # Khởi động reconnect monitor
        self._stop_reconnect = False
        if self._reconnect_task and not self._reconnect_task.done():
            self.add_log("[DIAG-CONNECT] _reconnect_loop cũ vẫn chạy, đang tạo thêm loop mới!", "WARNING")
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _browse_subscribe(self):
        """Browse mode (fallback): duyệt toàn bộ address space rồi subscribe."""
        self.add_log("Browse mode: Đang duyệt address space...")
        variables = []
        objects_node = self.client.nodes.objects
        await self._browse_recursive(objects_node, variables)

        self.add_log(f"Browse mode: Tìm thấy {len(variables)} biến")
        if not variables:
            return

        handler = SubscriptionHandler(self)
        self.subscription = await self.client.create_subscription(500, handler)
        self.monitored_items = await self.subscription.subscribe_data_change(variables)
        self.add_log(f"Browse mode: Đã subscribe {len(variables)} biến")

    # Kích thước batch cho read/subscribe (tối ưu cho OPC UA)
    _BATCH_SIZE = 200

    async def _direct_subscribe(self):
        """Direct mode: resolve và subscribe trực tiếp từ NodeId trong tag list CSV.

        Tối ưu hiệu năng:
          - get_node() chỉ tạo reference cục bộ (0 network call)
          - Đọc giá trị ban đầu bằng batch read (1 OPC UA request / batch)
          - Subscribe theo chunk (1 request / chunk)
        """
        total = len(self.tag_list)
        self.add_log(f"Direct mode: Khởi tạo {total} tag...")

        # Bước 1: tạo node reference (cục bộ, không gọi mạng)
        nodes = []
        tag_map: dict[int, dict] = {}  # index → tag info
        for i, tag in enumerate(self.tag_list):
            node = self.client.get_node(tag["address"])
            nodes.append(node)
            tag_map[i] = tag

        # Bước 2: đọc giá trị ban đầu theo batch (1 OPC UA Read request / batch)
        now = datetime.now(timezone.utc).isoformat()
        failed = 0
        for i in range(0, total, self._BATCH_SIZE):
            batch_nodes = nodes[i: i + self._BATCH_SIZE]
            batch_end = min(i + self._BATCH_SIZE, total)
            try:
                values = await asyncio.wait_for(
                    self.client.read_values(batch_nodes), timeout=30
                )
            except Exception:
                # Fallback: đánh dấu toàn batch là None
                values = [None] * len(batch_nodes)

            for j, val in enumerate(values):
                idx = i + j
                tag = tag_map[idx]
                address = tag["address"]
                serialized = SubscriptionHandler._serialize_value(val)
                self.variable_info[address] = {
                    "browseName": tag["tagName"],
                    "proxyBrowseName": tag["tagName"],
                    "dataType": tag.get("dataType", "Unknown"),
                    "tagName": tag["tagName"],
                }
                self.variable_cache[address] = {
                    "value": serialized,
                    "lastUpdate": now,
                }
            self.add_log(f"  Đọc giá trị: {batch_end}/{total} tag")

        self.add_log(f"Direct mode: {total - failed} tag OK, {failed} lỗi")
        if not nodes:
            return

        # Bước 3: subscribe theo chunk
        handler = SubscriptionHandler(self)
        self.subscription = await self.client.create_subscription(500, handler)
        self.monitored_items = await self._subscribe_chunked(nodes)
        self.add_log(f"Direct mode: Đã subscribe {total} tag (samplingInterval=500ms)")

    async def _subscribe_chunked(self, nodes: list) -> list:
        """Subscribe nodes theo batch nhỏ (1 OPC UA request / batch)."""
        all_items = []
        total = len(nodes)
        for i in range(0, total, self._BATCH_SIZE):
            chunk = nodes[i: i + self._BATCH_SIZE]
            try:
                items = await self.subscription.subscribe_data_change(chunk)
                if isinstance(items, list):
                    all_items.extend(items)
                else:
                    all_items.append(items)
                self.add_log(
                    f"  Subscribe: {min(i + self._BATCH_SIZE, total)}/{total} tag"
                )
            except Exception as e:
                self.add_log(
                    f"  Lỗi subscribe batch [{i}:{i + self._BATCH_SIZE}]: {e}",
                    "WARNING",
                )
            await asyncio.sleep(0.05)
        return all_items

    def _apply_tag_rename(self, original_name: str) -> str:
        """Áp dụng cấu hình đổi tên biến. Trả về tên mới hoặc tên gốc."""
        return self.tag_config.get(original_name, original_name)

    def _is_folder_excluded(self, folder_name: str) -> bool:
        """Kiểm tra folder có nằm trong danh sách bỏ qua không."""
        return folder_name in self.exclude_folders

    def _is_variable_excluded(self, var_name: str) -> bool:
        """Kiểm tra biến có nằm trong danh sách bỏ qua không."""
        return var_name in self.exclude_variables

    async def _browse_recursive(self, node, variables: list, depth: int = 0):
        """Browse đệ quy từ node, thu thập tất cả biến."""
        if depth > 20:  # Giới hạn độ sâu tránh vòng lặp vô hạn
            return
        try:
            children = await node.get_children()
            for child in children:
                try:
                    node_class = await child.read_node_class()
                    browse_name = await child.read_browse_name()
                    name = browse_name.Name

                    if node_class == ua.NodeClass.Variable:
                        # Bỏ qua biến nằm trong danh sách loại trừ
                        if self._is_variable_excluded(name):
                            continue

                        node_id_str = child.nodeid.to_string()
                        # Áp dụng đổi tên biến nếu có cấu hình
                        proxy_name = self._apply_tag_rename(name)

                        try:
                            data_type = await child.read_data_type()
                            dt_browse = await self.client.get_node(data_type).read_browse_name()
                            dt_name = dt_browse.Name
                        except Exception:
                            dt_name = "Unknown"

                        try:
                            val = await child.read_value()
                            serialized = SubscriptionHandler._serialize_value(val)
                        except Exception:
                            serialized = None

                        self.variable_info[node_id_str] = {
                            "browseName": name,
                            "proxyBrowseName": proxy_name,
                            "dataType": dt_name,
                        }
                        self.variable_cache[node_id_str] = {
                            "value": serialized,
                            "lastUpdate": datetime.now(timezone.utc).isoformat(),
                        }
                        variables.append(child)
                    elif node_class in (
                        ua.NodeClass.Object,
                        ua.NodeClass.ObjectType,
                    ):
                        # Bỏ qua folder nằm trong danh sách loại trừ
                        if self._is_folder_excluded(name):
                            continue
                        await self._browse_recursive(child, variables, depth + 1)
                except Exception:
                    continue
        except Exception as e:
            self.add_log(f"Lỗi browse node: {e}", "WARNING")

    # ── Reconnect ────────────────────────────────────────────

    async def _reconnect_loop(self):
        """Tự động reconnect khi mất kết nối source server."""
        while not self._stop_reconnect:
            await asyncio.sleep(5)
            if self._stop_reconnect:
                break
            try:
                if self.client:
                    # Kiểm tra kết nối bằng cách đọc server state
                    await self.client.nodes.server.read_browse_name()
            except Exception:
                self.add_log("Mất kết nối source server, đang thử kết nối lại...", "WARNING")
                self.source_connected = False
                try:
                    if self.subscription:
                        try:
                            await self.subscription.delete()
                        except Exception:
                            pass
                        self.subscription = None
                        self.monitored_items.clear()
                    if self.client:
                        try:
                            await self.client.disconnect()
                        except Exception:
                            pass
                    self.client = Client(url=self.source_url, timeout=10)
                    await self.client.connect()
                    self.source_connected = True
                    self.add_log("Đã kết nối lại thành công!")

                    # Subscribe lại đúng mode đang cấu hình
                    if self._direct_mode:
                        await self._direct_subscribe()
                    else:
                        variables = []
                        await self._browse_recursive(self.client.nodes.objects, variables)
                        if variables:
                            handler = SubscriptionHandler(self)
                            self.subscription = await self.client.create_subscription(500, handler)
                            self.monitored_items = await self._subscribe_chunked(variables)
                            self.add_log(f"Đã subscribe lại {len(variables)} biến")
                except Exception as e:
                    self.add_log(f"Kết nối lại thất bại: {e}", "ERROR")

    # ── Proxy Server ─────────────────────────────────────────

    async def start_server(self, port: int | None = None):
        """Khởi động OPC UA Proxy Server."""
        if port:
            self.proxy_port = port
        if self.proxy_running:
            self.add_log("Proxy server đang chạy rồi", "WARNING")
            return

        self.add_log(f"Đang khởi động Proxy Server trên port {self.proxy_port}...")
        try:
            self.server = Server()
            await self.server.init()
            endpoint = f"opc.tcp://0.0.0.0:{self.proxy_port}/opcuap_proxy/server/"
            self.server.set_endpoint(endpoint)
            self.server.set_server_name("OPC UA Proxy Server")

            # Cho phép anonymous access
            self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

            # Tạo namespace
            uri = "urn:opcua:proxy:server"
            idx = await self.server.register_namespace(uri)

            # Tạo cấu trúc node trên proxy server
            if self._direct_mode:
                await self._build_direct_proxy_structure(self.server.nodes.objects, idx)
            elif self.source_connected and self.client:
                await self._replicate_structure(
                    self.client.nodes.objects,
                    self.server.nodes.objects,
                    idx,
                    folder_path="",
                )

            await self.server.start()
            self.proxy_running = True
            self.start_time = datetime.now(timezone.utc)
            self.add_log(f"Proxy Server đã chạy tại {endpoint}")

            # Task cập nhật giá trị biến proxy từ cache
            asyncio.create_task(self._update_proxy_values())

        except OSError as e:
            self.proxy_running = False
            if "address already in use" in str(e).lower() or "10048" in str(e):
                self.add_log(f"Port {self.proxy_port} đã bị chiếm! Hãy chọn port khác.", "ERROR")
            else:
                self.add_log(f"Lỗi khởi động proxy server: {e}", "ERROR")
            raise
        except Exception as e:
            self.proxy_running = False
            self.add_log(f"Lỗi khởi động proxy server: {e}", "ERROR")
            raise

    async def _replicate_structure(
        self,
        source_node,
        proxy_parent,
        idx: int,
        depth: int = 0,
        folder_path: str = "",
    ):
        """Tạo lại cấu trúc folder/biến từ source lên proxy server.

        NodeId trên proxy server được tạo cố định (string NodeId) để đảm bảo
        không thay đổi sau mỗi lần deploy/restart:
          - Biến:  ns=<idx>;s=<source_node_id_str>    → duy nhất và ổn định
          - Folder: ns=<idx>;s=folder:<browse_path>   → dựa trên đường dẫn đầy đủ
        """
        if depth > 20:
            return
        try:
            children = await source_node.get_children()
            for child in children:
                try:
                    node_class = await child.read_node_class()
                    browse_name = await child.read_browse_name()
                    name = browse_name.Name

                    if node_class == ua.NodeClass.Variable:
                        # Bỏ qua biến nằm trong danh sách loại trừ
                        if self._is_variable_excluded(name):
                            continue
                        node_id_str = child.nodeid.to_string()
                        # Nếu biến chưa được browse thì bỏ qua
                        if node_id_str not in self.variable_info:
                            continue
                        # Dùng tên proxy (đã đổi tên nếu có cấu hình)
                        proxy_name = self.variable_info[node_id_str].get("proxyBrowseName", name)
                        cached = self.variable_cache.get(node_id_str, {})
                        val = cached.get("value")
                        try:
                            # NodeId cố định dựa trên NodeId của source → không đổi qua các lần restart
                            stable_node_id = ua.NodeId(node_id_str, idx)
                            dtype_str = self.variable_info[node_id_str].get("dataType", "")
                            init_val = _make_variant(val, dtype_str)
                            var = await proxy_parent.add_variable(
                                stable_node_id,
                                proxy_name,
                                init_val,
                            )
                            await var.set_writable()
                            self.proxy_nodes[node_id_str] = var
                        except Exception:
                            pass

                    elif node_class == ua.NodeClass.Object:
                        # Bỏ qua folder nằm trong danh sách loại trừ
                        if self._is_folder_excluded(name):
                            continue
                        try:
                            # NodeId cố định dựa trên đường dẫn đầy đủ → không đổi qua các lần restart
                            full_path = f"{folder_path}/{name}" if folder_path else name
                            stable_folder_id = ua.NodeId(f"folder:{full_path}", idx)
                            folder = await proxy_parent.add_folder(stable_folder_id, name)
                            await self._replicate_structure(
                                child, folder, idx, depth + 1, folder_path=full_path
                            )
                        except Exception:
                            pass
                except Exception:
                    continue
        except Exception:
            pass

    async def _build_direct_proxy_structure(self, parent_node, idx: int):
        """Tạo cấu trúc proxy từ tag list (Direct mode).

        NodeId proxy cố định: ns=<idx>;s=<tagName>
        Không bao giờ thay đổi qua các lần restart, dù có đổi địa chỉ source.
        """
        created = 0
        for tag in self.tag_list:
            tag_name = tag["tagName"]
            address = tag["address"]
            cached = self.variable_cache.get(address, {})
            val = cached.get("value")
            try:
                # NodeId cố định dùng tagName → ổn định qua mọi lần restart
                stable_node_id = ua.NodeId(tag_name, idx)
                init_val = _make_variant(val, tag.get("dataType", ""))
                var = await parent_node.add_variable(
                    stable_node_id,
                    tag_name,
                    init_val,
                )
                await var.set_writable()
                self.proxy_nodes[address] = var
                created += 1
            except Exception as e:
                self.add_log(f"  Lỗi tạo proxy node '{tag_name}': {e}", "WARNING")
        self.add_log(f"Direct mode: Đã tạo {created}/{len(self.tag_list)} node trên Proxy Server")

    async def _update_proxy_values(self):
        """Cập nhật giá trị biến trên proxy server - chỉ những node thực sự thay đổi."""
        diag_last: dict[str, Any] = {}
        while self.proxy_running:
            await asyncio.sleep(0.2)
            # ── DIAG: so sánh giá trị node proxy với cache để phát hiện client ghi đè ──
            for addr, pnode in list(self.proxy_nodes.items()):
                if not addr.endswith(DIAG_SUFFIX):
                    continue
                try:
                    pv = await pnode.read_value()
                except Exception:
                    continue
                cv = self.variable_cache.get(addr, {}).get("value")
                if pv != diag_last.get(addr) or (cv is not None and pv != cv):
                    if cv is not None and pv != cv and addr not in self._dirty_nodes:
                        self.add_log(
                            f"[DIAG-PROXY] node proxy={pv!r} KHÁC cache={cv!r} "
                            f"mà không có dirty → có thể client ghi đè",
                            "WARNING",
                        )
                    diag_last[addr] = pv
            if not self._dirty_nodes:
                continue
            # Lấy và xóa dirty set nguyên tử
            dirty = self._dirty_nodes.copy()
            self._dirty_nodes.clear()
            for node_id_str in dirty:
                proxy_node = self.proxy_nodes.get(node_id_str)
                if proxy_node is None:
                    continue
                cached = self.variable_cache.get(node_id_str)
                if cached and cached.get("value") is not None:
                    try:
                        val = cached["value"]
                        # Ghi với kiểu dữ liệu đúng để tránh lỗi BadTypeMismatch
                        dtype_str = self.variable_info.get(node_id_str, {}).get("dataType", "")
                        write_val = _make_variant(val, dtype_str)
                        await proxy_node.write_value(write_val)
                        if node_id_str.endswith(DIAG_SUFFIX):
                            _diag(f"[DIAG-WRITE] ghi lên proxy: {val!r}")
                    except Exception as e:
                        if node_id_str.endswith(DIAG_SUFFIX):
                            _diag(f"[DIAG-WRITE] LỖI ghi proxy: {e!r}")

    async def stop_server(self):
        """Dừng OPC UA Proxy Server."""
        if not self.proxy_running:
            return
        try:
            if self.server:
                await self.server.stop()
        except Exception as e:
            self.add_log(f"Lỗi khi dừng proxy server: {e}", "WARNING")
        self.server = None
        self.proxy_running = False
        self.proxy_nodes.clear()
        self._dirty_nodes.clear()
        self.start_time = None
        self.add_log("Đã dừng Proxy Server")

    # ── Thông tin ─────────────────────────────────────────────

    def get_status(self) -> dict:
        """Trả về trạng thái tổng quan."""
        uptime = None
        if self.start_time:
            delta = datetime.now(timezone.utc) - self.start_time
            uptime = int(delta.total_seconds())
        return {
            "sourceConnected": self.source_connected,
            "proxyRunning": self.proxy_running,
            "variableCount": len(self.variable_info),
            "uptime": uptime,
            "directMode": self._direct_mode,
            "tagListCount": len(self.tag_list),
        }

    def get_variables(self) -> list[dict]:
        """Trả về danh sách biến đang proxy."""
        result = []
        for node_id, info in self.variable_info.items():
            cached = self.variable_cache.get(node_id, {})
            proxy_name = info.get("proxyBrowseName", info.get("browseName", ""))
            value = cached.get("value")
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                value = None
            result.append({
                "nodeId": node_id,
                "browseName": info.get("browseName", ""),
                "proxyBrowseName": proxy_name,
                "value": value,
                "dataType": info.get("dataType", "Unknown"),
                "lastUpdate": cached.get("lastUpdate"),
            })
        return result
