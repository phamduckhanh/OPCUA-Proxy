"""
OPC UA Proxy - Module xử lý kết nối OPC UA Client/Server.
Đọc biến từ source server, expose lại trên proxy server.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from asyncua import Client, Server, ua
from asyncua.common.subscription import DataChangeNotificationHandler

logger = logging.getLogger("opcua_proxy")


class SubscriptionHandler(DataChangeNotificationHandler):
    """Handler nhận giá trị realtime từ subscription của source server."""

    def __init__(self, proxy: "OPCUAProxy"):
        self._proxy = proxy

    def datachange_notification(self, node, val, data):
        """Callback khi giá trị biến thay đổi trên source server."""
        node_id_str = node.nodeid.to_string()
        now = datetime.now(timezone.utc).isoformat()
        self._proxy.variable_cache[node_id_str] = {
            "value": self._serialize_value(val),
            "lastUpdate": now,
        }

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

    # ── Source Client ────────────────────────────────────────

    async def connect(self, url: str | None = None):
        """Kết nối tới OPC UA source server."""
        if url:
            self.source_url = url
        if not self.source_url:
            raise ValueError("Chưa cấu hình URL source server")

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
        """Browse đệ quy address space, subscribe tất cả biến."""
        if not self.client or not self.source_connected:
            return

        self.add_log("Đang browse address space...")
        variables = []
        objects_node = self.client.nodes.objects
        await self._browse_recursive(objects_node, variables)

        self.add_log(f"Tìm thấy {len(variables)} biến")

        if not variables:
            return

        # Tạo subscription
        handler = SubscriptionHandler(self)
        self.subscription = await self.client.create_subscription(500, handler)
        self.monitored_items = await self.subscription.subscribe_data_change(variables)
        self.add_log(f"Đã subscribe {len(variables)} biến (samplingInterval=500ms)")

        # Khởi động reconnect monitor
        self._stop_reconnect = False
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

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

                    # Browse và subscribe lại
                    variables = []
                    await self._browse_recursive(self.client.nodes.objects, variables)
                    if variables:
                        handler = SubscriptionHandler(self)
                        self.subscription = await self.client.create_subscription(500, handler)
                        self.monitored_items = await self.subscription.subscribe_data_change(variables)
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

            # Tạo lại cấu trúc folder và biến giống source
            if self.source_connected and self.client:
                await self._replicate_structure(self.client.nodes.objects, self.server.nodes.objects, idx)

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

    async def _replicate_structure(self, source_node, proxy_parent, idx: int, depth: int = 0):
        """Tạo lại cấu trúc folder/biến từ source lên proxy server."""
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
                            var = await proxy_parent.add_variable(idx, proxy_name, val if val is not None else 0)
                            await var.set_writable()
                            self.proxy_nodes[node_id_str] = var
                        except Exception:
                            pass

                    elif node_class == ua.NodeClass.Object:
                        # Bỏ qua folder nằm trong danh sách loại trừ
                        if self._is_folder_excluded(name):
                            continue
                        try:
                            folder = await proxy_parent.add_folder(idx, name)
                            await self._replicate_structure(child, folder, idx, depth + 1)
                        except Exception:
                            pass
                except Exception:
                    continue
        except Exception:
            pass

    async def _update_proxy_values(self):
        """Cập nhật liên tục giá trị biến trên proxy server từ cache."""
        while self.proxy_running:
            for node_id_str, proxy_node in list(self.proxy_nodes.items()):
                cached = self.variable_cache.get(node_id_str)
                if cached and cached.get("value") is not None:
                    try:
                        val = cached["value"]
                        await proxy_node.write_value(val)
                    except Exception:
                        pass
            await asyncio.sleep(1)

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
        }

    def get_variables(self) -> list[dict]:
        """Trả về danh sách biến đang proxy."""
        result = []
        for node_id, info in self.variable_info.items():
            cached = self.variable_cache.get(node_id, {})
            proxy_name = info.get("proxyBrowseName", info.get("browseName", ""))
            result.append({
                "nodeId": node_id,
                "browseName": info.get("browseName", ""),
                "proxyBrowseName": proxy_name,
                "value": cached.get("value"),
                "dataType": info.get("dataType", "Unknown"),
                "lastUpdate": cached.get("lastUpdate"),
            })
        return result
