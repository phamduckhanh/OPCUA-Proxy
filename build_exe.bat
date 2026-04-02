@echo off
chcp 65001 > nul
echo ================================================
echo   Build OPC UA Proxy Server - Windows EXE
echo ================================================

:: Kiểm tra Python
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [LỖI] Không tìm thấy Python. Vui lòng cài Python 3.12+ và thêm vào PATH.
    pause
    exit /b 1
)

:: Cài PyInstaller nếu chưa có
python -c "import PyInstaller" >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Cài đặt PyInstaller...
    pip install pyinstaller
)

:: Cài các dependencies nếu chưa có
echo [INFO] Kiểm tra dependencies...
pip install -r requirements.txt --quiet

:: Xóa build cũ
if exist "build" (
    echo [INFO] Xóa thư mục build cũ...
    rmdir /s /q "build"
)

:: Dùng thư mục tạm ngoài OneDrive để tránh bị lock khi sync
set TEMP_DIST=C:\Temp\opcua_dist_build

:: Build exe (xuất ra thư mục tạm để tránh OneDrive lock file)
echo [INFO] Đang build exe (có thể mất vài phút)...
python -m PyInstaller opcua_proxy.spec --noconfirm --distpath "%TEMP_DIST%"

if %errorlevel% neq 0 (
    echo.
    echo [LỖI] Build thất bại! Xem log trên để biết chi tiết.
    pause
    exit /b 1
)

:: Copy kết quả vào dist\OPCUAProxy
echo [INFO] Copy kết quả vào dist\OPCUAProxy\...
if exist "dist\OPCUAProxy" rmdir /s /q "dist\OPCUAProxy"
xcopy /e /i /q "%TEMP_DIST%\OPCUAProxy" "dist\OPCUAProxy" >nul
rmdir /s /q "%TEMP_DIST%" >nul 2>&1

:: Copy config.json mẫu vào thư mục dist nếu chưa có
if not exist "dist\OPCUAProxy\config.json" (
    echo [INFO] Copy config.json mẫu vào dist\OPCUAProxy\...
    copy "config.json" "dist\OPCUAProxy\config.json" >nul
)

echo.
echo ================================================
echo   Build thành công!
echo   Kết quả: dist\OPCUAProxy\OPCUAProxy.exe
echo.
echo   Cách chạy:
echo     cd dist\OPCUAProxy
echo     OPCUAProxy.exe
echo   Sau đó mở http://localhost:8080
echo ================================================
pause
