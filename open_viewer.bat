@echo off
setlocal

REM ====== Read non-secret environment variables ======
set "CHANNEL_ARN=%KINECAST_KVS_CHANNEL_ARN%"
set "REGION=%KINECAST_KVS_REGION%"

REM ====== Detect the IPv4 address used by the active default route ======
set "VIEWER_IP="
for /f "usebackq delims=" %%A in (`
    powershell -NoProfile -Command ^
      "$client = New-Object System.Net.Sockets.UdpClient;" ^
      "try {" ^
      "  $client.Connect('8.8.8.8', 53);" ^
      "  $client.Client.LocalEndPoint.Address.IPAddressToString" ^
      "} finally {" ^
      "  $client.Dispose()" ^
      "}"
`) do set "VIEWER_IP=%%A"

if "%VIEWER_IP%"=="" (
    echo ERROR: Failed to detect the viewer IPv4 address.
    pause
    exit /b 1
)

echo Detected viewer IPv4 address: %VIEWER_IP%

REM ====== URL-encode non-secret parameters ======
for /f "usebackq delims=" %%A in (`
    powershell -NoProfile -Command ^
      "[System.Uri]::EscapeDataString($env:CHANNEL_ARN)"
`) do set "ENC_CHANNEL_ARN=%%A"

for /f "usebackq delims=" %%A in (`
    powershell -NoProfile -Command ^
      "[System.Uri]::EscapeDataString($env:REGION)"
`) do set "ENC_REGION=%%A"

for /f "usebackq delims=" %%A in (`
    powershell -NoProfile -Command ^
      "[System.Uri]::EscapeDataString($env:VIEWER_IP)"
`) do set "ENC_VIEWER_IP=%%A"

REM ====== Find a free local HTTP port ======
for /f %%P in ('powershell -NoProfile -Command "$listener = New-Object System.Net.Sockets.TcpListener([IPAddress]::Loopback, 0); $listener.Start(); $port = $listener.LocalEndpoint.Port; $listener.Stop(); Write-Output $port"') do (
    set "PORT=%%P"
)

if "%PORT%"=="" (
    echo ERROR: Failed to find a free local HTTP port.
    pause
    exit /b 1
)

echo Using local HTTP port: %PORT%

REM ====== Start the local Viewer HTTP server ======
start "Kinecast Viewer HTTP Server" cmd /k "python -m http.server %PORT% --bind 127.0.0.1"

REM ====== Open Viewer without putting AWS credentials in the URL ======
REM Enter the Access Key ID, Secret Access Key, and optional Session Token in
REM the Viewer form after the page opens. Credentials are not echoed or logged
REM by this batch file.
set "VIEWER_URL=http://localhost:%PORT%/viewer.html?channelArn=%ENC_CHANNEL_ARN%&region=%ENC_REGION%&viewerIp=%ENC_VIEWER_IP%"
start "" "%VIEWER_URL%"

endlocal
