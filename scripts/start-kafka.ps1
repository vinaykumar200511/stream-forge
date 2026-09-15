# =============================================================================
# StreamForge — Start Kafka (KRaft Mode) on Windows
# =============================================================================
# Prerequisites:
#   1. Java 21 installed (run: winget install Microsoft.OpenJDK.21)
#   2. Kafka binary downloaded to Desktop\kafka.tgz
#      (run: Invoke-WebRequest -Uri 'https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz' -OutFile "$env:USERPROFILE\Desktop\kafka.tgz")
# =============================================================================

$ErrorActionPreference = "Stop"

$KAFKA_TGZ   = "$env:USERPROFILE\Desktop\kafka.tgz"
$KAFKA_DIR   = "$env:USERPROFILE\kafka"
$LOG_DIR     = "$env:USERPROFILE\kafka-logs"

Write-Host ""
Write-Host "=== StreamForge Kafka KRaft Startup ===" -ForegroundColor Cyan

# --- Check Java ---
try {
    $javaVersion = & java -version 2>&1
    Write-Host "[OK] Java found: $($javaVersion[0])" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Java not found. Install with:" -ForegroundColor Red
    Write-Host "        winget install --id Microsoft.OpenJDK.21 --silent --accept-source-agreements --accept-package-agreements"
    exit 1
}

# --- Extract Kafka if needed ---
if (-Not (Test-Path "$KAFKA_DIR\bin")) {
    if (-Not (Test-Path $KAFKA_TGZ)) {
        Write-Host "[ERROR] Kafka archive not found at: $KAFKA_TGZ" -ForegroundColor Red
        Write-Host "        Download it with:"
        Write-Host "        Invoke-WebRequest -Uri 'https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz' -OutFile `"$KAFKA_TGZ`""
        exit 1
    }
    Write-Host "[INFO] Extracting Kafka to $KAFKA_DIR ..." -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $KAFKA_DIR -Force | Out-Null
    tar -xf $KAFKA_TGZ -C $KAFKA_DIR --strip-components=1
    Write-Host "[OK] Kafka extracted." -ForegroundColor Green
} else {
    Write-Host "[OK] Kafka already extracted at $KAFKA_DIR" -ForegroundColor Green
}

# --- Clean old logs ---
if (Test-Path $LOG_DIR) {
    Write-Host "[INFO] Cleaning old Kafka logs at $LOG_DIR ..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force $LOG_DIR
}
New-Item -ItemType Directory -Path $LOG_DIR -Force | Out-Null

# --- Generate KRaft cluster UUID ---
Write-Host "[INFO] Generating KRaft cluster UUID..." -ForegroundColor Yellow
$CLUSTER_ID = & "$KAFKA_DIR\bin\windows\kafka-storage.bat" random-uuid
$CLUSTER_ID = $CLUSTER_ID.Trim()
Write-Host "[OK] Cluster ID: $CLUSTER_ID" -ForegroundColor Green

# --- Format KRaft storage ---
Write-Host "[INFO] Formatting KRaft storage..." -ForegroundColor Yellow
& "$KAFKA_DIR\bin\windows\kafka-storage.bat" format `
    -t $CLUSTER_ID `
    -c "$KAFKA_DIR\config\kraft\server.properties" `
    --ignore-formatted

# --- Patch log.dirs to our clean dir ---
$serverProps = Get-Content "$KAFKA_DIR\config\kraft\server.properties"
$serverProps = $serverProps -replace "^log\.dirs=.*", "log.dirs=$($LOG_DIR -replace '\\', '/')"
$serverProps | Set-Content "$KAFKA_DIR\config\kraft\server.properties"

Write-Host ""
Write-Host "=== Starting Kafka broker on localhost:9092 ===" -ForegroundColor Cyan
Write-Host "    Press Ctrl+C to stop." -ForegroundColor DarkGray
Write-Host ""

& "$KAFKA_DIR\bin\windows\kafka-server-start.bat" "$KAFKA_DIR\config\kraft\server.properties"
