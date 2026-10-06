param(
    [int]$ProxyPort = 18094,
    [int]$PostgresPort = 15494,
    [int]$KafkaPort = 19094,
    [int]$IdpPort = 19095,
    [string]$Subnet = '172.31.94.0/24',
    [string]$ProxyIp = '172.31.94.10',
    [string]$ChromePath = 'C:\Program Files\Google\Chrome\Application\chrome.exe'
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$project = 'diploma-task14-' + (Get-Date -Format 'yyyyMMddHHmmss')
$dbName = 'diploma_task14_smoke'
$testPassword = 'Smoke14-Test-Only-2026!'
$compose = @('-p', $project, '-f', 'docker-compose.yml', '-f', 'docker-compose.dev.yml')
$idp = $null
$started = $false

function Assert-FreePort([int]$port) {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
    try { $listener.Start() } finally { $listener.Stop() }
}

foreach ($port in @($ProxyPort, $PostgresPort, $KafkaPort, $IdpPort)) {
    Assert-FreePort $port
}
if (-not (Test-Path -LiteralPath $ChromePath)) { throw "Chrome not found: $ChromePath" }

$env:COMPOSE_SUBNET = $Subnet
$env:PROXY_IP = $ProxyIp
$env:FORWARDED_ALLOW_IPS = $ProxyIp
$env:POSTGRES_DB = $dbName
$env:PROXY_BIND = "127.0.0.1:${ProxyPort}:8080"
$env:POSTGRES_BIND = "127.0.0.1:${PostgresPort}:5432"
$env:KAFKA_HOST_PORT = "$KafkaPort"
$env:HMR_CLIENT_PORT = "$ProxyPort"
$env:OIDC_ENABLED = 'true'
$env:OIDC_ISSUER_URL = "http://host.docker.internal:$IdpPort"
$env:OIDC_CLIENT_ID = 'smoke-client'
$env:OIDC_CLIENT_SECRET = 'synthetic-smoke-secret'
$env:OIDC_REDIRECT_URI = "http://localhost:$ProxyPort/api/v1/auth/oidc/callback"
$env:OIDC_STUB_PORT = "$IdpPort"

Push-Location $root
try {
    $config = (& docker compose @compose config --format json | ConvertFrom-Json)
    if ($LASTEXITCODE -ne 0) { throw 'Compose config failed' }
    if ($config.name -ne $project -or $config.services.postgres.environment.POSTGRES_DB -ne $dbName) {
        throw 'Smoke project or database differs from preflight target'
    }
    $volumeNames = @($config.volumes.PSObject.Properties.Value | ForEach-Object { $_.name })
    $expected = @("${project}_postgres_data", "${project}_kafka_data", "${project}_frontend_node_modules")
    if (@($volumeNames | Where-Object { $_ -notin $expected }).Count -ne 0 -or $volumeNames.Count -ne 3) {
        throw 'Unexpected Compose volumes'
    }
    if ($config.networks.internal.ipam.config[0].subnet -ne $Subnet -or
        $config.services.api.environment.DATABASE_URL -notlike "*/$dbName" -or
        $config.services.migrate.environment.DATABASE_URL -notlike "*/$dbName") {
        throw 'Unexpected subnet or migration database URL'
    }
    if ((& docker compose @compose ps -a -q) -or (& docker volume ls -q --filter "name=$project")) {
        throw 'Smoke project resources already exist'
    }
    if ($LASTEXITCODE -ne 0) { throw 'Docker resource preflight failed' }

    $idp = Start-Process -FilePath 'python' -ArgumentList @('backend/tests/oidc_proxy_stub.py') -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 1
    $started = $true
    & docker compose @compose up --build -d
    if ($LASTEXITCODE -ne 0) { throw 'Isolated Compose up failed' }

    $lockPath = Join-Path $root 'frontend/package-lock.json'
    $originalLock = [System.IO.File]::ReadAllBytes($lockPath)
    $originalHash = (Get-FileHash -LiteralPath $lockPath -Algorithm SHA256).Hash.ToLowerInvariant()
    try {
        $installed = (& docker compose @compose exec -T frontend cat /app/node_modules/.lock-hash).Trim()
        if ($installed -ne $originalHash) { throw 'Clean frontend volume did not match lockfile' }
        [System.IO.File]::WriteAllBytes($lockPath, $originalLock + [byte[]](10))
        $changedHash = (Get-FileHash -LiteralPath $lockPath -Algorithm SHA256).Hash.ToLowerInvariant()
        & docker compose @compose restart frontend | Out-Null
        $matched = $false
        for ($attempt = 0; $attempt -lt 60; $attempt++) {
            Start-Sleep -Seconds 2
            $installed = (@(& docker compose @compose exec -T frontend sh -c 'cat /app/node_modules/.lock-hash 2>/dev/null || true') -join '').Trim()
            if ($installed -eq $changedHash) { $matched = $true; break }
        }
        if (-not $matched) { throw 'Existing frontend volume was not refreshed after lockfile change' }
    } finally {
        [System.IO.File]::WriteAllBytes($lockPath, $originalLock)
        & docker compose @compose restart frontend | Out-Null
    }
    $restored = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        Start-Sleep -Seconds 2
        $installed = (@(& docker compose @compose exec -T frontend sh -c 'cat /app/node_modules/.lock-hash 2>/dev/null || true') -join '').Trim()
        if ($installed -eq $originalHash) { $restored = $true; break }
    }
    if (-not $restored) { throw 'Frontend volume did not return to the original lockfile' }

    $env:SMOKE_PASSWORD = $testPassword
    & docker compose @compose exec -T -e SMOKE_PASSWORD api python -c `
        "import getpass, os, sys; getpass.getpass = lambda prompt: os.environ['SMOKE_PASSWORD']; from app.cli import create_admin; raise SystemExit(create_admin(sys.argv[1], sys.argv[2]))" `
        smoke14-long-username-visual-check-0123456789 'Алиса Очень Длинное Отображаемое Имя Для Проверки Экрана'
    if ($LASTEXITCODE -ne 0) { throw 'Synthetic account creation failed' }
    & docker compose @compose exec -T -e SMOKE_PASSWORD api python -c "import os; from sqlalchemy import create_engine, text; from app.config import settings; from app.core.security import verify_password; engine = create_engine(settings.database_url); row = engine.connect().execute(text('select password_hash from app.users where username = :username'), {'username': 'smoke14-long-username-visual-check-0123456789'}).first(); assert row and verify_password(os.environ['SMOKE_PASSWORD'], row[0]); print('Synthetic password verified')"
    Remove-Item Env:SMOKE_PASSWORD
    if ($LASTEXITCODE -ne 0) { throw 'Synthetic credential verification failed' }
    & docker compose @compose exec -T postgres psql -U diploma_user -d $dbName -c `
        "UPDATE app.roles SET name = 'Переименованная роль для проверки адаптивности' WHERE id = '00000000-0000-4000-8000-000000000001'"
    if ($LASTEXITCODE -ne 0) { throw 'Synthetic role rename failed' }

    $env:PLAYWRIGHT_BASE_URL = "http://localhost:$ProxyPort"
    $env:PLAYWRIGHT_CHROME_PATH = $ChromePath
    $env:PLAYWRIGHT_USERNAME = 'smoke14-long-username-visual-check-0123456789'
    $env:PLAYWRIGHT_PASSWORD = $testPassword
    $env:PLAYWRIGHT_OIDC = '1'
    & npm run test:browser --prefix frontend
    if ($LASTEXITCODE -ne 0) { throw 'Playwright acceptance failed' }
} finally {
    if ($started) { & docker compose @compose down -v }
    if ($idp -and -not $idp.HasExited) { Stop-Process -Id $idp.Id -Force }
    Pop-Location
}
