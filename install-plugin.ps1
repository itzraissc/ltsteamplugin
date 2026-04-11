param(
    [string]$DownloadLink,
    [string]$PluginName
)

$Host.UI.RawUI.WindowTitle = "Luatools plugin installer | Cozy Edition 🌿"
$name = "ltsteamplugin"
$link = "https://github.com/itzraissc/ltsteamplugin/releases/latest/download/ltsteamplugin.zip"

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
chcp 65001 > $null
Add-Type -AssemblyName System.IO.Compression.FileSystem

$steam = (Get-ItemProperty "HKLM:\SOFTWARE\WOW6432Node\Valve\Steam" -ErrorAction SilentlyContinue).InstallPath
if (-not $steam) { $steam = "C:\Program Files (x86)\Steam" }

$upperName = $name.Substring(0, 1).ToUpper() + $name.Substring(1).ToLower()
if ($DownloadLink) { $link = $DownloadLink }
if ($PluginName)   { $name = $PluginName }

function Log {
    param ([string]$Type, [string]$Message, [boolean]$NoNewline = $false)
    $Type = $Type.ToUpper()
    switch ($Type) {
        "OK"   { $foreground = "Green" }
        "INFO" { $foreground = "Cyan" }
        "ERR"  { $foreground = "Red" }
        "WARN" { $foreground = "Yellow" }
        "LOG"  { $foreground = "DarkCyan" }
        "AUX"  { $foreground = "DarkGray" }
        default { $foreground = "White" }
    }
    $date = Get-Date -Format "HH:mm:ss"
    $prefix = if ($NoNewline) { "`r[$date] " } else { "[$date] " }
    Write-Host $prefix -ForegroundColor "Cyan" -NoNewline
    Write-Host "[$Type] $Message" -ForegroundColor $foreground -NoNewline:$NoNewline
}

Write-Host
Log "INFO" "Instalador LuaTools - v4.0 🌿 (Direct Oficial Bypass)"
Log "AUX" "Engine: SteamTools + Millennium Oficial GitHub"
Write-Host

$ProgressPreference = 'SilentlyContinue'

# --- PHASE 0: Parar a Steam ---
Log "LOG" "Finalizando a Steam para iniciar a injecao segura..."
Get-Process steam -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 2

# --- PHASE 1: Steamtools ---
function CheckSteamtools {
    $files = @( "dwmapi.dll", "xinput1_4.dll" )
    foreach($file in $files) {
        if (!(Test-Path (Join-Path $steam $file))) { return $false }
    }
    return $true
}

if (CheckSteamtools) {
    Log "INFO" "Steamtools local intacto - Mantendo versao atual"
} else {
    $script = Invoke-RestMethod "https://luatools.vercel.app/st.ps1"
    $keptLines = @()
    foreach ($line in $script -split "`n") {
        $conditions = @(
            ($line -imatch "Start-Process" -and $line -imatch "steam"),
            ($line -imatch "steam\.exe"),
            ($line -imatch "Start-Sleep" -or $line -imatch "Write-Host"),
            ($line -imatch "cls" -or $line -imatch "exit"),
            ($line -imatch "Stop-Process" -and -not ($line -imatch "Get-Process"))
        )
        if (-not($conditions -contains $true)) { $keptLines += $line }
    }
    $SteamtoolsScript = $keptLines -join "`n"
    Log "WARN" "Steamtools ausente."
    Log "LOG" "O Download e Instalacao do Steamtools acontecera abaixo..."
    Invoke-Expression $SteamtoolsScript *> $null

    if (CheckSteamtools) {
        Log "OK" "Steamtools injetado vira proxy (dwmapi/xinput)"
    } else {
        Log "ERR" "Falha na injecao SteamTools. Verifique antivirus."
    }
}

# --- PHASE 2: Millennium (Direct Github Release) ---
$milHook = Join-Path $steam "wsock32.dll"
$millenniumInstalling = $false

if (!(Test-Path $milHook)) {
    Log "ERR" "Millennium (v3 Architecture) ausente."
    Log "WARN" "Iniciando instalacao limpa a partir do GitHub Oficial SteamClientHomebrew..."
    Log "AUX" "Isso resolve a falha do loop infinito que ocorria em versoes antigas."
    
    $milZipPath = Join-Path $env:TEMP "millennium_official.zip"
    $installed = $false

    try {
        Log "LOG" "Contatando API do GitHub para buscar a versao x86_64..."
        $milRel = Invoke-RestMethod "https://api.github.com/repos/SteamClientHomebrew/Millennium/releases/latest"
        $asset = $milRel.assets | Where-Object { $_.name -match "windows-x86_64\.zip$" } | Select-Object -First 1
        
        if ($asset -and $asset.browser_download_url) {
            Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $milZipPath *> $null
            $installed = $true
        }
    } catch {
        Log "WARN" "Livre limite da API do Github atingido. Rotacionando para URL Estatica Diretiva (v3.0.0-beta.19)..."
        $fallbackUrl = "https://github.com/SteamClientHomebrew/Millennium/releases/download/v3.0.0-beta.19/millennium-v3.0.0-beta.19-windows-x86_64.zip"
        try {
            Invoke-WebRequest -Uri $fallbackUrl -OutFile $milZipPath *> $null
            $installed = $true
        } catch {
            Log "ERR" "Erro critico de Rede. Cancele e tente novamente mais tarde."
        }
    }

    if ($installed) {
        Log "LOG" "Extraindo pacote Oficial do Millennium localmente em: $steam"
        try {
            Expand-Archive -Path $milZipPath -DestinationPath $steam -Force
            Log "OK" "Millennium Hook (wsock32) e sub-pastas extraidos."
            $millenniumInstalling = $true
        } catch {
            Log "ERR" "Erro de IO ou Permissao durante extracao oficial."
        }
        if (Test-Path $milZipPath) { Remove-Item $milZipPath -Force -ErrorAction SilentlyContinue }
    }
} else {
    Log "INFO" "Millennium Oficial ja instalado - Sem necessidade de intervencao."
}

# --- PHASE 3: Plugin ---
if (!(Test-Path (Join-Path $steam "plugins"))) {
    New-Item -Path (Join-Path $steam "plugins") -ItemType Directory *> $null
}

$PathPlugin = Join-Path $steam "plugins\$name"

foreach ($plugin in Get-ChildItem -Path (Join-Path $steam "plugins") -Directory) {
    $testpath = Join-Path $plugin.FullName "plugin.json"
    if (Test-Path $testpath) {
        $json = Get-Content $testpath -Raw | ConvertFrom-Json
        if ($json.name -eq $name) {
            Log "INFO" "Repositorio do Plugin encontrado: Atualizando pacotes internos..."
            $PathPlugin = $plugin.FullName
            break
        }
    }
}

$subPath = Join-Path $env:TEMP "$name.zip"
Log "LOG" "Puxando repositorio direto de seu GitHub (Bypass LT)..."
try {
    Invoke-WebRequest -Uri $link -OutFile $subPath *> $null
} catch {
    Log "ERR" "Falha HTTP no acesso ao repositorio do LuaTools."
    exit
}

if (!(Test-Path $subPath)) {
    Log "ERR" "Bypass Cloud: Arquivo origin nao alcancado"
    exit
}

Log "LOG" "Descompactando Engine..."
try {      
    $zip = [System.IO.Compression.ZipFile]::OpenRead($subPath)
    foreach ($entry in $zip.Entries) {
        $destinationPath = Join-Path $PathPlugin $entry.FullName
        if (-not $entry.FullName.EndsWith('/') -and -not $entry.FullName.EndsWith('\')) {
            $parentDir = Split-Path -Path $destinationPath -Parent
            if ($parentDir -and $parentDir.Trim() -ne '') {
                $pathParts = $parentDir -replace [regex]::Escape($steam), '' -split '[\\/]' | Where-Object { $_ }
                $currentPath = $PathPlugin
                foreach ($part in $pathParts) {
                    $currentPath = Join-Path $currentPath $part
                    if (Test-Path $currentPath) {
                        $item = Get-Item $currentPath
                        if (-not $item.PSIsContainer) { Remove-Item $currentPath -Force }
                    }
                }
                [System.IO.Directory]::CreateDirectory($parentDir) | Out-Null
                [System.IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $destinationPath, $true)
            }
        }
    }
    $zip.Dispose()
} catch {
    Log "WARN" "Rotacionando para fallback nativo nativo_cmd..."
    if ($zip) { $zip.Dispose() }
    Expand-Archive -Path $subPath -DestinationPath $PathPlugin -Force
}

if (Test-Path $subPath) {
    Remove-Item $subPath -ErrorAction SilentlyContinue
}
Log "OK" "Luatools Premium extraido e implantado."

# --- PHASE 4: LIMPANDO O CAOS ANTIGO ---
Log "FIX" "Desarmando bugs e caches herdados (Limpando loop de tela)..."
# Exclui as velhas DLLs que a arquitetura legada (installer antigo) injetava.
@("millennium.dll", "python311.dll", "python310.dll", "libcore.dll") | ForEach-Object {
    $fPath = Join-Path $steam $_
    if (Test-Path $fPath) {
        Remove-Item $fPath -Force -ErrorAction SilentlyContinue
        Log "FIX" "DLL de Hook antíga deletada ($_)"
    }
}

$betaPath = Join-Path $steam "package\beta"
if (Test-Path $betaPath) { Remove-Item $betaPath -Recurse -Force }

$cfgPath = Join-Path $steam "steam.cfg"
if (Test-Path $cfgPath) { Remove-Item $cfgPath -Recurse -Force }

Remove-ItemProperty -Path "HKCU:\Software\Valve\Steam" -Name "SteamCmdForceX86" -ErrorAction SilentlyContinue
Remove-ItemProperty -Path "HKLM:\SOFTWARE\Valve\Steam" -Name "SteamCmdForceX86" -ErrorAction SilentlyContinue
Remove-ItemProperty -Path "HKLM:\SOFTWARE\WOW6432Node\Valve\Steam" -Name "SteamCmdForceX86" -ErrorAction SilentlyContinue

# --- PHASE 5: Config.json System Engine ---
$configPath = Join-Path $steam "ext/config.json"
if (-not (Test-Path $configPath)) {
    $config = @{
        plugins = @{ enabledPlugins = @($name) }
        general = @{ checkForMillenniumUpdates = $false }
    }
    New-Item -Path (Split-Path $configPath) -ItemType Directory -Force | Out-Null
    $config | ConvertTo-Json -Depth 10 | Set-Content $configPath -Encoding UTF8
} else {
    $config = (Get-Content $configPath -Raw -Encoding UTF8) | ConvertFrom-Json

    function _EnsureProperty {
        param($Object, $PropertyName, $DefaultValue)
        if (-not $Object.$PropertyName) {
            $Object | Add-Member -MemberType NoteProperty -Name $PropertyName -Value $DefaultValue -Force
        }
    }

    _EnsureProperty $config "general" @{}
    _EnsureProperty $config "general.checkForMillenniumUpdates" $false
    $config.general.checkForMillenniumUpdates = $false

    _EnsureProperty $config "plugins" @{ enabledPlugins = @() }
    _EnsureProperty $config "plugins.enabledPlugins" @()
    
    $pluginsList = @($config.plugins.enabledPlugins)
    if ($pluginsList -notcontains $name) {
        $pluginsList += $name
        $config.plugins.enabledPlugins = $pluginsList
    }
    
    $config | ConvertTo-Json -Depth 10 | Set-Content $configPath -Encoding UTF8
}

# --- PHASE 6: Inicialização ---
Write-Host
if ($milleniumInstalling) { 
    Log "WARN" "A Inicializacao principal sofrera bootstrap prolongado..."
    Log "WARN" "Isso é uma reestruturacao padrao do Millennium. Apenas aguarde e nao interrompa a Steam."
}

$exe = Join-Path $steam "steam.exe"
Start-Process $exe -ArgumentList "-clearbeta"

Log "INFO" "Bypass Phantom injetado no Bootloader (Clearbeta Ativo)"
Log "AUX" "Millennium Operando em Estagio Seguro v3.0 Oficial"
Log "OK" "Sistema Armado e Inicializando. Have a nice run! 🌿"
Write-Host
