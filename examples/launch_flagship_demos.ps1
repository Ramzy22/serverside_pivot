<# Launch all local flagship showcases.

Dash:      http://127.0.0.1:8050
React:     http://127.0.0.1:5174
Streamlit: http://127.0.0.1:8502
Runtime:   http://127.0.0.1:8765/health
#>

$demoRoot = Split-Path -Parent $PSScriptRoot
$logRoot = Join-Path $demoRoot '.runlogs'
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null

function Start-FlagshipProcess {
    param(
        [string]$Name,
        [string]$FilePath,
        [string[]]$Arguments
    )

    $stdout = Join-Path $logRoot "flagship-$Name.out.log"
    $stderr = Join-Path $logRoot "flagship-$Name.err.log"
    Start-Process -FilePath $FilePath -ArgumentList $Arguments -WorkingDirectory $demoRoot -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr | Out-Null
}

Start-FlagshipProcess -Name 'runtime' -FilePath 'python' -Arguments @('-m', 'uvicorn', 'examples.flagship_runtime_server:app', '--host', '127.0.0.1', '--port', '8765')
Start-FlagshipProcess -Name 'dash' -FilePath 'python' -Arguments @('examples/flagship_dash.py')
Start-FlagshipProcess -Name 'react' -FilePath 'node' -Arguments @('serverside_pivot/node_modules/vite/bin/vite.js', '--config', 'examples/flagship_react/vite.config.ts', '--host', '127.0.0.1', '--port', '5174')
Start-FlagshipProcess -Name 'streamlit' -FilePath 'streamlit' -Arguments @('run', 'examples/flagship_streamlit.py', '--server.address', '127.0.0.1', '--server.port', '8502', '--server.headless', 'true', '--browser.gatherUsageStats', 'false')

Write-Host 'Flagship demos launched. Open Dash :8050, React :5174, and Streamlit :8502.'
