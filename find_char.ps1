# find_char.ps1 — 检查文件中是否包含特定字符，找到则打印匹配行
# 用法: .\find_char.ps1 -FilePath "D:\data\file.txt"
# 用法: .\find_char.ps1 -FilePath "D:\data\file.txt" -Char "|" -MaxLines 20

param(
    [Parameter(Mandatory=$true)]
    [string]$FilePath,

    [string]$Char = "|",          # 要查找的字符，默认 |

    [int]$MaxLines = 10           # 最多显示几行
)

# ── 检查文件 ──────────────────────────────────────────────────────────────────
if (-not (Test-Path $FilePath)) {
    Write-Host "错误：文件不存在: $FilePath" -ForegroundColor Red
    exit 1
}

$fileSize = (Get-Item $FilePath).Length
Write-Host ""
Write-Host "文件路径 : $FilePath"
Write-Host "文件大小 : $("{0:N0}" -f $fileSize) 字节 ($("{0:F2}" -f ($fileSize / 1GB)) GB)"
Write-Host "查找字符 : '$Char'"
Write-Host ""

# ── 自动检测文件编码 ───────────────────────────────────────────────────────────
function Get-FileEncoding($path) {
    $bytes = New-Object byte[] 4
    $fs = [System.IO.File]::OpenRead($path)
    $null = $fs.Read($bytes, 0, 4)
    $fs.Close()

    # BOM 检测
    if ($bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
        return [System.Text.Encoding]::UTF8
    }
    if ($bytes[0] -eq 0xFF -and $bytes[1] -eq 0xFE) {
        return [System.Text.Encoding]::Unicode      # UTF-16 LE
    }
    if ($bytes[0] -eq 0xFE -and $bytes[1] -eq 0xFF) {
        return [System.Text.Encoding]::BigEndianUnicode
    }

    # 无 BOM：尝试 UTF-8 验证（读前 200KB）
    $sample = New-Object byte[] ([Math]::Min([long]200000, (Get-Item $path).Length))
    $fs = [System.IO.File]::OpenRead($path)
    $null = $fs.Read($sample, 0, $sample.Length)
    $fs.Close()

    $utf8 = New-Object System.Text.UTF8Encoding $false  # 无 BOM UTF-8
    try {
        $decoded = $utf8.GetString($sample)
        # 若解码后无替换字符(0xFFFD)，认为是 UTF-8
        if ($decoded -notmatch [char]0xFFFD) {
            return $utf8
        }
    } catch {}

    # 默认回退到系统 GBK（中文 Windows 代码页 936）
    return [System.Text.Encoding]::GetEncoding(936)
}

$encoding = Get-FileEncoding $FilePath
Write-Host "文件编码 : $($encoding.EncodingName)"

# ── 搜索 ──────────────────────────────────────────────────────────────────────
$matchCount  = 0
$lineCount   = 0
$firstMatch  = $null
$sampleLines = @()

$reader = [System.IO.StreamReader]::new($FilePath, $encoding)
try {
    while ($null -ne ($line = $reader.ReadLine())) {
        $lineCount++
        if ($line.Contains($Char)) {
            $matchCount++
            if ($null -eq $firstMatch) {
                $firstMatch = $lineCount
            }
            if ($sampleLines.Count -lt $MaxLines) {
                $sampleLines += [PSCustomObject]@{ LineNo = $lineCount; Content = $line }
            }
        }
    }
} finally {
    $reader.Close()
}

# ── 输出结果 ──────────────────────────────────────────────────────────────────
Write-Host "总行数   : $("{0:N0}" -f $lineCount)"

if ($matchCount -gt 0) {
    Write-Host "结论     : " -NoNewline
    Write-Host "包含字符 '$Char'" -ForegroundColor Green
    Write-Host "匹配行数 : $("{0:N0}" -f $matchCount)"
    Write-Host "首次出现 : 第 $firstMatch 行"

    Write-Host ""
    Write-Host "── 前 $MaxLines 条匹配行 ──────────────────────────────────"
    foreach ($item in $sampleLines) {
        $preview = if ($item.Content.Length -gt 200) { $item.Content.Substring(0, 200) + "..." } else { $item.Content }
        Write-Host ("[{0,8}] {1}" -f $item.LineNo, $preview)
    }
} else {
    Write-Host "结论     : " -NoNewline
    Write-Host "未找到字符 '$Char'" -ForegroundColor Yellow
}

Write-Host ""
