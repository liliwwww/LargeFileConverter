# find_delimiter.ps1 — 探测文件内容，推荐可用的分隔符
# 用法: .\find_delimiter.ps1 -FilePath "D:\data\file.txt"
# 用法: .\find_delimiter.ps1 -FilePath "D:\data\file.txt" -SampleMB 50

param(
    [Parameter(Mandatory=$true)]
    [string]$FilePath,

    [int]$SampleMB = 20        # 采样大小(MB)，默认扫描前 20MB，0 = 全文件扫描
)

if (-not (Test-Path $FilePath)) {
    Write-Host "错误：文件不存在: $FilePath" -ForegroundColor Red
    exit 1
}

# ── 候选分隔符（半角英文，按常用优先级排列）────────────────────────────────────
$candidates = @(
    @{ Char='|';  Byte=0x7C; Desc='竖线' },
    @{ Char="`t"; Byte=0x09; Desc='Tab' },
    @{ Char='^';  Byte=0x5E; Desc='脱字符' },
    @{ Char='~';  Byte=0x7E; Desc='波浪线' },
    @{ Char='`';  Byte=0x60; Desc='反引号' },
    @{ Char=';';  Byte=0x3B; Desc='分号' },
    @{ Char='#';  Byte=0x23; Desc='井号' },
    @{ Char='@';  Byte=0x40; Desc='At符号' },
    @{ Char='!';  Byte=0x21; Desc='感叹号' },
    @{ Char='%';  Byte=0x25; Desc='百分号' },
    @{ Char='&';  Byte=0x26; Desc='And符号' },
    @{ Char='*';  Byte=0x2A; Desc='星号' },
    @{ Char='+';  Byte=0x2B; Desc='加号' },
    @{ Char='=';  Byte=0x3D; Desc='等号' },
    @{ Char=',';  Byte=0x2C; Desc='逗号' },
    @{ Char='/';  Byte=0x2F; Desc='斜杠' },
    @{ Char='\';  Byte=0x5C; Desc='反斜杠' }
)

# ── 文件信息 ──────────────────────────────────────────────────────────────────
$fileSize = (Get-Item $FilePath).Length
$scanBytes = if ($SampleMB -eq 0) { $fileSize } else { [Math]::Min([long]$SampleMB * 1MB, $fileSize) }
$isSample = $scanBytes -lt $fileSize

Write-Host ""
Write-Host "文件路径 : $FilePath"
Write-Host "文件大小 : $("{0:N0}" -f $fileSize) 字节 ($("{0:F2}" -f ($fileSize / 1GB)) GB)"
if ($isSample) {
    Write-Host "扫描范围 : 前 $SampleMB MB（如需全文件扫描请加 -SampleMB 0）" -ForegroundColor Yellow
} else {
    Write-Host "扫描范围 : 全文件" -ForegroundColor Cyan
}
Write-Host ""

# ── 初始化计数器（只统计候选字符）─────────────────────────────────────────────
$counts = @{}
foreach ($c in $candidates) {
    $counts[$c.Byte] = [long]0
}

# ── 二进制分块扫描（ASCII字符在UTF-8/GBK中字节值相同，直接按字节统计）──────────
$chunkSize = 4MB
$totalRead = [long]0
$fs = [System.IO.File]::OpenRead($FilePath)
$buf = New-Object byte[] $chunkSize

try {
    Write-Host "扫描中..." -NoNewline
    $dotInterval = [Math]::Max(1, [int]($scanBytes / $chunkSize / 20))
    $chunkIndex = 0

    while ($totalRead -lt $scanBytes) {
        $toRead = [Math]::Min([long]$chunkSize, $scanBytes - $totalRead)
        $read = $fs.Read($buf, 0, [int]$toRead)
        if ($read -eq 0) { break }

        # 统计候选字节出现次数
        for ($i = 0; $i -lt $read; $i++) {
            $b = $buf[$i]
            if ($counts.ContainsKey([int]$b)) {
                $counts[[int]$b]++
            }
        }

        $totalRead += $read
        $chunkIndex++
        if ($chunkIndex % $dotInterval -eq 0) { Write-Host "." -NoNewline }
    }
} finally {
    $fs.Close()
}

Write-Host " 完成`n"

# ── 输出结果 ──────────────────────────────────────────────────────────────────
$safe   = @()   # 文件中完全不存在的字符
$unsafe = @()   # 文件中存在的字符

foreach ($c in $candidates) {
    $count = $counts[$c.Byte]
    if ($count -eq 0) {
        $safe += $c
    } else {
        $c['Count'] = $count
        $unsafe += $c
    }
}

# 推荐（安全）
Write-Host "══ 推荐分隔符（文件中不存在，可安全使用）══════════════════" -ForegroundColor Green
if ($safe.Count -eq 0) {
    Write-Host "  无完全安全的候选字符，请从下方低频字符中选择" -ForegroundColor Yellow
} else {
    foreach ($c in $safe) {
        $display = if ($c.Char -eq "`t") { '\t(Tab)' } else { $c.Char }
        Write-Host ("  {0,-6} {1}" -f $display, $c.Desc) -ForegroundColor Green
    }
}

Write-Host ""

# 已使用（不安全，按出现次数排序）
Write-Host "══ 已存在于文件中（不建议用作分隔符）════════════════════════"
$unsafe | Sort-Object Count | ForEach-Object {
    $display = if ($_.Char -eq "`t") { '\t(Tab)' } else { $_.Char }
    $countStr = if ($isSample) { "$("{0:N0}" -f $_.Count)（采样）" } else { "$("{0:N0}" -f $_.Count)" }
    Write-Host ("  {0,-6} {1,-10}  出现 {2} 次" -f $display, $_.Desc, $countStr)
}

Write-Host ""
if ($isSample -and $safe.Count -eq 0) {
    Write-Host "提示: 当前为采样扫描，建议用 -SampleMB 0 全文件扫描后再决定" -ForegroundColor Yellow
}
