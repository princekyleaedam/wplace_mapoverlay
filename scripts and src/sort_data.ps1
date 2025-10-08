New-Item -ItemType Directory -Force -Name "sorted" | Out-Null

$total = 2048
for ($i = 0; $i -le 2047; $i++) {
    $file = "tileY-$i-uncompressed.jsonl"
    $outfile = Join-Path "sorted" $file

    if (Test-Path $file) {
        Write-Host "Processing $file ($($i+1)/$total)..."
        .\jq.exe -c -s 'sort_by(.coord.tileX)[]' $file | Out-File -Encoding utf8 $outfile
    } else {
        Write-Host "Warning: $file not found, skipping."
    }
}

Write-Host "All files processed!"