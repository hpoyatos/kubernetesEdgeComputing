# Script para compilar as imagens Docker localmente
Write-Host "🐳 Compilando imagem: sensor-generator:latest..." -ForegroundColor Cyan
docker build -t sensor-generator:latest ./sensor-generator

Write-Host "🐳 Compilando imagem: edge-processor:latest..." -ForegroundColor Cyan
docker build -t edge-processor:latest ./edge-processor

Write-Host "🐳 Compilando imagem: cloud-mock:latest..." -ForegroundColor Cyan
docker build -t cloud-mock:latest ./cloud-mock

Write-Host "✅ Imagens Docker compiladas com sucesso!" -ForegroundColor Green
