# Script PowerShell para deploy completo no Kubernetes
param (
    [string]$CloudUrl = "https://webhook.site/placeholder-endpoint"
)

Write-Host "🌿 Implantando o Laboratório de Edge Computing no Kubernetes..." -ForegroundColor Cyan

# 1. Aplica o Namespace
kubectl apply -f ./k8s/00-namespace.yaml

# 2. Aplica o Armazenamento (PVC)
kubectl apply -f ./k8s/01-storage.yaml

# 3. Atualiza e aplica o ConfigMap com a URL da Nuvem
if ($CloudUrl -ne "https://webhook.site/placeholder-endpoint") {
    Write-Host "🌐 Configurando URL da Nuvem: $CloudUrl" -ForegroundColor Yellow
    (Get-Content ./k8s/02-config.yaml) -replace 'https://webhook.site/placeholder-endpoint', $CloudUrl | Set-Content ./k8s/02-config.yaml
}
kubectl apply -f ./k8s/02-config.yaml

# 4. Aplica o Broker MQTT Mosquitto
kubectl apply -f ./k8s/03-mqtt.yaml

# 5. Aplica o Edge Processor
kubectl apply -f ./k8s/04-edge-processor.yaml

# 6. Aplica o Gerador de Sensores
kubectl apply -f ./k8s/05-sensor-generator.yaml

# 7. Aplica as Políticas de Segurança de Rede (Microsegmentação)
kubectl apply -f ./k8s/06-network-policies.yaml

Write-Host "`n⏳ Aguardando os Pods ficarem prontos no namespace 'smart-greenhouse'..." -ForegroundColor Cyan
kubectl get pods -n smart-greenhouse -w
