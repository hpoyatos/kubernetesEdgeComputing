# 🎓 Roteiro de Aula: Segurança em Edge Computing (Estufa Inteligente no Kubernetes)

Este documento é o guia didático passo a passo para o professor conduzir a aula prática de **Segurança em Edge Computing** utilizando este laboratório.

---

## 🎯 Objetivos de Aprendizagem da Aula

Ao final da aula, os alunos serão capazes de:
1. **Compreender a Arquitetura em 3 Camadas da Borda:**
   - *Extrema Borda (Far Edge):* Sensores IoT geradores de telemetria.
   - *Borda Próxima (Near/On-Premise Edge):* Gateway e Cluster Kubernetes local processando e filtrando dados.
   - *Nuvem Central (Cloud):* Armazenamento e inteligência consolidada de longo prazo.
2. **Identificar Ameaças de Segurança em Ambientes IoT/Edge:**
   - *Data Poisoning / Sensor Spoofing:* Injeção deliberada de dados falsos para enganar automações agrícolas/industriais.
   - *Exposição de Dados / Falta de Minimização de Dados (LGPD/GDPR):* Enviar dados brutos não higienizados para a nuvem.
   - *Movimentação Lateral:* Sensores comprometidos tentando atacar outros dispositivos na rede.
3. **Aplicar Controles e Mitigações de Segurança Práticos:**
   - Detecção estatística de anomalias em tempo real na Borda (IQR / Outliers).
   - Minimização de dados (de 600 leituras/min para 1 payload/min).
   - Persistência e resiliência offline com SQLite.
   - Microsegmentação de rede com Kubernetes Network Policies.

---

## ⏱️ Cronograma Sugerido (60 a 90 minutos)

| Tempo | Etapa | Descrição |
| :--- | :--- | :--- |
| **00 - 15 min** | **Conceituação Teórica** | Por que segurança em Edge Computing é diferente da nuvem tradicional? Apresentação do cenário da estufa inteligente. |
| **15 - 35 min** | **Demo 1: O Fluxo de Dados & Injeção de Anomalias** | Mostrar os 100 sensores gerando leituras a cada 10s e os 4 sensores adulterados. |
| **35 - 55 min** | **Demo 2: Processamento de Borda, SQLite & Sanitização** | Mostrar o Edge Processor recebendo MQTT, salvando no SQLite e expurgando os ataques antes de enviar para a nuvem. |
| **55 - 75 min** | **Demo 3: Microsegmentação no Kubernetes** | Analisar e testar as `NetworkPolicies` impedindo sensores de acessar a internet e a nuvem diretamente. |
| **75 - 90 min** | **Discussão & Perguntas** | Debate reflexivo com a turma sobre custos, ataques reais e arquitetura de borda. |

---

## 🖥️ Roteiro Prático de Demonstração (Passo a Passo)

### 📌 Preparação Antes da Aula (2 minutos)
1. Abra uma aba no navegador com o [Webhook.site](https://webhook.site).
2. Copie a sua URL única gerada (ex: `https://webhook.site/abc-123-xyz`).
3. Inicie o laboratório via Docker Compose ou Kubernetes:
   ```bash
   # Exemplo via Docker Compose:
   $env:CLOUD_API_URL="https://webhook.site/abc-123-xyz"
   docker compose up -d
   ```

---

### 📍 Ponto 1: Ameaça de Data Poisoning & O Ambiente dos Sensores

**O que falar para os alunos:**
> *"Em um ambiente real de IoT agrícola ou industrial, sensores ficam expostos fisicamente em campo. Um atacante pode alterar a calibração de um sensor ou injetar leituras falsas (por exemplo, simulando que a estufa está a 95°C ou -18°C) para forçar o sistema a acionar ventiladores, aquecedores ou irrigação desnecessariamente, destruindo a plantação."*

**Comando de Demonstração:**
```bash
# Inspecione os logs do gerador de sensores:
docker logs -f sensor-generator
# (ou no K8s: kubectl logs -n smart-greenhouse -l app=sensor-generator -f)
```

**O que destacar na tela:**
- A cada 10 segundos, são geradas 100 leituras.
- A maioria dos sensores oscila naturalmente em torno de **24°C**.
- Sensores específicos injetam anomalias gritantes:
  - `sensor_013`: ~89.4°C *(Simulação de queima/superaquecimento)*
  - `sensor_042`: ~ -18.2°C *(Simulação de bug/congelamento)*
  - `sensor_077`: ~145.0°C *(Simulação de ataque ativo de spoofing)*
  - `sensor_099`: ~72.3°C *(Simulação de pico elétrico)*

---

### 📍 Ponto 2: O Papel do Edge Computing (Ingestão, Persistência Local & Sanitização)

**O que falar para os alunos:**
> *"Por que não mandamos todas as 600 leituras por minuto direto para a Nuvem?*
> 1. *Custo de tráfego e latência de rede.*
> 2. *Poluição do banco central com dados falsos.*
> 3. *Se a internet cair, a estufa não pode parar.*
> *Por isso, o Edge Processor recebe as mensagens via MQTT, grava o histórico bruto no SQLite local da estufa e atua como uma barreira de segurança sanitizando os dados."*

**Comando de Demonstração:**
```bash
# Inspecione os logs do processador de borda:
docker logs -f edge-processor
# (ou no K8s: kubectl logs -n smart-greenhouse -l app=edge-processor -f)
```

**O que destacar na tela:**
- A cada 60 segundos, o processador junta as ~600 leituras brutas acumuladas.
- O algoritmo **IQR (Interquartile Range)** identifica automaticamente os sensores anômalos sem precisar de regras manuais fixas.
- O sistema exibe:
  ```text
  📥 Leituras Brutas Recebidas: 600
  🛡️ Outliers Detectados e Removidos: 24
  🚨 Sensores Anômalos Flagrados: ['sensor_013', 'sensor_042', 'sensor_077', 'sensor_099']
  ✅ Média Sanitizada da Estufa: 24.12°C (Min: 21.85°C, Max: 26.32°C)
  🌐 Enviando 1 pacote consolidado para a Nuvem
  ```

---

### 📍 Ponto 3: Inspecionando o Banco de Dados Local na Borda (SQLite)

**O que falar para os alunos:**
> *"Vejam que temos auditabilidade total na borda. Todas as leituras brutas continuam registradas no SQLite para auditoria forense, enquanto a nuvem recebe apenas o resumo confiável."*

**Comando de Demonstração:**
```bash
# Acessar o pod / container e rodar consultas SQL:
docker exec -it edge-processor sqlite3 /data/edge_telemetry.db "SELECT sensor_id, temperature, received_at FROM raw_telemetry ORDER BY id DESC LIMIT 10;"

# Consultar os agregados de 1 minuto com os status de envio:
docker exec -it edge-processor sqlite3 /data/edge_telemetry.db "SELECT id, window_start, total_raw_readings, valid_readings_count, outliers_detected_count, avg_clean_temperature, cloud_dispatch_status FROM minute_aggregates ORDER BY id DESC LIMIT 5;"
```

---

### 📍 Ponto 4: Visualizando a Chegada na Nuvem (Webhook.site)

**O que mostrar aos alunos:**
- Abra a aba do **Webhook.site** no projetor.
- Mostre a requisição HTTP POST chegando exatamente 1 vez por minuto.
- Destaque o payload JSON limpo:
  - `average_clean`: ~24.1°C
  - `anomalies_detected`: `true`
  - `data_reduction_ratio`: `99.83%` (Economia colossal de banda e requisições HTTP!)

---

### 📍 Ponto 5: Microsegmentação com Kubernetes Network Policies

**O que falar para os alunos:**
> *"Se um hacker tiver acesso físico a um sensor ou explorar uma vulnerabilidade em seu firmware, o que ele consegue fazer? No Kubernetes tradicional sem Network Policies, ele poderia escanear toda a rede do cluster e se comunicar com o banco de dados da empresa ou outros serviços. Com Network Policies, nós confinamos o sensor."*

**Apresentar o arquivo [`k8s/06-network-policies.yaml`](file:///c:/Users/henrique.poyatos/CodeProjects/kubernetesEdgeComputing/k8s/06-network-policies.yaml):**
- **Default Deny:** Bloqueia todo tráfego não explicitamente autorizado.
- **Sensor Isolation:** O pod do sensor só tem permissão de saída para a porta `1883` do `mqtt-broker`. Se tentar fazer `curl https://google.com` ou `ping` outro pod, o tráfego é descartado na camada de rede.
- **Edge Gateway Egress:** Apenas o `edge-processor` possui regra de saída para a Nuvem externa.

---

## ❓ Perguntas Provocativas para a Turma (Debate em Sala)

1. **Pergunta:** *"O que aconteceria com os atuadores da estufa (ar condicionado / aquecedor) se não tivéssemos o filtro de outliers na Borda e calculássemos a média com todas as 100 leituras?"*
   - **Resposta esperada:** Os valores de 145°C e 89°C puxariam a média para cima (~26°C a 29°C), ligando o resfriamento máximo e congelando a plantação de verdade!
2. **Pergunta:** *"Por que não usamos autenticação e criptografia direta do sensor para a Nuvem sem passar pela Borda?"*
   - **Resposta esperada:** Sensores IoT de baixo custo muitas vezes não têm memória/processador para manter sessões TLS pesadas nem conexão 4G/5G individual. O Edge Gateway concentra a segurança e descarrega a nuvem.
3. **Pergunta:** *"Se a conexão de internet da estufa for cortada por uma tempestade, o que acontece?"*
   - **Resposta esperada:** O Edge Processor continua funcionando 100% offline, controlando a estufa localmente e gravando tudo no SQLite. Quando a conexão voltar, ele sincroniza.
