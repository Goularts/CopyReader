# TradingView CopyReader — Leitor de Ordens + Interface de Execução

CopyReader é um leitor de ordens do TradingView, capaz de capturar eventos tanto do Paper Trading quanto de corretoras conectadas, utilizando o Chromium (Playwright) para interceptar mensagens internas da plataforma.

O objetivo do projeto é transformar qualquer ordem gerada no TradingView em um sinal padronizado JSON, enviado para um endpoint genérico.  
A partir disso, qualquer sistema externo pode consumir esse JSON e executar ações em outras plataformas de trading.

---

## Como funciona

### 1. Monitoramento via Chromium
O TradingView é carregado dentro de uma instância controlada do Playwright.  
O arquivo `SocketMonitor.cs` intercepta mensagens do WebSocket interno do TradingView.

---

### 2. Conversão para JSON
Após interpretar o estado da posição, o CopyReader cria um JSON simples, enviado para o endpoint configurado. Por exemplo:

```json
{
  "action": "C",
  "ticker": "ES",
  "close": 5412.25,
  "source": "TradingView"
}
