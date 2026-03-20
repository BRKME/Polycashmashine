# 🌤️ Polymarket Weather Bot

**Автономный торговый бот для погодных рынков Polymarket.**

Использует ансамблевые метеопрогнозы (ECMWF/GFS) для обнаружения mispricing в погодных prediction markets.

## Стратегия

Polymarket хостит 400+ погодных рынков (температура в NYC, Tel Aviv, Seoul, London, Shanghai).  
Рынки резолвятся по данным Weather Underground.

**Edge:** Ансамблевые модели (51 прогон ECMWF, 31 прогон GFS) дают вероятностное распределение.  
Толпа на Polymarket торгует по интуиции. Разница = прибыль.

## Быстрый старт

```bash
pip install -r requirements.txt

# Тест: прогноз для NYC на завтра
python weather_model.py

# Бэктест: 30 дней, NYC
python backtest.py nyc

# Бэктест: несколько городов
python backtest.py nyc london tel_aviv
```

## Архитектура

```
weather_model.py    — ансамблевые прогнозы → вероятности по температурным бинам
market_scanner.py   — Polymarket API → реальные цены → edge сигналы
trader.py           — risk management → CLOB API → размещение ордеров
backtest.py         — исторический бэктест (модель vs рынок)
config.py           — города, пороги, API endpoints
```

## Источники данных

| Источник | Что даёт | API |
|---|---|---|
| Open-Meteo Ensemble | 51 прогон ECMWF + 31 GFS | Бесплатно, без ключа |
| Open-Meteo Historical | Фактические наблюдения | Бесплатно, без ключа |
| Polymarket Gamma API | Текущие цены рынков | Бесплатно |
| Polymarket CLOB API | Размещение ордеров | Нужен кошелёк Polygon |

## Фазы

- [x] Phase 1: Weather model + бэктест (v3 — 71% hit rate, +$170/60 дней)
- [x] Phase 2: Live market scanner (реальные цены Polymarket, avg edge +26.7%)
- [x] Phase 3: Auto-trade через py-clob-client
- [ ] Phase 4: Production hardening (Telegram alerts, P&L tracking)

## Auto-Trade

```bash
# Dry run (по умолчанию) — показывает что бы сделал, но не торгует
python trader.py

# Live trading — размещает реальные ордера
python trader.py --live
```

**Для live trading нужны GitHub Secrets:**
- `POLY_PRIVATE_KEY` — приватный ключ Polygon кошелька
- `POLY_FUNDER` — адрес Polymarket proxy/funder
- `POLY_SIG_TYPE` — тип подписи (0=EOA, 1=email, 2=browser)

**Risk management:**
- $2 за ставку (настраивается BET_SIZE)
- $20 дневной лимит (DAILY_BUDGET)
- Минимум 15% edge (MIN_TRADE_EDGE)
- Макс 3 ставки на город в день

## Лицензия

MIT
