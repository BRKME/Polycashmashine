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
backtest.py         — исторический бэктест (модель vs наивная оценка)
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

- [x] Phase 1: Weather model + бэктест
- [ ] Phase 2: Polymarket market scanner (сравнение с реальными ценами)
- [ ] Phase 3: Auto-trade через py-clob-client
- [ ] Phase 4: GitHub Actions (каждые 6 часов по обновлению моделей)

## Лицензия

MIT
