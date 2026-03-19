"""
Configuration for Polymarket Weather Bot.

Cities, temperature bins, and API settings.
"""

# Polymarket weather markets use Fahrenheit temperature bins (2°F wide)
# Resolution source: Weather Underground station data
# Example: "Highest temperature in NYC on March 18?" → bins like 36-37°F, 38-39°F

CITIES = {
    "nyc": {
        "name": "New York City",
        "lat": 40.7790,  # LaGuardia Airport
        "lon": -73.8740,
        "timezone": "America/New_York",
        "unit": "fahrenheit",
        "bin_width": 2,
    },
    "tel_aviv": {
        "name": "Tel Aviv",
        "lat": 32.0853,
        "lon": 34.7818,
        "timezone": "Asia/Jerusalem",
        "unit": "celsius",
        "bin_width": 1,
    },
    "seoul": {
        "name": "Seoul",
        "lat": 37.5665,
        "lon": 126.9780,
        "timezone": "Asia/Seoul",
        "unit": "celsius",
        "bin_width": 1,
    },
    "london": {
        "name": "London",
        "lat": 51.4700,
        "lon": -0.4543,
        "timezone": "Europe/London",
        "unit": "celsius",
        "bin_width": 1,
    },
    "shanghai": {
        "name": "Shanghai",
        "lat": 31.2304,
        "lon": 121.4737,
        "timezone": "Asia/Shanghai",
        "unit": "celsius",
        "bin_width": 1,
    },
    "hong_kong": {
        "name": "Hong Kong",
        "lat": 22.3193,
        "lon": 114.1694,
        "timezone": "Asia/Hong_Kong",
        "unit": "celsius",
        "bin_width": 1,
    },
    "warsaw": {
        "name": "Warsaw",
        "lat": 52.2297,
        "lon": 21.0122,
        "timezone": "Europe/Warsaw",
        "unit": "celsius",
        "bin_width": 1,
    },
    "tokyo": {
        "name": "Tokyo",
        "lat": 35.6762,
        "lon": 139.6503,
        "timezone": "Asia/Tokyo",
        "unit": "celsius",
        "bin_width": 1,
    },
    "la": {
        "name": "Los Angeles",
        "lat": 33.9425,  # LAX
        "lon": -118.4081,
        "timezone": "America/Los_Angeles",
        "unit": "fahrenheit",
        "bin_width": 2,
    },
    "chicago": {
        "name": "Chicago",
        "lat": 41.9742,  # O'Hare
        "lon": -87.9073,
        "timezone": "America/Chicago",
        "unit": "fahrenheit",
        "bin_width": 2,
    },
    "miami": {
        "name": "Miami",
        "lat": 25.7959,  # MIA
        "lon": -80.2870,
        "timezone": "America/New_York",
        "unit": "fahrenheit",
        "bin_width": 2,
    },
    "paris": {
        "name": "Paris",
        "lat": 49.0097,  # Orly
        "lon": 2.3381,
        "timezone": "Europe/Paris",
        "unit": "celsius",
        "bin_width": 1,
    },
}

# Open-Meteo ensemble models
# GFS has 31 members, ECMWF IFS has 51 members
ENSEMBLE_MODELS = ["gfs_seamless", "ecmwf_ifs025"]

# Edge thresholds
MIN_EDGE_PERCENT = 10.0   # Minimum edge to consider (model prob - market prob)
MIN_BET_EDGE = 15.0       # Minimum edge for auto-trade

# Backtest settings
BACKTEST_DAYS = 60         # How many past days to backtest
LOOKBACK_HOURS = 24        # How many hours before resolution to evaluate forecast

# API
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
OPEN_METEO_HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
