from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # App
    APP_NAME: str = "NQ-Signal API"
    DEBUG: bool = True
    
    # Cache (Seconds)
    # Cache (Seconds)
    CACHE_TTL_PRICE: int = 60      # Real-time price (Increased 15->60s)
    CACHE_TTL_INTRADAY: int = 300  # Charts (Increased 60->300s)
    CACHE_TTL_INDICATOR: int = 600 # Indicators (Increased 300->600s)
    CACHE_TTL_FUND: int = 3600     # Fund Info

    # Proxies (Optional)
    HTTP_PROXY: str | None = None
    HTTPS_PROXY: str | None = None

    class Config:
        env_file = ".env"

settings = Settings()
