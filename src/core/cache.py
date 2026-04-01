from cachetools import TTLCache, cached
from functools import wraps
from typing import Callable, Any
from ..config import settings

# Simple in-memory caches
# Production Upgrade: Replace with Redis client

price_cache = TTLCache(maxsize=100, ttl=settings.CACHE_TTL_PRICE)
chart_cache = TTLCache(maxsize=100, ttl=settings.CACHE_TTL_INTRADAY)
indicator_cache = TTLCache(maxsize=100, ttl=settings.CACHE_TTL_INDICATOR)
fund_cache = TTLCache(maxsize=100, ttl=settings.CACHE_TTL_FUND)
fund_history_cache = TTLCache(maxsize=100, ttl=settings.CACHE_TTL_FUND)  # Separate cache for fund history
high52w_cache = TTLCache(maxsize=100, ttl=3600)  # Separate cache for 52w high

def cache_response(ttl: int = 60):
    """
    Simple decorator for caching function results based on arguments.
    Note: For FastAPI, better to use fastapi-cache or response headers, 
    but this is sufficient for internal service methods.
    """
    def decorator(func: Callable):
        # Create a specific cache for this function to avoid key collision
        # or use one of the global caches if aligned
        local_cache = TTLCache(maxsize=100, ttl=ttl)
        
        @wraps(func)
        @cached(cache=local_cache)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator
