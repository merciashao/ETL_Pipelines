from typing import Callable, Dict

ACTION_REGISTRY: Dict[str, Callable] = {}

def register_action(func_name: str):
    """Decorator to register a function as an ETL action"""
    def wrapper(func: Callable) -> Callable:
        ACTION_REGISTRY[func_name] = func  # store reference
        return func
    return wrapper