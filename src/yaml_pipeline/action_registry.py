from typing import Callable, Dict

ACTION_REGISTRY: Dict[str, Callable] = {}

def register_action(func_name: str):
    """Decorator to register a function as an ETL action"""
    def wrapper(func: Callable) -> Callable:
        if func_name in ACTION_REGISTRY:
            raise ValueError(f"Action '{func_name}' is already registered with {ACTION_REGISTRY[func_name].__name__}")
        ACTION_REGISTRY[func_name] = func  # store reference
        return func
    return wrapper