from __future__ import annotations

import difflib
import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

# Ensure registry is populated by importing actions package
from yaml_pipeline.action_registry import ACTION_REGISTRY
from yaml_pipeline.config_schema import RootConfig


logger = logging.getLogger(__name__)

def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file and return it as a Python dictionary"""
    with open(path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    if data is None:
        raise ValueError(f"YAML file '{path}' is empty.")
    if not isinstance(data, dict):
        raise TypeError(f"Expected a dictionary at root, got {type(data).__name__}")
    return data

def validate_config_dict(cfg: dict[str, Any]) -> RootConfig:
    """Validate dictionary object against the Pydantic schema (RootConfig).""" 
    try:
        return RootConfig.model_validate(cfg)
    except ValidationError as e:
        # Pretty-print Pydantic errors with YAML-like paths
        formatted = [
            "[YAML Schema Validation Error]",
            *[f" - {'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()]
        ]
        #Log structured error details for debugging or CI pipelines
        raise ValueError('\n'.join(formatted)) from e


def crosscheck_actions(cfg: RootConfig) -> None:
    """Ensure all actions declared in YAML exist in the action registry."""
    used = {rule.action for rule in cfg.precleaning_rules.rules}
    registered = set(ACTION_REGISTRY)
    missing = used - registered
    # If no missing actions, exits silently
    if not missing:
        return
    
    suggestions = {
        name: difflib.get_close_matches(name, registered, n=1)
        for name in missing
    }
    message_lines = ["[Action Registry Cross-Check Error] Missing actions:"]
    for name, hint in suggestions.items():
        suggestion = f" (did you mean '{hint[0]}'?)" if hint else ""
        message_lines.append(f" - '{name}'{suggestion}")
    message_lines.append("Hint: Decorate functions with @register_action('name') and import the module.")
    raise ValueError("\n".join(message_lines))


def load_and_validate(path: str | Path, verbose: bool = True) -> RootConfig:
    """Load YAML, validate schema, and verify all actions are registered."""
    cfg = validate_config_dict(load_yaml(path))  # Validated RootConfig object
    crosscheck_actions(cfg)          # Ensures all actions are registered
    if verbose:
        logger.info(f"✅ Config loaded and validated successfully: {path}")
    return cfg