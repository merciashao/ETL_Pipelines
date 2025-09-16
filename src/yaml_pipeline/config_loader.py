from __future__ import annotations
from pathlib import Path
import difflib
from typing import Any

import yaml
from pydantic import ValidationError

# Ensure registry is populated by importing actions package
from yaml_pipeline.action_registry import ACTION_REGISTRY
from yaml_pipeline.config_schema import RootConfig


def load_yaml(path: str | Path) -> dict[str, Any]:
    # Load a YAML file from disk and turns it into a Python dictionary
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def validate_config_dict(cfg: dict[str, Any]) -> RootConfig:
    # Ensure that the dictionary object matches the strict schema defined in RootConfig 
    try:
        return RootConfig.model_validate(cfg)
    except ValidationError as e:
        # Pretty-print Pydantic errors with YAML-like paths
        lines = ['[YAML Schema Validation Error]']
        for err in e.errors():
            loc = ".".join(str(p) for p in err["loc"])
            lines.append(f" - {loc}: {err['msg']}")
        raise ValueError('\n'.join(lines)) from e

def crosscheck_actions(cfg: RootConfig) -> None:
    # Check all actions declared in the YAML config actually exist in action registry
    used_actions: list[str] = [
        rule.action for rule in cfg.precleaning_rules.rules
    ]
    missing: list[str] = [a for a in used_actions if a not in ACTION_REGISTRY]
    # If no missing actions, exits silently
    if not missing:
        return
    
    lines = ["[Action Registry Cross-Check Error] The following actions are not registered:"]
    for a in missing:
        suggestion = difflib.get_close_matches(a, ACTION_REGISTRY.keys(), n=1)
        if suggestion:
            # extracts the first (and only) string from that list
            lines.append(f" - '{a}' (did you mean '{suggestion[0]}'?)")
        else:
            lines.append(f" - '{a}'")
    lines.append("Hint: Ensure the function is decorated with @register_action('name') "
                 "and the module is imported by src.actions.__init__.")
    raise ValueError("\n".join(lines))

def load_and_validate(path: str | Path) -> RootConfig:
    # Orchestrate the entire config-loading pipeline
    raw = load_yaml(path)            # Raw Python dict
    cfg = validate_config_dict(raw)  # Validated RootConfig object
    crosscheck_actions(cfg)          # Ensures all actions are registered
    return cfg