from typing import List, Dict, Union, Literal
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated


# ----- Base -----
class BaseRule(BaseModel):
    model_config = ConfigDict(    # a dictionary of per-model configuration options
        extra = "forbid",             # any unexpected fields in input will raise an error
        populate_by_name = True,      # Accept both the alias and the real field name
        str_strip_whitespace = True,  # trims whitespace
    )
    task_name: str
    description: str
    type: Literal[
        "schema-level",
        "row-level",
        "column-level",
        "data-type-level",
        "domain-specific-transformations",
        "validation-sanity-checks",
    ]

# ----- Parameters per action -----
class ConvertCRSParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    to_crs: str = Field(alias="to")

class RenameColumnsParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mappings: Dict[str, str]

# ----- Action-bound rule models (discriminated by `action`) -----
class ConvertCRSRule(BaseRule):
    action: Literal["convert_crs"]
    parameters: ConvertCRSParams

class RenameColumnsRule(BaseRule):
    action: Literal['rename_columns']
    parameters: RenameColumnsParams

# The discriminated union: Pydantic will "choose" the right model based on `action`
Rule = Annotated[
    Union[ConvertCRSRule, RenameColumnsRule],
    Field(discriminator="action"),
]

# Define the block under precleaning_rules in YAML file
class PrecleaningRules(BaseModel):
    model_config = ConfigDict(extra="forbid")
    description: str
    rules: List[Rule]

# Define the entire YAML file ensuring it starts with exactly one top-level key
class RootConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    precleaning_rules: PrecleaningRules
