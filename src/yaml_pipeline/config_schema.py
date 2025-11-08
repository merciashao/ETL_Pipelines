from typing import Any, Literal, Optional, Union
from pydantic import BaseModel, Field
from typing_extensions import Annotated


# ----- Base -----
class BaseRule(BaseModel):
    """Base schema shared by all pre-cleaning rules."""
    model_config = {  # a dictionary of per-model configuration options
        "extra": "forbid",             # any unexpected fields in input will raise an error
        "populate_by_name": True,      # Accept both the alias and the real field name
        "str_strip_whitespace": True,  # trims whitespace
    }   
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
    model_config = {"extra": "forbid"}
    to_crs: str

class RenameColumnsParams(BaseModel):
    model_config = {"extra": "forbid"}
    mappings: dict[str, str]

class StripWhitespaceParams(BaseModel):
    model_config = {"extra": "forbid"}
    dtype: str
    remove_newlines: bool

class TypoMappingParams(BaseModel):
    model_config = {"extra": "forbid"}
    mode: list[Literal["exact", "regex"]]
    columns: dict[str, dict[str, Any]]

class ConvertDatetimeParams(BaseModel):
    model_config = {"extra": "forbid"}
    columns: list[str]
    regex: str
    shift: int
    format: str
    cast_to: str
    errors: str

class DropNullsParams(BaseModel):
    model_config = {"extra": "forbid"}
    index: list[int]

class DropByPairsParams(BaseModel):
    model_config = {"extra": "forbid"}
    index_name: str
    columns: list[str]
    exclude_pairs: list[list[Union[int, str]]]

class ExplodeVillageParams(BaseModel):
    model_config = {"extra": "forbid"}
    source_column: str
    delimiter: str
    keep_original_as: str
    sort_by: str
    reset_index: bool
    new_index: str

class SplitDuplicatesParams(BaseModel):
    model_config = {"extra": "forbid"}
    subset: list[str]
    output_aliases: dict[str, str]
    sort_duplicates_by: str

class DissolveVillagesParams(BaseModel):
    model_config = {"extra": "forbid"}
    input: str
    where: str
    by: str
    aggregation_rules: dict[str, Any]

class ConcatFinalizeParams(BaseModel):
    model_config = {"extra": "forbid"}
    input: list[str]
    ignore_index: bool
    sort_by: str
    reset_index: bool
    index_name: str
    output_alias: str

# ----- Action-bound rule models (discriminated by `action`) -----
class ConvertCRSRule(BaseRule):
    action: Literal["convert_crs"]
    parameters: ConvertCRSParams

class RenameColumnsRule(BaseRule):
    action: Literal['rename_columns']
    parameters: RenameColumnsParams

class StripWhitespaceRule(BaseRule):
    action: Literal['strip_whitespace']
    parameters: StripWhitespaceParams

class TypoMappingRule(BaseRule):
    action: Literal['typo_mapping']
    parameters: TypoMappingParams

class ConvertDatetimeRule(BaseRule):
    action: Literal['convert_datetime']
    parameters: ConvertDatetimeParams

class DropNullsRule(BaseRule):
    action: Literal['dropnulls']
    parameters: DropNullsParams

class DropByPairsRule(BaseRule):
    action: Literal['drop_by_pairs']
    parameters: DropByPairsParams

class ExplodeVillageRule(BaseRule):
    action: Literal['explode_village']
    parameters: ExplodeVillageParams

class SplitDuplicatesRule(BaseRule):
    action: Literal['split_duplicates']
    parameters: SplitDuplicatesParams

class DissolveVillagesRule(BaseRule):
    action: Literal['dissolve_villages']
    parameters: DissolveVillagesParams

class ConcatFinalizeRule(BaseRule):
    action: Literal['concat_finalize']
    parameters: ConcatFinalizeParams

# The discriminated union: Pydantic will "choose" the right model based on `action`
Rule = Annotated[
    Union[
        ConvertCRSRule,
        RenameColumnsRule,
        StripWhitespaceRule,
        TypoMappingRule,
        ConvertDatetimeRule,
        DropNullsRule,
        DropByPairsRule,
        ExplodeVillageRule,
        SplitDuplicatesRule,
        DissolveVillagesRule,
        ConcatFinalizeRule,
    ],
    Field(discriminator="action"),
]

# Define the block under precleaning_rules in YAML file
class PrecleaningRules(BaseModel):
    model_config = {"extra": "forbid"}
    description: str
    rules: list[Rule]

# Define the entire YAML file ensuring it starts with exactly one top-level key
class RootConfig(BaseModel):
    model_config = {"extra": "forbid"}
    precleaning_rules: PrecleaningRules
