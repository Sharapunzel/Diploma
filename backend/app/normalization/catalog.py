"""Public, stable block capabilities for the normalizer editor."""

BLOCK_TYPES = [
    {
        "kind": "regex", "description": "RE2 named-capture extraction",
        "parameters": ["key", "input", "candidates[].pattern", "candidates[].set"],
        "input": "log or preceding extractor field", "output": "named captures, match, constants",
        "limits": {"candidates": 8, "pattern_characters": 1024, "engine": "RE2"},
    },
    {
        "kind": "template", "description": "Literal template with %{name} placeholders",
        "parameters": ["key", "input", "candidates[].template", "candidates[].set"],
        "input": "log or preceding extractor field", "output": "placeholders, match, constants",
        "limits": {"candidates": 8},
    },
    {
        "kind": "columns", "description": "Ordered delimited columns",
        "parameters": ["key", "input", "candidates[].delimiter", "candidates[].columns", "candidates[].set"],
        "input": "log or preceding extractor field", "output": "column names, constants",
        "limits": {"candidates": 8, "columns": 64},
    },
    {
        "kind": "json", "description": "JSON object extraction by dotted paths",
        "parameters": ["key", "input", "candidates[].requires", "candidates[].extract", "candidates[].set"],
        "input": "log or preceding extractor field", "output": "extract keys, constants",
        "limits": {"candidates": 8, "path_depth": 8},
    },
    {
        "kind": "map_ecs", "description": "Typed mapping into one ECS field",
        "parameters": ["key", "target", "source", "required", "transform"],
        "input": "reference, constant, joined references or array", "output": "ECS field",
        "limits": {"one_target_per_block": True},
    },
]
