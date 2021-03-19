"""Misc utility functions.
"""


def list_to_dict(values: list[dict]) -> dict:
    """Convert a list of dicts each containing the "name" field to
    a mapping of the form name -> value.
    """
    return {v["name"]: v for v in values}


def filter_dict_nulls(mapping: dict) -> dict:
    """Return a new dict instance whose values are not None."""
    return {k: v for k, v in mapping.items() if v is not None}


def apply_filter(filter_conf: dict, name: str) -> bool:
    """Given a filter configuration, determine if the name should
    be included or excluded.

    Confer the default_filter function for the structure of a
    filter configuration.

    Returns:
        A flag indicating if the table is ok to be included.
    """
    ok = name in filter_conf["items"]
    if filter_conf["type"] == "exclude":
        ok = not ok
    elif filter_conf["type"] != "include":
        raise ValueError
    return ok


def default_filter() -> dict:
    """Return a no-op filter as default when no table
    or column filter is defined in a configuration.
    """
    return {
        "type": "exclude",
        "items": [],
        "pattern": None,
    }
