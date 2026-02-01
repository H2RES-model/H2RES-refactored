from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Tuple, Union, get_args, get_origin
import sys

from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from data_models.Bus import Bus
from data_models.SystemSets import SystemSets
from data_models.Generators import Generators
from data_models.StorageUnits import StorageUnits
from data_models.Demand import Demand
from data_models.SystemParameters import SystemParameters

OUTPUT_DIR = ROOT / "docs" / "components" / "_tables"

COMPONENTS: Dict[str, type[BaseModel]] = {
    "buses": Bus,
    "system_sets": SystemSets,
    "generators": Generators,
    "storage_units": StorageUnits,
    "demand": Demand,
    "system_parameters": SystemParameters,
}


def format_type(tp: Any) -> str:
    origin = get_origin(tp)
    if origin is None:
        if hasattr(tp, "__name__"):
            return tp.__name__
        return str(tp)
    args = get_args(tp)
    if origin is list or origin is Iterable:
        return f"List[{format_type(args[0])}]" if args else "List"
    if origin is dict:
        if len(args) == 2:
            return f"Dict[{format_type(args[0])}, {format_type(args[1])}]"
        return "Dict"
    if origin is tuple or origin is Tuple:
        return "Tuple[" + ", ".join(format_type(a) for a in args) + "]"
    if origin is Union:
        return "Union[" + ", ".join(format_type(a) for a in args) + "]"
    return str(tp)


def format_default(field: Any) -> str:
    if field.is_required():
        return "required"
    if field.default_factory is not None:
        return "default_factory"
    if field.default is None:
        return "None"
    return repr(field.default)


def get_extra(field: Any, key: str, fallback: str) -> str:
    extra = field.json_schema_extra or {}
    value = extra.get(key)
    if value is None or value == "":
        return fallback
    return str(value)


def render_table(model: type[BaseModel]) -> str:
    lines = []
    lines.append("| Attribute | Type | Unit | Description | Status |")
    lines.append("| --- | --- | --- | --- | --- |")

    for name, field in model.model_fields.items():
        annotation = field.annotation
        type_str = format_type(annotation)
        unit = get_extra(field, "unit", "n.a.")
        status_default = "mandatory" if field.is_required() else "optional"
        status = get_extra(field, "status", status_default)
        desc = field.description or ""
        lines.append(f"| `{name}` | {type_str} | {unit} | {desc} | {status} |")

    return "\n".join(lines) + "\n"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, model in COMPONENTS.items():
        table = render_table(model)
        out_path = OUTPUT_DIR / f"{name}.md"
        out_path.write_text(table, encoding="utf-8")


if __name__ == "__main__":
    main()
