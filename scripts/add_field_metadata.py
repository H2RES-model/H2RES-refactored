from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

ROOT = Path(__file__).resolve().parents[1]

FIELDS: Dict[str, Dict[str, Tuple[str, str]]] = {
    "data_models/Generators.py": {
        "unit": ("n.a.", "mandatory"),
        "system": ("n.a.", "optional"),
        "region": ("n.a.", "optional"),
        "tech": ("n.a.", "mandatory"),
        "fuel": ("n.a.", "mandatory"),
        "unit_type": ("n.a.", "optional"),
        "carrier_in": ("n.a.", "optional"),
        "carrier_out": ("n.a.", "optional"),
        "bus_in": ("n.a.", "optional"),
        "bus_out": ("n.a.", "optional"),
        "p_nom": ("MW", "mandatory"),
        "p_nom_max": ("MW", "optional"),
        "cap_factor": ("p.u.", "mandatory"),
        "capital_cost": ("EUR/MW", "mandatory"),
        "lifetime": ("years", "mandatory"),
        "decom_start_existing": ("year", "mandatory"),
        "decom_start_new": ("year", "mandatory"),
        "final_cap": ("MW", "mandatory"),
        "efficiency": ("p.u.", "mandatory"),
        "efficiency_ts": ("p.u.", "optional"),
        "co2_intensity": ("tCO2/MWh_output", "mandatory"),
        "var_cost_no_fuel": ("EUR/MWh_output", "mandatory"),
        "ramp_up_rate": ("MW/period or p.u.", "mandatory"),
        "ramp_down_rate": ("MW/period or p.u.", "mandatory"),
        "ramping_cost": ("n.a.", "optional"),
        "chp_power_to_heat": ("p.u.", "optional"),
        "chp_power_loss_factor": ("p.u.", "optional"),
        "chp_max_heat": ("MW_heat", "optional"),
        "chp_type": ("n.a.", "optional"),
        "bus_out_2": ("n.a.", "optional"),
        "carrier_out_2": ("n.a.", "optional"),
        "p_t": ("p.u.", "optional"),
        "var_cost": ("EUR/MWh_output", "optional"),
    },
    "data_models/StorageUnits.py": {
        "unit": ("n.a.", "optional"),
        "system": ("n.a.", "optional"),
        "region": ("n.a.", "optional"),
        "tech": ("n.a.", "optional"),
        "carrier_in": ("n.a.", "optional"),
        "carrier_out": ("n.a.", "optional"),
        "bus_in": ("n.a.", "optional"),
        "bus_out": ("n.a.", "optional"),
        "e_nom": ("MWh", "optional"),
        "e_min": ("MWh", "optional"),
        "e_nom_max": ("MWh", "optional"),
        "p_charge_nom": ("MW", "optional"),
        "p_charge_nom_max": ("MW", "optional"),
        "p_discharge_nom": ("MW", "optional"),
        "p_discharge_nom_max": ("MW", "optional"),
        "duration_charge": ("hours", "optional"),
        "duration_discharge": ("hours", "optional"),
        "efficiency_charge": ("p.u.", "optional"),
        "efficiency_discharge": ("p.u.", "optional"),
        "standby_loss": ("p.u.", "optional"),
        "capital_cost_energy": ("EUR/MWh", "optional"),
        "capital_cost_power_charge": ("EUR/MW", "optional"),
        "capital_cost_power_discharge": ("EUR/MW", "optional"),
        "lifetime": ("years", "optional"),
        "inflow": ("MWh/period", "optional"),
        "spillage_cost": ("EUR/MWh", "optional"),
        "e_nom_inv_cost": ("EUR/MWh", "optional"),
    },
}


def inject_metadata(path: Path, field_meta: Dict[str, Tuple[str, str]]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    out = []
    i = 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        if line.startswith("    ") and ":" in line and "= Field(" in line:
            name = line.strip().split(":", 1)[0]
            if name in field_meta:
                unit, status = field_meta[name]
                # Find the end of this Field(...) block at the same indent level.
                j = i + 1
                while j < len(lines):
                    if lines[j].startswith("    )"):
                        # Insert json_schema_extra just before closing
                        out.insert(len(out), f"        json_schema_extra={{\"unit\": \"{unit}\", \"status\": \"{status}\"}},")
                        break
                    if "json_schema_extra" in lines[j]:
                        break
                    j += 1
        i += 1
    path.write_text("\n".join(out) + "\n", encoding="utf-8")


def main() -> None:
    for rel, meta in FIELDS.items():
        path = ROOT / rel
        inject_metadata(path, meta)


if __name__ == "__main__":
    main()
