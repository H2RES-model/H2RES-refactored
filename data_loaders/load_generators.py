"""Generator loader that merges static and time-series inputs."""

from __future__ import annotations

from typing import Dict, Optional, Literal, cast, Hashable, TypeVar, Tuple

from data_models.SystemSets import SystemSets
from data_models.Generators import Generators
from data_models.Bus import Bus
from data_loaders.helpers.io import TableCache
from data_loaders.helpers.model_factory import build_model
from data_loaders.load_generators_static import load_generators_static
from data_loaders.load_generators_ts import load_generators_ts

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

UPY = Tuple[str, int, int]  # (unit, period, year)


def load_generators(
    powerplants_path: str,
    sets: SystemSets,
    buses: Bus,
    renewable_profiles_path: Optional[str] = None,
    fuel_cost_path: Optional[str] = None,
    efficiency_ts_path: Optional[str] = None,
    existing_generators: Optional[Generators] = None,
    table_cache: Optional[TableCache] = None,
) -> Generators:
    """Load generator parameters and time series into a Generators model.

    When used: called by `load_sector` to populate generator inputs for a sector.

    Args:
        powerplants_path: Path to powerplants (or converters) input file.
        sets: SystemSets with unit lists and time horizon.
        buses: Bus model for validating bus mappings.
        renewable_profiles_path: Optional RES profile time series.
        fuel_cost_path: Optional fuel cost time series.
        efficiency_ts_path: Optional efficiency time series.
        existing_generators: Existing Generators to merge into (existing wins).

    Returns:
        Generators model containing static parameters and time-series data.

    Raises:
        ValueError: If required inputs are missing or inconsistent.
    """

    def merge_no_overwrite(base: Dict[K, V], new: Dict[K, V]) -> Dict[K, V]:
        merged: Dict[K, V] = dict(base)
        for k, v in new.items():
            if k not in merged:
                merged[k] = v
        return merged

    static = load_generators_static(
        powerplants_path=powerplants_path,
        sets=sets,
        buses=buses,
        table_cache=table_cache,
    )

    units = cast(list, static.get("units", []))
    if not units:
        return existing_generators or build_model(
            Generators,
            unit=[],
            system={},
            region={},
            tech={},
            fuel={},
            unit_type={},
            carrier_in={},
            carrier_out={},
            bus_in={},
            bus_out={},
            p_nom={},
            p_nom_max={},
            cap_factor={},
            capital_cost={},
            lifetime={},
            decom_start_existing={},
            decom_start_new={},
            final_cap={},
            efficiency={},
            efficiency_ts={},
            co2_intensity={},
            var_cost_no_fuel={},
            ramping_cost={},
            ramp_up_rate={},
            ramp_down_rate={},
            chp_power_to_heat={},
            chp_power_loss_factor={},
            chp_max_heat={},
            chp_type={},
            bus_out_2={},
            carrier_out_2={},
            p_t={},
            var_cost={},
        )

    ts = load_generators_ts(
        sets=sets,
        units=units,
        renewable_profiles_path=renewable_profiles_path,
        fuel_cost_path=fuel_cost_path,
        efficiency_ts_path=efficiency_ts_path,
        fuel=cast(Dict[str, str], static.get("fuel", {})),
        var_cost_no_fuel=cast(Dict[str, float], static.get("var_cost_no_fuel", {})),
        efficiency=cast(Dict[str, float], static.get("efficiency", {})),
        buses=buses,
        table_cache=table_cache,
    )

    ex = existing_generators

    unit = sorted(set(units).union(set(ex.unit) if ex else set()))

    system = merge_no_overwrite(ex.system if ex else {}, cast(Dict[str, str], static.get("system", {})))
    region = merge_no_overwrite(ex.region if ex else {}, cast(Dict[str, str], static.get("region", {})))
    tech = merge_no_overwrite(ex.tech if ex else {}, cast(Dict[str, str], static.get("tech", {})))
    fuel = merge_no_overwrite(ex.fuel if ex else {}, cast(Dict[str, str], static.get("fuel", {})))
    unit_type = merge_no_overwrite(
        ex.unit_type if ex else {},
        cast(Dict[str, Literal["supply", "conversion"]], static.get("unit_type", {})),
    )

    carrier_out = merge_no_overwrite(
        ex.carrier_out if ex else {},
        cast(Dict[str, str], static.get("carrier_out", {})),
    )
    carrier_in = merge_no_overwrite(
        ex.carrier_in if ex else {},
        cast(Dict[str, Optional[str]], static.get("carrier_in", {})),
    )
    bus_out = merge_no_overwrite(
        ex.bus_out if ex else {},
        cast(Dict[str, str], static.get("bus_out", {})),
    )
    bus_in = merge_no_overwrite(
        ex.bus_in if ex else {},
        cast(Dict[str, Optional[str]], static.get("bus_in", {})),
    )

    bus_out_2 = merge_no_overwrite(
        ex.bus_out_2 if ex else {},
        cast(Dict[str, Optional[str]], static.get("bus_out_2", {})),
    )
    carrier_out_2 = merge_no_overwrite(
        ex.carrier_out_2 if ex else {},
        cast(Dict[str, Optional[str]], static.get("carrier_out_2", {})),
    )

    p_nom = merge_no_overwrite(ex.p_nom if ex else {}, cast(Dict[str, float], static.get("p_nom", {})))
    p_nom_max = merge_no_overwrite(
        ex.p_nom_max if ex else {},
        cast(Dict[str, float], static.get("p_nom_max", {})),
    )
    cap_factor = merge_no_overwrite(
        ex.cap_factor if ex else {},
        cast(Dict[str, float], static.get("cap_factor", {})),
    )
    capital_cost = merge_no_overwrite(
        ex.capital_cost if ex else {},
        cast(Dict[str, float], static.get("capital_cost", {})),
    )
    ramping_cost = merge_no_overwrite(
        ex.ramping_cost if ex else {},
        cast(Dict[str, float], static.get("ramping_cost", {})),
    )
    ramp_up_rate = merge_no_overwrite(
        ex.ramp_up_rate if ex else {},
        cast(Dict[str, float], static.get("ramp_up_rate", {})),
    )
    ramp_down_rate = merge_no_overwrite(
        ex.ramp_down_rate if ex else {},
        cast(Dict[str, float], static.get("ramp_down_rate", {})),
    )
    co2_intensity = merge_no_overwrite(
        ex.co2_intensity if ex else {},
        cast(Dict[str, float], static.get("co2_intensity", {})),
    )
    decom_start_existing = merge_no_overwrite(
        ex.decom_start_existing if ex else {},
        cast(Dict[str, int], static.get("decom_start_existing", {})),
    )
    decom_start_new = merge_no_overwrite(
        ex.decom_start_new if ex else {},
        cast(Dict[str, int], static.get("decom_start_new", {})),
    )
    final_cap = merge_no_overwrite(
        ex.final_cap if ex else {},
        cast(Dict[str, float], static.get("final_cap", {})),
    )
    lifetime = merge_no_overwrite(
        ex.lifetime if ex else {},
        cast(Dict[str, int], static.get("lifetime", {})),
    )
    var_cost_no_fuel = merge_no_overwrite(
        ex.var_cost_no_fuel if ex else {},
        cast(Dict[str, float], static.get("var_cost_no_fuel", {})),
    )
    efficiency = merge_no_overwrite(
        ex.efficiency if ex else {},
        cast(Dict[str, float], static.get("efficiency", {})),
    )

    chp_power_to_heat = merge_no_overwrite(
        ex.chp_power_to_heat if ex else {},
        cast(Dict[str, float], static.get("chp_power_to_heat", {})),
    )
    chp_power_loss_factor = merge_no_overwrite(
        ex.chp_power_loss_factor if ex else {},
        cast(Dict[str, float], static.get("chp_power_loss_factor", {})),
    )
    chp_max_heat = merge_no_overwrite(
        ex.chp_max_heat if ex else {},
        cast(Dict[str, float], static.get("chp_max_heat", {})),
    )
    chp_type = merge_no_overwrite(
        ex.chp_type if ex else {},
        cast(Dict[str, str], static.get("chp_type", {})),
    )

    p_t = merge_no_overwrite(ex.p_t if ex else {}, cast(Dict[UPY, float], ts.get("p_t", {})))
    var_cost = merge_no_overwrite(ex.var_cost if ex else {}, cast(Dict[UPY, float], ts.get("var_cost", {})))
    efficiency_ts = merge_no_overwrite(
        ex.efficiency_ts if ex else {},
        cast(Dict[UPY, float], ts.get("efficiency_ts", {})),
    )

    return build_model(
        Generators,
        unit=unit,
        system=system,
        region=region,
        tech=tech,
        fuel=fuel,
        unit_type=unit_type,
        carrier_out=carrier_out,
        carrier_in=carrier_in,
        bus_out=bus_out,
        bus_in=bus_in,
        p_nom=p_nom,
        p_nom_max=p_nom_max,
        cap_factor=cap_factor,
        capital_cost=capital_cost,
        ramping_cost=ramping_cost,
        ramp_up_rate=ramp_up_rate,
        ramp_down_rate=ramp_down_rate,
        co2_intensity=co2_intensity,
        decom_start_existing=decom_start_existing,
        decom_start_new=decom_start_new,
        final_cap=final_cap,
        lifetime=lifetime,
        var_cost_no_fuel=var_cost_no_fuel,
        efficiency=efficiency,
        chp_power_to_heat=chp_power_to_heat,
        chp_power_loss_factor=chp_power_loss_factor,
        chp_max_heat=chp_max_heat,
        chp_type=chp_type,
        bus_out_2=bus_out_2,
        carrier_out_2=carrier_out_2,
        efficiency_ts=efficiency_ts,
        p_t=p_t,
        var_cost=var_cost,
    )
