from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from data_loaders.load_system import load_system_from_config


POWERPLANTS_HEADER = (
    "system,region,name,tech,fuel,unit_type,carrier_in,carrier_out,bus_in,bus_out,"
    "p_nom,p_nom_max,cap_factor,capital_cost,lifetime,decom_start_existing,decom_start_new,"
    "final_cap,efficiency,co2_intensity,var_cost_no_fuel,ramp_up_rate,ramp_down_rate,"
    "ramping_cost,e_nom,p_charge_nom,standby_loss,efficiency_charge,efficiency_discharge,"
    "chp_power_to_heat,chp_power_loss_factor,chp_max_heat,chp_type,carrier_out_2,bus_out_2\n"
)


class FuelsLoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd() / ".tmp_tests" / f"fuels-loader-{uuid.uuid4().hex}"
        self.data_dir = self.root / "data"
        for rel in (
            "shared",
            "electricity",
            "heating",
            "fuels",
        ):
            (self.data_dir / rel).mkdir(parents=True, exist_ok=True)

        (self.data_dir / "shared" / "buses.csv").write_text(
            "\n".join(
                [
                    "system,region,bus,carrier",
                    "HR,HR,HR_EL,electricity",
                    "HR,HR,HR_LOAD,electricity",
                    "HR,HR,HR_HEAT,heat",
                    "HR,HR,HR_H2,hydrogen",
                    "HR,HR,HR_H2_DEMAND,hydrogen",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (self.data_dir / "shared" / "fuel_cost.csv").write_text(
            "\n".join(
                [
                    "year,period,electricity,hydrogen",
                    "2020,1,10,50",
                    "2020,2,10,50",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (self.data_dir / "electricity" / "powerplants.csv").write_text(
            POWERPLANTS_HEADER
            + "HR,HR,GridSupply,GridSupply,electricity,supply,,electricity,,HR_EL,10,100,1,1000,20,0,0,0,1,0,5,1,1,0,,,,,,,,,,\n",
            encoding="utf-8",
        )
        (self.data_dir / "electricity" / "storage_units.csv").write_text(
            "\n".join(
                [
                    "system,region,name,tech,carrier_in,carrier_out,bus_in,bus_out,e_nom,e_nom_max,e_min,duration_charge,duration_discharge,efficiency_charge,efficiency_discharge,standby_loss,capital_cost_energy,capital_cost_power_charge,capital_cost_power_discharge,lifetime,spillage_cost",
                    "HR,HR,Battery_1,battery,electricity,electricity,HR_EL,HR_EL,5,10,0,2,2,0.95,0.95,0.001,10,20,20,15,0",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (self.data_dir / "electricity" / "electricity_demand.csv").write_text(
            "\n".join(["year,period,HR_LOAD", "2020,1,5", "2020,2,5"]) + "\n",
            encoding="utf-8",
        )
        (self.data_dir / "electricity" / "scaled_inflows.csv").write_text(
            "\n".join(["year,period", "2020,1", "2020,2"]) + "\n",
            encoding="utf-8",
        )
        (self.data_dir / "heating" / "converters.csv").write_text(
            POWERPLANTS_HEADER
            + "HR,HR,HeatPump,HeatPump,electricity,conversion,electricity,heat,HR_EL,HR_HEAT,5,50,1,2000,20,0,0,0,3.0,0,1,1,1,0,,,,,,,,,,\n",
            encoding="utf-8",
        )
        (self.data_dir / "heating" / "storage_units.csv").write_text(
            "system,region,name,tech,carrier_in,carrier_out,bus_in,bus_out,e_nom,e_nom_max,e_min,duration_charge,duration_discharge,efficiency_charge,efficiency_discharge,standby_loss,capital_cost_energy,capital_cost_power_charge,capital_cost_power_discharge,lifetime,spillage_cost\n",
            encoding="utf-8",
        )
        (self.data_dir / "heating" / "heat_demand.csv").write_text(
            "\n".join(["year,period,HR_HEAT", "2020,1,4", "2020,2,4"]) + "\n",
            encoding="utf-8",
        )
        (self.data_dir / "fuels" / "converters.csv").write_text(
            POWERPLANTS_HEADER
            + "HR,HR,PEM_elec,PEM_elec,electricity,conversion,electricity,hydrogen,HR_EL,HR_H2,10,100,1,1000,15,0,0,0,0.64,0,0,1,1,0,,,,,,,,,,\n",
            encoding="utf-8",
        )
        (self.data_dir / "fuels" / "storage_units.csv").write_text(
            "\n".join(
                [
                    "system,region,name,tech,carrier_in,carrier_out,bus_in,bus_out,e_nom,e_nom_max,e_min,duration_charge,duration_discharge,efficiency_charge,efficiency_discharge,standby_loss,capital_cost_energy,capital_cost_power_charge,capital_cost_power_discharge,lifetime,spillage_cost",
                    "HR,HR,H2_storage_tank,H2_storage_tank,hydrogen,hydrogen,HR_H2,HR_H2,10,100,0,8,8,0.7,0.7,0.001,20,150,50,15,0",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (self.data_dir / "fuels" / "demand.csv").write_text(
            "\n".join(["year,period,HR_H2_DEMAND", "2020,1,1", "2020,2,2"]) + "\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _config(self, sectors: list[str]) -> dict:
        return {
            "sectors": sectors,
            "shared": {
                "data_dir": str(self.data_dir),
                "buses_path": str(self.data_dir / "shared" / "buses.csv"),
                "fuel_cost_path": str(self.data_dir / "shared" / "fuel_cost.csv"),
                "use_prebuilt": False,
            },
            "electricity": {
                "powerplants_path": str(self.data_dir / "electricity" / "powerplants.csv"),
                "storage_path": str(self.data_dir / "electricity" / "storage_units.csv"),
                "electricity_demand_path": str(self.data_dir / "electricity" / "electricity_demand.csv"),
                "renewable_profiles_path": None,
                "inflow_path": str(self.data_dir / "electricity" / "scaled_inflows.csv"),
            },
            "heating": {
                "powerplants_path": str(self.data_dir / "heating" / "converters.csv"),
                "storage_path": str(self.data_dir / "heating" / "storage_units.csv"),
                "efficiency_ts_path": None,
                "heating_demand_path": str(self.data_dir / "heating" / "heat_demand.csv"),
            },
            "fuels": {
                "enabled": True,
                "converters_path": str(self.data_dir / "fuels" / "converters.csv"),
                "storage_path": str(self.data_dir / "fuels" / "storage_units.csv"),
                "demand_path": str(self.data_dir / "fuels" / "demand.csv"),
                "demand_carrier": "hydrogen",
            },
        }

    def test_build_with_electricity_and_fuels_attaches_fuels_assets(self) -> None:
        system = load_system_from_config(self._config(["electricity"]))
        self.assertIn("PEM_elec", system.generators.static.index.astype(str).tolist())
        self.assertIn("H2_storage_tank", system.storage_units.static.index.astype(str).tolist())
        hydrogen_rows = system.demands.p_t[system.demands.p_t["carrier"] == "hydrogen"]
        self.assertEqual(sorted(hydrogen_rows["bus"].astype(str).unique().tolist()), ["HR_H2_DEMAND"])

    def test_build_with_electricity_and_heating_keeps_fuels_attached(self) -> None:
        system = load_system_from_config(self._config(["electricity", "heating"]))
        self.assertIn("PEM_elec", system.generators.static.index.astype(str).tolist())
        self.assertIn("HeatPump", system.generators.static.index.astype(str).tolist())
        self.assertIn("H2_storage_tank", system.storage_units.static.index.astype(str).tolist())

    def test_duplicate_names_between_sector_and_fuels_fail(self) -> None:
        (self.data_dir / "fuels" / "converters.csv").write_text(
            POWERPLANTS_HEADER
            + "HR,HR,GridSupply,PEM_elec,electricity,conversion,electricity,hydrogen,HR_EL,HR_H2,10,100,1,1000,15,0,0,0,0.64,0,0,1,1,0,,,,,,,,,,\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "Duplicate name values"):
            load_system_from_config(self._config(["electricity"]))


if __name__ == "__main__":
    unittest.main()
