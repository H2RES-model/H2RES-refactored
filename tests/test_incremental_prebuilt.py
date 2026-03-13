from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from data_loaders.prebuilt_system import (
    PREBUILT_SYSTEM_FORMAT_VERSION,
    analyze_prebuilt_refresh,
    load_or_build_prebuilt_system,
    save_prebuilt_system,
)
from data_models.Bus import Bus
from data_models.Demand import Demand
from data_models.Generators import Generators
from data_models.StorageUnits import StorageUnits
from data_models.SystemParameters import MarketParams, PolicyParams, SystemParameters
from data_models.SystemSets import SystemSets
from data_models.Transport import Transport


def _minimal_system() -> SystemParameters:
    return SystemParameters(
        sets=SystemSets(years=[2020], periods=[1], carriers=[], buses=[]),
        buses=Bus(),
        generators=Generators(),
        storage_units=StorageUnits(),
        demands=Demand(),
        transport_units=Transport(),
        market=MarketParams(),
        policy=PolicyParams(),
    )


class IncrementalPrebuiltTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path.cwd() / ".tmp_tests"
        root.mkdir(exist_ok=True)
        self.temp_dir = root / f"incremental-prebuilt-{uuid.uuid4().hex}"
        self.temp_dir.mkdir(parents=True, exist_ok=False)
        self.prebuilt_dir = self.temp_dir / "prebuilt"
        self.sources = {
            "buses": self.temp_dir / "buses.csv",
            "fuel": self.temp_dir / "fuel_cost.csv",
            "fuels": self.temp_dir / "fuels_converters.csv",
            "heating": self.temp_dir / "heating_demand.csv",
            "transport": self.temp_dir / "transport.xlsx",
        }
        for path in self.sources.values():
            path.write_text(f"{path.name}\n", encoding="utf-8")
        self.source_paths = list(self.sources.values())
        self.source_scopes = {
            self.sources["buses"]: "shared:buses",
            self.sources["fuel"]: "shared:fuel_cost",
            self.sources["fuels"]: "shared:fuels_converters",
            self.sources["heating"]: "sector:heating",
            self.sources["transport"]: "transport",
        }
        save_prebuilt_system(
            _minimal_system(),
            self.prebuilt_dir,
            overwrite=True,
            source_paths=self.source_paths,
            source_scopes=self.source_scopes,
            validation_mode="fast",
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_analyze_no_changes_returns_none(self) -> None:
        analysis = analyze_prebuilt_refresh(
            self.prebuilt_dir,
            source_paths=self.source_paths,
            source_scopes=self.source_scopes,
        )
        self.assertEqual(analysis.scope, "none")
        self.assertEqual(analysis.sectors, ())

    def test_analyze_heating_change_returns_sector_scope(self) -> None:
        self.sources["heating"].write_text("changed\n", encoding="utf-8")
        analysis = analyze_prebuilt_refresh(
            self.prebuilt_dir,
            source_paths=self.source_paths,
            source_scopes=self.source_scopes,
        )
        self.assertEqual(analysis.scope, "sector:heating")
        self.assertEqual(analysis.sectors, ("heating",))

    def test_analyze_transport_change_returns_electricity_transport_scope(self) -> None:
        self.sources["transport"].write_text("changed\n", encoding="utf-8")
        analysis = analyze_prebuilt_refresh(
            self.prebuilt_dir,
            source_paths=self.source_paths,
            source_scopes=self.source_scopes,
        )
        self.assertEqual(analysis.scope, "electricity+transport")
        self.assertEqual(analysis.sectors, ("electricity",))

    def test_analyze_shared_change_forces_full_rebuild(self) -> None:
        self.sources["buses"].write_text("changed\n", encoding="utf-8")
        analysis = analyze_prebuilt_refresh(
            self.prebuilt_dir,
            source_paths=self.source_paths,
            source_scopes=self.source_scopes,
        )
        self.assertEqual(analysis.scope, "full")
        self.assertFalse(analysis.supports_incremental)

    def test_analyze_fuels_change_forces_full_rebuild(self) -> None:
        self.sources["fuels"].write_text("changed\n", encoding="utf-8")
        analysis = analyze_prebuilt_refresh(
            self.prebuilt_dir,
            source_paths=self.source_paths,
            source_scopes=self.source_scopes,
        )
        self.assertEqual(analysis.scope, "full")
        self.assertFalse(analysis.supports_incremental)

    def test_analyze_stale_legacy_manifest_forces_full_rebuild(self) -> None:
        manifest_path = self.prebuilt_dir / "manifest.json"
        text = manifest_path.read_text(encoding="utf-8")
        manifest_path.write_text(text.replace(f'"format_version": {PREBUILT_SYSTEM_FORMAT_VERSION}', '"format_version": 3'), encoding="utf-8")
        self.sources["heating"].write_text("changed\n", encoding="utf-8")
        analysis = analyze_prebuilt_refresh(
            self.prebuilt_dir,
            source_paths=self.source_paths,
            source_scopes=self.source_scopes,
        )
        self.assertEqual(analysis.scope, "full")
        self.assertFalse(analysis.supports_incremental)

    def test_load_or_build_uses_incremental_builder_for_sector_change(self) -> None:
        self.sources["heating"].write_text("changed\n", encoding="utf-8")
        calls: list[tuple[str, tuple[str, ...]]] = []

        def build_full() -> SystemParameters:
            calls.append(("full", ()))
            return _minimal_system()

        def build_incremental(existing: SystemParameters, sectors: tuple[str, ...]) -> SystemParameters:
            calls.append(("incremental", tuple(sectors)))
            return existing

        load_or_build_prebuilt_system(
            path=self.prebuilt_dir,
            build_fn=build_full,
            incremental_build_fn=build_incremental,
            source_paths=self.source_paths,
            source_scopes=self.source_scopes,
            overwrite_if_stale=False,
        )
        self.assertEqual(calls, [("incremental", ("heating",))])


if __name__ == "__main__":
    unittest.main()
