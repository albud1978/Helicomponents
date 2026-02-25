#!/usr/bin/env python3
import argparse
import re
from pathlib import Path


IMPORT_LINE = "import Echarts6GanttChartPlugin from './Echarts6GanttChartPlugin';"
REGISTER_LINE_REGISTER_CHARTS = "      new Echarts6GanttChartPlugin().configure({ key: 'echarts6_gantt' }),"
REGISTER_LINE_PLUGINS = "        new Echarts6GanttChartPlugin().configure({ key: 'echarts6_gantt' }),"


def resolve_main_preset(frontend_dir: Path) -> Path:
    candidates = [
        frontend_dir / "src/visualizations/presets/MainPreset.js",
        frontend_dir / "src/visualizations/presets/MainPreset.ts"
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("MainPreset file was not found in superset-frontend/src/visualizations/presets")


def ensure_wrapper(frontend_dir: Path) -> None:
    wrapper = frontend_dir / "src/visualizations/presets/Echarts6GanttChartPlugin.ts"
    content = (
        "import Echarts6GanttChartPlugin from '../../../plugins/plugin-chart-echarts6-gantt/src';\n\n"
        "export default Echarts6GanttChartPlugin;\n"
    )
    wrapper.write_text(content, encoding="utf-8")


def patch_main_preset(main_preset: Path) -> None:
    text = main_preset.read_text(encoding="utf-8")

    if IMPORT_LINE not in text:
        imports = list(re.finditer(r"^import .+;$", text, flags=re.MULTILINE))
        if not imports:
            raise RuntimeError("Could not find import block in MainPreset")
        insert_at = imports[-1].end()
        text = text[:insert_at] + "\n" + IMPORT_LINE + text[insert_at:]

    if (
        REGISTER_LINE_REGISTER_CHARTS not in text
        and REGISTER_LINE_PLUGINS not in text
    ):
        marker = "plugins: ["
        start = text.find(marker)
        if start != -1:
            close = text.find("\n      ],", start)
            if close == -1:
                raise RuntimeError("Could not locate end of plugins array")
            text = text[:close] + REGISTER_LINE_PLUGINS + "\n" + text[close:]
        else:
            marker = "this.registerCharts(["
            start = text.find(marker)
            if start == -1:
                raise RuntimeError("Could not find plugin registration block in MainPreset")
            close = text.find("]);", start)
            if close == -1:
                raise RuntimeError("Could not locate end of this.registerCharts block")
            text = text[:close] + REGISTER_LINE_REGISTER_CHARTS + "\n" + text[close:]

    main_preset.write_text(text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Register local ECharts6 Gantt plugin in Superset frontend")
    parser.add_argument("--superset-src", required=True, help="Path to apache/superset source root")
    args = parser.parse_args()

    superset_src = Path(args.superset_src).resolve()
    frontend_dir = superset_src / "superset-frontend"
    if not frontend_dir.exists():
        raise FileNotFoundError(f"superset-frontend not found in {superset_src}")

    main_preset = resolve_main_preset(frontend_dir)
    ensure_wrapper(frontend_dir)
    patch_main_preset(main_preset)
    print(f"Patched: {main_preset}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
