import { ChartMetadata, ChartPlugin, t } from "@superset-ui/core";
import controlPanel from "./controlPanel";
import transformProps from "./transformProps";
import buildQuery from "./buildQuery";

export default class Echarts6GanttChartPlugin extends ChartPlugin {
  constructor() {
    super({
      metadata: new ChartMetadata({
        name: t("ECharts6 Gantt"),
        description: t("Custom Gantt-like chart using ECharts 6 custom series."),
        thumbnail: "",
        tags: [t("ECharts"), t("Gantt"), t("Time-series")]
      }),
      controlPanel,
      buildQuery,
      loadChart: () => import("./Echarts6Gantt"),
      transformProps
    });
  }
}
