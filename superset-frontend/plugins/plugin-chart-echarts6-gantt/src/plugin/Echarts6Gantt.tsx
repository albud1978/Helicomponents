import { useEffect, useMemo, useRef } from "react";
import * as echarts from "echarts";
import { Echarts6GanttTransformedProps } from "./types";

export default function Echarts6Gantt(
  props: Echarts6GanttTransformedProps
): JSX.Element {
  const { width, height, data, metricLabel } = props;
  const rootRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<echarts.ECharts | null>(null);

  const categories = useMemo(
    () =>
      Array.from(new Set(data.map(point => point.category))).sort((a, b) =>
        a.localeCompare(b, "en", { numeric: true })
      ),
    [data]
  );

  const xExtent = useMemo(() => {
    if (!data.length) {
      return { min: undefined, max: undefined };
    }
    const day0Starts = data
      .filter(point => point.dayIndexStart === 0)
      .map(point => point.startTs);
    const min = day0Starts.length
      ? Math.min(...day0Starts)
      : Math.min(...data.map(point => point.startTs));
    const max = Math.max(...data.map(point => point.endTs));
    return { min, max };
  }, [data]);

  const dominantGroupByLabel = useMemo(() => {
    const counters = new Map<string, { g1: number; g2: number }>();
    for (const point of data) {
      const label = point.label;
      if (!counters.has(label)) {
        counters.set(label, { g1: 0, g2: 0 });
      }
      const row = counters.get(label)!;
      if (point.groupBy === 2) {
        row.g2 += 1;
      } else if (point.groupBy === 1) {
        row.g1 += 1;
      }
    }
    const result = new Map<string, number>();
    for (const [label, c] of counters.entries()) {
      result.set(label, c.g2 >= c.g1 ? 2 : 1);
    }
    return result;
  }, [data]);

  useEffect(() => {
    if (!rootRef.current) return undefined;

    if (!chartRef.current) {
      chartRef.current = echarts.init(rootRef.current, undefined, {
        renderer: "canvas"
      });
    }

    const seriesData = data.map(point => {
      const categoryIndex = categories.indexOf(point.category);
      const stableGroup = dominantGroupByLabel.get(point.label) ?? point.groupBy ?? 1;
      const color = stableGroup === 2 ? "#22c55e" : "#facc15";
      return {
        value: [
          categoryIndex,
          point.startTs,
          point.endTs,
          point.value,
          point.label,
          point.category,
          stableGroup
        ],
        itemStyle: {
          color
        }
      };
    });

    const option: echarts.EChartsOption = {
      animation: true,
      grid: { top: 24, right: 28, bottom: 84, left: 96 },
      tooltip: {
        trigger: "item",
        axisPointer: { type: "cross" },
        formatter: (params: any) => {
          const payload = Array.isArray(params) ? params[0] : params;
          const value = Array.isArray(payload?.value) ? payload.value : [];
          const fmt = (x: unknown) => {
            const d = new Date(Number(x));
            return Number.isFinite(d.getTime()) ? d.toISOString().slice(0, 10) : "-";
          };
          return [
            `Линия: ${value[5] ?? "-"}`,
            `Борт: ${value[4] ?? "-"}`,
            `Группа: ${value[6] ?? "-"}`,
            `${metricLabel}: ${value[3] ?? "-"}`,
            `${fmt(value[1])} -> ${fmt(value[2])}`
          ].join("<br/>");
        }
      },
      xAxis: {
        type: "time",
        min: xExtent.min,
        max: xExtent.max,
        splitLine: { show: true, lineStyle: { color: "#e2e8f0" } },
        axisLabel: {
          formatter: (value: number) => {
            const d = new Date(value);
            return Number.isFinite(d.getTime()) ? d.toISOString().slice(0, 10) : "";
          }
        }
      },
      yAxis: {
        type: "category",
        data: categories,
        axisLabel: { interval: 0 },
        splitLine: { show: true, lineStyle: { color: "#f1f5f9" } }
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "weakFilter",
          moveOnMouseMove: true,
          moveOnMouseWheel: true,
          zoomOnMouseWheel: "shift"
        },
        {
          type: "slider",
          xAxisIndex: 0,
          height: 18,
          bottom: 28,
          filterMode: "weakFilter",
          showDataShadow: false,
          brushSelect: false
        },
        {
          type: "inside",
          yAxisIndex: 0,
          filterMode: "weakFilter",
          moveOnMouseMove: true,
          moveOnMouseWheel: true,
          zoomOnMouseWheel: false
        },
        {
          type: "slider",
          yAxisIndex: 0,
          width: 10,
          right: 6,
          filterMode: "weakFilter",
          showDataShadow: false,
          brushSelect: false
        }
      ],
      series: [
        {
          type: "custom",
          name: "Gantt",
          progressive: 0,
          renderItem: (params: any, api: any) => {
            const categoryIndex = Number(api.value(0));
            const start = api.coord([api.value(1), categoryIndex]);
            const end = api.coord([api.value(2), categoryIndex]);
            const size = api.size([0, 1]);
            const h = (Array.isArray(size) ? size[1] : Number(size || 0)) * 0.64;
            const coord = params.coordSys || {};
            const baseStyle = api.style();

            const rect = echarts.graphic.clipRectByRect(
              {
                x: start[0],
                y: start[1] - h / 2,
                width: Math.max(1, end[0] - start[0]),
                height: h
              },
              {
                x: Number(coord.x || 0),
                y: Number(coord.y || 0),
                width: Number(coord.width || 0),
                height: Number(coord.height || 0)
              }
            );

            if (!rect) {
              return null;
            }

            const barText = String(api.value(4) ?? "");
            const showText = rect.width >= 26;
            return {
              type: "group",
              children: [
                {
                  type: "rect",
                  shape: rect,
                  style: {
                    ...baseStyle,
                    stroke: "#1f2937",
                    lineWidth: 0.6,
                    opacity: 0.92
                  },
                  emphasis: {
                    style: {
                      opacity: 1,
                      lineWidth: 1
                    }
                  }
                },
                {
                  type: "text",
                  style: {
                    x: rect.x + 4,
                    y: rect.y + rect.height / 2,
                    text: showText ? barText : "",
                    verticalAlign: "middle",
                    textAlign: "left",
                    fill: "#ffffff",
                    fontWeight: 700,
                    fontSize: Math.max(10, Math.min(12, rect.height - 1)),
                    width: Math.max(rect.width - 8, 0),
                    overflow: "truncate"
                  },
                  silent: true
                }
              ]
            };
          },
          encode: { x: [1, 2], y: 0, tooltip: [5, 4, 6, 3, 1, 2] },
          markLine: {
            symbol: "none",
            label: { formatter: "Сегодня", position: "insideEndTop" },
            lineStyle: { type: "dashed", color: "#475569", width: 1 },
            data: [{ xAxis: Date.now() }]
          },
          data: seriesData
        }
      ]
    };

    chartRef.current.setOption(option, true);
    chartRef.current.resize({ width, height });

    return () => {
      if (chartRef.current) {
        chartRef.current.dispose();
        chartRef.current = null;
      }
    };
  }, [categories, data, dominantGroupByLabel, height, metricLabel, width]);

  return <div ref={rootRef} style={{ width, height }} />;
}
