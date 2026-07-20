import { useEffect, useMemo, useRef } from "react";
import * as echarts from "echarts";
import { Echarts6GanttTransformedProps } from "./types";

const DAY_MS = 24 * 60 * 60 * 1000;
const LONG_FREE_WINDOW_DAYS = 180;
const COLOR_MI8 = "#003594";
const COLOR_MI17 = "#7CA3DC";
const COLOR_SLOT_BORDER = "#64748b";
const COLOR_AXIS_TEXT = "#111820";

type AircraftSeriesPoint = {
  // 0 catIdx, 1 start, 2 end, 3 barText, 4 category, 5 group, 6 overdue,
  // 7 board, 8 days, 9 status, 10 description
  value: [
    number,
    number,
    number,
    string,
    string,
    number,
    number,
    string,
    number,
    string,
    string
  ];
  itemStyle: { color: string };
};

const COLOR_OVERDUE_BORDER = "#DC2328";

function durationDays(startTs: number, endTs: number): number {
  return Math.max(1, Math.round((endTs - startTs) / DAY_MS));
}

function composeBarText(
  board: string,
  days: number,
  status: string,
  description: string,
  multiline: boolean
): string {
  const head = [board, `${days}д`];
  if (status) {
    head.push(status);
  }
  const headStr = head.join(" · ");
  if (!description) {
    return headStr;
  }
  return multiline ? `${headStr}\n${description}` : `${headStr} · ${description}`;
}

type SlotSeriesPoint = {
  value: [number, number, number, number, string];
};

function formatDateDDMMYYYY(value: unknown): string {
  const ts = Number(value);
  const d = new Date(ts);
  if (!Number.isFinite(d.getTime())) {
    return "-";
  }
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yyyy = d.getFullYear();
  return `${dd}-${mm}-${yyyy}`;
}

function formatDateYYYY(value: number): string {
  const d = new Date(value);
  return Number.isFinite(d.getTime()) ? String(d.getFullYear()) : "";
}

function formatType(group: number): string {
  return group === 2 ? "Ми-17" : "Ми-8";
}

function buildLongFreeWindowPoints(
  aircraftPoints: AircraftSeriesPoint[],
  categories: string[],
  xMin: number,
  xMax: number
): SlotSeriesPoint[] {
  if (!aircraftPoints.length || !Number.isFinite(xMin) || !Number.isFinite(xMax)) {
    return [];
  }

  const occupiedByCategory = new Map<number, Array<{ start: number; end: number }>>();
  for (const item of aircraftPoints) {
    const [categoryIndex, start, end] = item.value;
    if (!occupiedByCategory.has(categoryIndex)) {
      occupiedByCategory.set(categoryIndex, []);
    }
    occupiedByCategory.get(categoryIndex)!.push({ start, end });
  }

  const slotPoints: SlotSeriesPoint[] = [];

  for (let idx = 0; idx < categories.length; idx += 1) {
    const ranges = (occupiedByCategory.get(idx) || [])
      .slice()
      .sort((a, b) => a.start - b.start);

    const merged: Array<{ start: number; end: number }> = [];
    for (const range of ranges) {
      const last = merged[merged.length - 1];
      if (!last || range.start > last.end) {
        merged.push({ ...range });
      } else {
        last.end = Math.max(last.end, range.end);
      }
    }

    let cursor = xMin;
    for (const range of merged) {
      if (range.start > cursor) {
        const days = Math.floor((range.start - cursor) / DAY_MS);
        const slots = Math.floor(days / LONG_FREE_WINDOW_DAYS);
        if (days > LONG_FREE_WINDOW_DAYS && slots > 0) {
          slotPoints.push({
            value: [idx, cursor, range.start, slots, categories[idx]!]
          });
        }
      }
      cursor = Math.max(cursor, range.end);
    }

    if (cursor < xMax) {
      const days = Math.floor((xMax - cursor) / DAY_MS);
      const slots = Math.floor(days / LONG_FREE_WINDOW_DAYS);
      if (days > LONG_FREE_WINDOW_DAYS && slots > 0) {
        slotPoints.push({
          value: [idx, cursor, xMax, slots, categories[idx]!]
        });
      }
    }
  }

  return slotPoints;
}

function renderAircraftBar(params: any, api: any) {
  const categoryIndex = Number(api.value(0));
  const start = api.coord([api.value(1), categoryIndex]);
  const end = api.coord([api.value(2), categoryIndex]);
  const size = api.size([0, 1]);
  const h = (Array.isArray(size) ? size[1] : Number(size || 0)) * 0.78;
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

  const board = String(api.value(7) ?? api.value(3) ?? "");
  const days = Number(api.value(8) ?? 0);
  const status = String(api.value(9) ?? "");
  const description = String(api.value(10) ?? "");
  const multiline = rect.height >= 22 && rect.width >= 90;
  const barText = composeBarText(board, days, status, description, multiline);
  const showText = rect.width >= 26;
  const isOverdue = Number(api.value(6)) === 1;
  const fontSize = multiline
    ? Math.max(9, Math.min(11, rect.height / 2 - 1))
    : Math.max(10, Math.min(12, rect.height - 1));
  return {
    type: "group",
    children: [
      {
        type: "rect",
        shape: rect,
        style: {
          ...baseStyle,
          stroke: isOverdue ? COLOR_OVERDUE_BORDER : "#1f2937",
          lineWidth: isOverdue ? 2.5 : 0.6,
          opacity: 0.92
        },
        emphasis: {
          style: {
            opacity: 1,
            lineWidth: isOverdue ? 3 : 1
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
          fontSize,
          width: Math.max(rect.width - 8, 0),
          overflow: "truncate",
          lineHeight: fontSize + 2
        },
        silent: true
      }
    ]
  };
}

function renderSlotBar(params: any, api: any) {
  const categoryIndex = Number(api.value(0));
  const start = api.coord([api.value(1), categoryIndex]);
  const end = api.coord([api.value(2), categoryIndex]);
  const size = api.size([0, 1]);
  const h = (Array.isArray(size) ? size[1] : Number(size || 0)) * 0.52;
  const coord = params.coordSys || {};

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

  const slots = Number(api.value(3) ?? 0);
  return {
    type: "group",
    children: [
      {
        type: "rect",
        shape: rect,
        style: {
          fill: "rgba(0,0,0,0)",
          stroke: COLOR_SLOT_BORDER,
          lineWidth: 2
        }
      },
      {
        type: "text",
        style: {
          x: rect.x + rect.width / 2,
          y: rect.y + rect.height / 2,
          text: slots > 0 ? String(slots) : "",
          verticalAlign: "middle",
          textAlign: "center",
          fill: COLOR_SLOT_BORDER,
          fontWeight: 700,
          fontSize: 11
        },
        silent: true
      }
    ]
  };
}

export default function Echarts6Gantt(props: Echarts6GanttTransformedProps): JSX.Element {
  const { width, height, data } = props;
  const rootRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<echarts.ECharts | null>(null);

  const categories = useMemo(
    () =>
      Array.from(new Set(data.map(point => point.category))).sort((a, b) =>
        a.localeCompare(b, "en", { numeric: true })
      ),
    [data]
  );

  const categoryIndexMap = useMemo(
    () => new Map(categories.map((category, idx) => [category, idx])),
    [categories]
  );

  const xExtent = useMemo(() => {
    if (!data.length) {
      return { min: undefined as number | undefined, max: undefined as number | undefined };
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

  const aircraftSeriesData = useMemo<AircraftSeriesPoint[]>(
    () =>
      data
        .map(point => {
          const categoryIndex = categoryIndexMap.get(point.category);
          if (categoryIndex === undefined) {
            return null;
          }
          const stableGroup = dominantGroupByLabel.get(point.label) ?? point.groupBy ?? 1;
          const days = durationDays(point.startTs, point.endTs);
          const status = point.status || "";
          const description = point.description || "";
          const barText = composeBarText(
            String(point.label),
            days,
            status,
            description,
            false
          );
          return {
            value: [
              categoryIndex,
              point.startTs,
              point.endTs,
              barText,
              point.category,
              stableGroup,
              point.overdue === 1 ? 1 : 0,
              String(point.label),
              days,
              status,
              description
            ],
            itemStyle: {
              color: stableGroup === 2 ? COLOR_MI17 : COLOR_MI8
            }
          };
        })
        .filter((item): item is AircraftSeriesPoint => item !== null),
    [categoryIndexMap, data, dominantGroupByLabel]
  );

  const freeWindowSeriesData = useMemo(
    () =>
      buildLongFreeWindowPoints(
        aircraftSeriesData,
        categories,
        xExtent.min ?? 0,
        xExtent.max ?? 0
      ),
    [aircraftSeriesData, categories, xExtent.max, xExtent.min]
  );

  useEffect(() => {
    if (!rootRef.current) {
      return undefined;
    }

    if (!chartRef.current) {
      chartRef.current = echarts.init(rootRef.current, undefined, {
        renderer: "canvas"
      });
    }

    const option: echarts.EChartsOption = {
      animation: true,
      backgroundColor: "#ffffff",
      color: [COLOR_MI8, COLOR_MI17, "rgba(0,0,0,0)"],
      grid: { top: 52, right: 28, bottom: 84, left: 96 },
      tooltip: {
        trigger: "item",
        axisPointer: { type: "cross" },
        formatter: (params: any) => {
          const payload = Array.isArray(params) ? params[0] : params;
          const value = Array.isArray(payload?.value) ? payload.value : [];
          const seriesName = String(payload?.seriesName || "");
          if (seriesName === "Слоты") {
            return [
              `Линия: ${value[4] ?? "-"}`,
              `Свободные слоты (180д): ${value[3] ?? "-"}`,
              `${formatDateDDMMYYYY(value[1])} -> ${formatDateDDMMYYYY(value[2])}`
            ].join("<br/>");
          }
          const group = Number(value[5] ?? 1);
          const board = String(value[7] ?? value[3] ?? "-");
          const days = Number(value[8] ?? 0);
          const status = String(value[9] ?? "");
          const description = String(value[10] ?? "");
          const lines = [
            `Линия: ${value[4] ?? "-"}`,
            `Борт: ${board}`,
            `Тип ВС: ${formatType(group)}`,
            days > 0 ? `Длительность: ${days}д` : null,
            status ? `Статус: ${status}` : null,
            description ? `Описание: ${description}` : null,
            `${formatDateDDMMYYYY(value[1])} -> ${formatDateDDMMYYYY(value[2])}`
          ].filter(Boolean) as string[];
          if (Number(value[6]) === 1) {
            lines.push(
              `<span style="color:${COLOR_OVERDUE_BORDER};font-weight:700">⚠ Просрочен (дата в прошлом, не закрыт)</span>`
            );
          }
          return lines.join("<br/>");
        }
      },
      legend: {
        top: 8,
        left: "center",
        selectedMode: true,
        textStyle: { color: COLOR_AXIS_TEXT, fontWeight: 600 },
        backgroundColor: "rgba(255,255,255,0.86)",
        borderColor: "#cbd5e1",
        borderWidth: 1,
        borderRadius: 8,
        padding: [6, 10, 6, 10],
        itemWidth: 20,
        itemHeight: 10,
        data: [
          { name: "Ми-8", icon: "roundRect" },
          { name: "Ми-17", icon: "roundRect" },
          {
            name: "Слоты",
            icon: "roundRect",
            itemStyle: {
              color: "rgba(0,0,0,0)",
              borderColor: COLOR_SLOT_BORDER,
              borderWidth: 2
            }
          }
        ]
      },
      xAxis: {
        type: "time",
        min: xExtent.min,
        max: xExtent.max,
        splitLine: { show: true, lineStyle: { color: "#e2e8f0" } },
        axisLabel: {
          color: COLOR_AXIS_TEXT,
          formatter: formatDateYYYY
        }
      },
      yAxis: {
        type: "category",
        data: categories,
        axisLabel: {
          interval: 0,
          color: COLOR_AXIS_TEXT,
          formatter: (value: string) => {
            const n = Number(value);
            return Number.isFinite(n) ? String(n + 1) : value;
          }
        },
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
      series: (([
        {
          type: "custom",
          name: "Ми-8",
          progressive: 0,
          itemStyle: { color: COLOR_MI8 },
          renderItem: renderAircraftBar,
          encode: { x: [1, 2], y: 0, tooltip: [4, 3, 5, 1, 2] },
          data: aircraftSeriesData.filter(item => Number(item.value[5]) === 1)
        },
        {
          type: "custom",
          name: "Ми-17",
          progressive: 0,
          itemStyle: { color: COLOR_MI17 },
          renderItem: renderAircraftBar,
          encode: { x: [1, 2], y: 0, tooltip: [4, 3, 5, 1, 2] },
          markLine: {
            symbol: "none",
            label: { formatter: "Сегодня", position: "insideEndTop" },
            lineStyle: { type: "dashed", color: "#475569", width: 1 },
            data: [{ xAxis: Date.now() }]
          },
          data: aircraftSeriesData.filter(item => Number(item.value[5]) === 2)
        },
        {
          type: "custom",
          name: "Слоты",
          progressive: 0,
          itemStyle: {
            color: "rgba(0,0,0,0)",
            borderColor: COLOR_SLOT_BORDER,
            borderWidth: 2
          },
          renderItem: renderSlotBar,
          encode: { x: [1, 2], y: 0, tooltip: [4, 3, 1, 2] },
          data: freeWindowSeriesData
        }
      ]) as any[]),
    };

    chartRef.current.setOption(option as any, true);
    chartRef.current.resize({ width, height });

    return () => {
      if (chartRef.current) {
        chartRef.current.dispose();
        chartRef.current = null;
      }
    };
  }, [aircraftSeriesData, categories, data, freeWindowSeriesData, height, width, xExtent.max, xExtent.min]);

  return <div ref={rootRef} style={{ width, height }} />;
}
