import { ChartProps, getMetricLabel } from "@superset-ui/core";
import { Echarts6GanttTransformedProps, GanttPoint } from "./types";

const START_TIME_KEYS = [
  "__timestamp",
  "start",
  "start_ts",
  "start_date",
  "day_date",
  "timestamp",
  "ds"
];
const END_TIME_KEYS = ["end", "end_ts", "end_date", "finish", "to"];
const LABEL_KEYS = ["label", "name", "aircraft_number"];
const GROUP_KEYS = ["group_by", "groupBy", "group"];
const DAY_INDEX_KEYS = ["day_u16", "day_index", "day_u32"];
const DAY_MS = 24 * 60 * 60 * 1000;

type RawRow = Record<string, unknown>;

function parseTs(raw: unknown): number {
  if (raw instanceof Date) {
    return raw.getTime();
  }
  if (typeof raw === "string") {
    const parsed = Date.parse(raw);
    return Number.isFinite(parsed) ? parsed : Number.NaN;
  }
  const num = Number(raw);
  return Number.isFinite(num) ? num : Number.NaN;
}

function detectTimeKey(rows: RawRow[], candidates: string[]): string | undefined {
  return candidates.find(key => rows.some(row => row[key] !== undefined));
}

function parseOptionalInt(raw: unknown): number | null {
  const num = Number(raw);
  if (!Number.isFinite(num)) {
    return null;
  }
  return Math.trunc(num);
}

export default function transformProps(
  chartProps: ChartProps
): Echarts6GanttTransformedProps {
  const { width, height, queriesData, formData } = chartProps;
  const queryData = queriesData?.[0];
  const rawRows = (queryData?.data || []) as RawRow[];
  const categoryKey = Array.isArray(formData.groupby) && formData.groupby.length
    ? String(formData.groupby[0])
    : "category";

  const metricLabel = Array.isArray(formData.metrics) && formData.metrics.length
    ? getMetricLabel(formData.metrics[0])
    : "value";

  const startKey = detectTimeKey(rawRows, START_TIME_KEYS);
  const endKey = detectTimeKey(rawRows, END_TIME_KEYS);
  const labelKey = detectTimeKey(rawRows, LABEL_KEYS);
  const groupKey = detectTimeKey(rawRows, GROUP_KEYS);
  const dayIndexKey = detectTimeKey(rawRows, DAY_INDEX_KEYS);

  // Prefer explicit start/end ranges; otherwise merge contiguous day points into intervals.
  let data: GanttPoint[] = [];

  if (endKey) {
    data = rawRows
      .map(row => {
        const startTs = parseTs(startKey ? row[startKey] : undefined);
        const rawEnd = parseTs(row[endKey]);
        const endTs = Number.isFinite(rawEnd) ? rawEnd : startTs + DAY_MS;
        const left = Math.min(startTs, endTs);
        const right = Math.max(startTs, endTs);
        const metricValue = Number(row[metricLabel] ?? 0);
        const fallbackLabel = labelKey ? row[labelKey] : row[metricLabel];
        const groupByParsed = parseOptionalInt(
          row.group_by ?? (groupKey ? row[groupKey] : null)
        );
        const dayIndexStart = parseOptionalInt(
          row.day_u16 ?? (dayIndexKey ? row[dayIndexKey] : null)
        );
        return {
          startTs: left,
          endTs: right,
          category: String(row[categoryKey] ?? "N/A"),
          value: Number.isFinite(metricValue) ? metricValue : 0,
          label: String(fallbackLabel ?? metricValue ?? "N/A"),
          groupBy: groupByParsed,
          dayIndexStart
        };
      })
      .filter(point => Number.isFinite(point.startTs) && Number.isFinite(point.endTs));
  } else {
    const points = rawRows
      .map(row => {
        const startTs = parseTs(startKey ? row[startKey] : undefined);
        const metricValue = Number(row[metricLabel] ?? 0);
        const fallbackLabel = labelKey ? row[labelKey] : row[metricLabel];
        const groupByParsed = parseOptionalInt(
          row.group_by ?? (groupKey ? row[groupKey] : null)
        );
        const dayIndexStart = parseOptionalInt(
          row.day_u16 ?? (dayIndexKey ? row[dayIndexKey] : null)
        );
        return {
          startTs,
          category: String(row[categoryKey] ?? "N/A"),
          value: Number.isFinite(metricValue) ? metricValue : 0,
          label: String(fallbackLabel ?? metricValue ?? "N/A"),
          groupBy: groupByParsed,
          dayIndexStart
        };
      })
      .filter(point => Number.isFinite(point.startTs))
      .sort((a, b) => {
        if (a.category !== b.category) {
          return a.category.localeCompare(b.category, "en", { numeric: true });
        }
        if (a.groupBy !== b.groupBy) {
          return (a.groupBy ?? -1) - (b.groupBy ?? -1);
        }
        if (a.label !== b.label) {
          return a.label.localeCompare(b.label, "en", { numeric: true });
        }
        return a.startTs - b.startTs;
      });

    for (const point of points) {
      const last = data[data.length - 1];
      if (
        last &&
        last.category === point.category &&
        last.groupBy === point.groupBy &&
        last.label === point.label &&
        point.startTs <= last.endTs + DAY_MS
      ) {
        last.endTs = Math.max(last.endTs, point.startTs + DAY_MS);
      } else {
        data.push({
          startTs: point.startTs,
          endTs: point.startTs + DAY_MS,
          category: point.category,
          value: point.value,
          label: point.label,
          groupBy: point.groupBy,
          dayIndexStart: point.dayIndexStart
        });
      }
    }
  }

  return {
    width,
    height,
    data,
    metricLabel
  };
}
