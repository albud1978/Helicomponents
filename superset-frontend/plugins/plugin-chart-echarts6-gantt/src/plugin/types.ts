export type GanttPoint = {
  startTs: number;
  endTs: number;
  category: string;
  value: number;
  label: string;
  groupBy: number | null;
  dayIndexStart: number | null;
};

export type Echarts6GanttTransformedProps = {
  width: number;
  height: number;
  data: GanttPoint[];
  metricLabel: string;
};
