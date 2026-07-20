export type GanttPoint = {
  startTs: number;
  endTs: number;
  category: string;
  value: number;
  label: string;
  groupBy: number | null;
  dayIndexStart: number | null;
  overdue: number | null;
  description: string;
  status: string;
};

export type Echarts6GanttTransformedProps = {
  width: number;
  height: number;
  data: GanttPoint[];
  metricLabel: string;
};
