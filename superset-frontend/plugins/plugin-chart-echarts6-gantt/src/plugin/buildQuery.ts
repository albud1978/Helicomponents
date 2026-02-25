import {
  BuildQuery,
  SqlaFormData,
  buildQueryContext,
  ensureIsArray
} from "@superset-ui/core";

const buildQuery: BuildQuery<SqlaFormData> = formData =>
  buildQueryContext(formData, baseQueryObject => {
    const inputColumns = ensureIsArray(formData.groupby).map(String);
    const columns = Array.from(new Set([...inputColumns, "group_by"]));
    const metrics = ensureIsArray(formData.metrics);
    const rowLimitRaw = formData.row_limit || baseQueryObject.row_limit || 10000;
    const rowLimit = Number(rowLimitRaw);

    return [
      {
        ...baseQueryObject,
        columns,
        metrics,
        row_limit: Number.isFinite(rowLimit) ? rowLimit : 10000
      }
    ];
  });

export default buildQuery;
