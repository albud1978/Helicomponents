import { t } from "@superset-ui/core";
import { ControlPanelConfig } from "@superset-ui/chart-controls";

const config: ControlPanelConfig = {
  controlPanelSections: [
    {
      label: t("Query"),
      expanded: true,
      controlSetRows: [["groupby"], ["metrics"], ["adhoc_filters"], ["row_limit"]]
    }
  ]
};

export default config;
