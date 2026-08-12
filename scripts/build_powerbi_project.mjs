import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const args = process.argv.slice(2);
const option = (name, fallback) => {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : fallback;
};

const outputRoot = path.resolve(option("--output", "powerbi/project"));
const dataRoot = option("--data-root", "__DATA_ROOT__");
const projectName = option("--project-name", "SupplyChainOperations");
const reportRoot = path.join(outputRoot, `${projectName}.Report`);
const modelRoot = path.join(outputRoot, `${projectName}.SemanticModel`);
const reportDefinition = path.join(reportRoot, "definition");
const modelDefinition = path.join(modelRoot, "definition");

const write = (filePath, content) => {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const text = typeof content === "string" ? content : `${JSON.stringify(content, null, 2)}\n`;
  fs.writeFileSync(filePath, text, "utf8");
};

const stableHex = (value, length) =>
  crypto.createHash("sha256").update(value).digest("hex").slice(0, length);
const stableUuid = (value) => {
  const hex = stableHex(value, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};
const pageId = (name) => `ReportSection${stableHex(`page:${name}`, 24)}`;
const visualId = (page, name) => stableHex(`visual:${page}:${name}`, 20);
const mString = (value) => `"${value.replaceAll('"', '""')}"`;
const tmdlName = (value) => (/^[A-Za-z_][A-Za-z0-9_]*$/.test(value) ? value : `'${value}'`);

const databaseId = stableUuid(`database:${projectName}`);
const tableSpecs = [
  {
    name: "dim_date",
    file: "dim_date.parquet",
    dataCategory: "Time",
    columns: [
      ["date_key", "int64", { hidden: true, summarize: "none" }],
      ["calendar_date", "dateTime", { format: "yyyy-mm-dd", summarize: "none" }],
      ["calendar_year", "int64", { format: "0", summarize: "none" }],
      ["calendar_quarter", "int64", { format: "0", summarize: "none" }],
      ["calendar_month", "int64", { format: "0", summarize: "none" }],
      ["month_name", "string", { sortBy: "calendar_month", summarize: "none" }],
      ["iso_week", "int64", { format: "0", summarize: "none" }],
      ["day_of_week", "int64", { format: "0", summarize: "none" }],
      ["day_name", "string", { summarize: "none" }],
      ["is_weekend", "boolean", { summarize: "none" }],
    ],
  },
  {
    name: "dim_hub",
    file: "dim_hub.parquet",
    columns: [
      ["hub_key", "int64", { hidden: true, summarize: "none" }],
      ["hub_id", "string", { summarize: "none" }],
      ["hub_name", "string", { summarize: "none" }],
      ["city", "string", { summarize: "none" }],
      ["state_code", "string", { summarize: "none" }],
    ],
  },
  {
    name: "dim_route",
    file: "dim_route.parquet",
    columns: [
      ["route_key", "int64", { hidden: true, summarize: "none" }],
      ["route_id", "string", { summarize: "none" }],
      ["origin_hub_id", "string", { summarize: "none" }],
      ["destination_hub_id", "string", { summarize: "none" }],
      ["distance_km", "int64", { format: "#,##0", summarize: "none" }],
      ["route_label", "string", { summarize: "none" }],
    ],
  },
  {
    name: "dim_service_level",
    file: "dim_service_level.parquet",
    columns: [
      ["service_level_key", "int64", { hidden: true, summarize: "none" }],
      ["service_level_id", "string", { summarize: "none" }],
      ["service_level_name", "string", { summarize: "none" }],
      ["sla_days", "int64", { format: "0", summarize: "none" }],
      ["base_price_eur", "double", { format: "€#,##0.00", summarize: "none" }],
    ],
  },
  {
    name: "fact_shipments",
    file: "fact_shipments.parquet",
    columns: [
      ["shipment_id", "string", { summarize: "none" }],
      ["created_date_key", "int64", { hidden: true, summarize: "none" }],
      ["promised_date_key", "int64", { hidden: true, summarize: "none" }],
      ["delivered_date_key", "int64", { hidden: true, summarize: "none" }],
      ["origin_hub_key", "int64", { hidden: true, summarize: "none" }],
      ["destination_hub_key", "int64", { hidden: true, summarize: "none" }],
      ["route_key", "int64", { hidden: true, summarize: "none" }],
      ["service_level_key", "int64", { hidden: true, summarize: "none" }],
      ["customer_region", "string", { summarize: "none" }],
      ["weight_kg", "double", { format: "0.00", summarize: "none" }],
      ["created_ts", "dateTime", { format: "yyyy-mm-dd HH:mm", summarize: "none" }],
      ["promised_delivery_ts", "dateTime", { format: "yyyy-mm-dd HH:mm", summarize: "none" }],
      ["actual_delivery_ts", "dateTime", { format: "yyyy-mm-dd HH:mm", summarize: "none" }],
      ["transit_hours", "double", { format: "0.0", summarize: "none" }],
      ["delay_minutes", "int64", { format: "#,##0", summarize: "none" }],
      ["is_on_time", "boolean", { summarize: "none" }],
      ["current_status", "string", { summarize: "none" }],
      ["valid_event_count", "int64", { format: "0", summarize: "none" }],
      ["late_arriving_event_count", "int64", { format: "0", summarize: "none" }],
    ],
  },
  {
    name: "fact_events",
    file: "fact_events.parquet",
    columns: [
      ["event_id", "string", { summarize: "none" }],
      ["shipment_id", "string", { summarize: "none" }],
      ["event_date_key", "int64", { hidden: true, summarize: "none" }],
      ["hub_key", "int64", { hidden: true, summarize: "none" }],
      ["event_sequence", "int64", { format: "0", summarize: "none" }],
      ["event_type", "string", { summarize: "none" }],
      ["event_ts", "dateTime", { format: "yyyy-mm-dd HH:mm", summarize: "none" }],
      ["arrival_batch_date", "dateTime", { format: "yyyy-mm-dd", summarize: "none" }],
      ["arrival_lag_days", "int64", { format: "0", summarize: "none" }],
      ["is_late_arriving", "boolean", { summarize: "none" }],
    ],
  },
  {
    name: "fact_data_quality",
    file: "fact_data_quality.parquet",
    columns: [
      ["evaluation_run_id", "string", { hidden: true, summarize: "none" }],
      ["evaluated_at", "dateTime", { format: "yyyy-mm-dd HH:mm", summarize: "none" }],
      ["rule_code", "string", { summarize: "none" }],
      ["record_type", "string", { summarize: "none" }],
      ["failed_record_count", "int64", { format: "#,##0", summarize: "sum" }],
      ["affected_file_count", "int64", { format: "#,##0", summarize: "sum" }],
    ],
  },
];

const tableTmdl = (spec) => {
  const fileExpression =
    dataRoot === "__DATA_ROOT__"
      ? `#\"pDataRoot\" & \"\\${spec.file}\"`
      : mString(path.resolve(dataRoot, spec.file));
  const lines = [`table ${tmdlName(spec.name)}`];
  if (spec.dataCategory) lines.push(`\tdataCategory: ${spec.dataCategory}`);
  for (const [name, type, options] of spec.columns) {
    lines.push("", `\t/// ${name.replaceAll("_", " ")}`, `\tcolumn ${tmdlName(name)}`);
    lines.push(`\t\tdataType: ${type}`);
    if (options.hidden) lines.push("\t\tisHidden");
    if (options.format) lines.push(`\t\tformatString: ${options.format}`);
    if (options.summarize) lines.push(`\t\tsummarizeBy: ${options.summarize}`);
    if (options.sortBy) lines.push(`\t\tsortByColumn: ${options.sortBy}`);
    lines.push(`\t\tsourceColumn: ${name}`);
  }
  lines.push(
    "",
    `\tpartition ${tmdlName(spec.name)} = m`,
    "\t\tmode: import",
    "\t\tsource =",
    "\t\t\tlet",
    `\t\t\t\tSource = Parquet.Document(File.Contents(${fileExpression}))`,
    "\t\t\tin",
    "\t\t\t\tSource",
    "",
  );
  return lines.join("\n");
};

const measuresTmdl = `table _Measures

	/// Distinct valid shipments in the active filter context
	measure Shipments = DISTINCTCOUNT(fact_shipments[shipment_id])
		formatString: #,##0

	/// Valid shipments with a delivery timestamp
	measure 'Delivered Shipments' = CALCULATE([Shipments], NOT ISBLANK(fact_shipments[actual_delivery_ts]))
		formatString: #,##0

	/// Delivered shipments that met the promise timestamp
	measure 'On-Time Deliveries' = CALCULATE([Delivered Shipments], fact_shipments[is_on_time] = TRUE())
		formatString: #,##0

	/// Share of delivered shipments that met the promise timestamp
	measure 'On-Time Delivery %' = DIVIDE([On-Time Deliveries], [Delivered Shipments])
		formatString: 0.0%

	/// Average positive delivery delay in hours
	measure 'Average Delay Hours' = DIVIDE(AVERAGEX(FILTER(fact_shipments, fact_shipments[delay_minutes] > 0), fact_shipments[delay_minutes]), 60)
		formatString: 0.0

	/// Ninetieth percentile of delivered transit duration
	measure 'P90 Transit Hours' = PERCENTILEX.INC(FILTER(fact_shipments, NOT ISBLANK(fact_shipments[actual_delivery_ts])), fact_shipments[transit_hours], 0.9)
		formatString: 0.0

	/// Delivered shipments that missed their promise
	measure 'Late Shipments' = CALCULATE([Delivered Shipments], fact_shipments[is_on_time] = FALSE())
		formatString: #,##0

	/// Shipments whose latest valid event is not delivered
	measure 'Exception Shipments' = CALCULATE([Shipments], fact_shipments[current_status] <> "DELIVERED")
		formatString: #,##0

	/// Valid deduplicated shipment events
	measure Events = COUNTROWS(fact_events)
		formatString: #,##0

	/// Valid events arriving at least two days after business event date
	measure 'Late-Arriving Events' = CALCULATE([Events], fact_events[is_late_arriving] = TRUE())
		formatString: #,##0

	/// Share of valid events arriving at least two days late
	measure 'Late-Arriving Event %' = DIVIDE([Late-Arriving Events], [Events])
		formatString: 0.00%

	/// Rows rejected by current data-quality evaluation
	measure 'Quarantined Records' = SUM(fact_data_quality[failed_record_count])
		formatString: #,##0

	/// Sum of affected file counts across failed rules
	measure 'Affected Source Files' = SUM(fact_data_quality[affected_file_count])
		formatString: #,##0

	/// Valid rows as a share of valid plus quarantined rows
	measure 'First-Pass Quality %' = 1 - DIVIDE([Quarantined Records], [Shipments] + [Events] + [Quarantined Records])
		formatString: 0.00%

	column Dummy
		isHidden
		summarizeBy: none
		sourceColumn: [Dummy]

	partition _Measures = calculated
		mode: import
		source = ROW("Dummy", BLANK())
`;

write(
  path.join(outputRoot, `${projectName}.pbip`),
  {
    $schema: "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
    version: "1.0",
    artifacts: [{ report: { path: `${projectName}.Report` } }],
    settings: { enableAutoRecovery: true },
  },
);
write(path.join(reportRoot, ".platform"), {
  $schema:
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  metadata: {
    type: "Report",
    displayName: "Supply Chain Operations",
    description: "Operations report for the Nordlicht Logistics synthetic dataset",
  },
  config: { version: "2.0", logicalId: stableUuid(`report:${projectName}`) },
});
write(path.join(modelRoot, ".platform"), {
  $schema:
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  metadata: {
    type: "SemanticModel",
    displayName: "Supply Chain Operations",
    description: "Semantic model for the Nordlicht Logistics synthetic dataset",
  },
  config: { version: "2.0", logicalId: stableUuid(`model:${projectName}`) },
});
write(
  path.join(modelRoot, "definition.pbism"),
  {
    $schema:
      "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
    version: "4.2",
    settings: { qnaEnabled: false },
  },
);
write(
  path.join(modelDefinition, "database.tmdl"),
  `database ${databaseId}\n\tcompatibilityLevel: 1702\n\tcompatibilityMode: powerBI\n\tlanguage: 1033\n`,
);
write(
  path.join(modelDefinition, "expressions.tmdl"),
  `expression pDataRoot = ${mString(dataRoot)} meta [IsParameterQuery=true, Type=\"Text\", IsParameterQueryRequired=true]\n`,
);
write(
  path.join(modelDefinition, "model.tmdl"),
  `model Model
\tculture: en-US
\tdefaultPowerBIDataSourceVersion: powerBI_V3
\tsourceQueryCulture: en-US
\tdiscourageImplicitMeasures

${tableSpecs.map((table) => `ref table ${tmdlName(table.name)}`).join("\n")}
ref table _Measures
`,
);
for (const spec of tableSpecs) {
  write(path.join(modelDefinition, "tables", `${spec.name}.tmdl`), tableTmdl(spec));
}
write(path.join(modelDefinition, "tables", "_Measures.tmdl"), measuresTmdl);
write(
  path.join(modelDefinition, "relationships.tmdl"),
  `relationship 'Shipments Created Date'
\tfromColumn: fact_shipments.created_date_key
\ttoColumn: dim_date.date_key

relationship 'Shipments Promised Date'
\tisActive: false
\tfromColumn: fact_shipments.promised_date_key
\ttoColumn: dim_date.date_key

relationship 'Shipments Delivered Date'
\tisActive: false
\tfromColumn: fact_shipments.delivered_date_key
\ttoColumn: dim_date.date_key

relationship 'Events Date'
\tfromColumn: fact_events.event_date_key
\ttoColumn: dim_date.date_key

relationship 'Events Hub'
\tfromColumn: fact_events.hub_key
\ttoColumn: dim_hub.hub_key

relationship 'Shipments Route'
\tfromColumn: fact_shipments.route_key
\ttoColumn: dim_route.route_key

relationship 'Shipments Service Level'
\tfromColumn: fact_shipments.service_level_key
\ttoColumn: dim_service_level.service_level_key
`,
);

write(
  path.join(reportRoot, "definition.pbir"),
  {
    $schema:
      "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
    version: "4.0",
    datasetReference: { byPath: { path: `../${projectName}.SemanticModel` } },
  },
);
write(
  path.join(reportDefinition, "version.json"),
  {
    $schema:
      "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
    version: "2.0.0",
  },
);
write(
  path.join(reportDefinition, "report.json"),
  {
    $schema:
      "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.3.0/schema.json",
    themeCollection: {
      baseTheme: {
        name: "CY24SU06",
        reportVersionAtImport: { visual: "2.6.0", report: "3.1.0", page: "2.3.0" },
        type: "SharedResources",
      },
    },
    annotations: [{ name: "dataClassification", value: "Synthetic demonstration data" }],
  },
);

const literal = (value, kind = "string") => {
  if (kind === "bool") return { expr: { Literal: { Value: value ? "true" : "false" } } };
  if (kind === "number") return { expr: { Literal: { Value: `${value}D` } } };
  return { expr: { Literal: { Value: `'${String(value).replaceAll("'", "''")}'` } } };
};
const measureField = (property) => ({
  Measure: { Expression: { SourceRef: { Entity: "_Measures" } }, Property: property },
});
const columnField = (entity, property) => ({
  Column: { Expression: { SourceRef: { Entity: entity } }, Property: property },
});
const projection = (field, queryRef, nativeQueryRef) => ({ field, queryRef, nativeQueryRef });
const titleObjects = (title, altText) => ({
  title: [
    {
      properties: {
        show: literal(true, "bool"),
        text: literal(title),
        fontSize: literal(12, "number"),
        bold: literal(true, "bool"),
      },
    },
  ],
  general: [{ properties: { altText: literal(altText ?? title) } }],
});
const makeVisual = (pageName, name, type, title, position, roles, altText) => ({
  $schema:
    "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.9.0/schema.json",
  name: visualId(pageName, name),
  position: { ...position, z: position.tabOrder, tabOrder: position.tabOrder },
  visual: {
    visualType: type,
    query: { queryState: roles },
    visualContainerObjects: titleObjects(title, altText),
  },
});
const measureProjection = (name) => projection(measureField(name), `_Measures.${name}`, name);
const columnProjection = (table, name) =>
  projection(columnField(table, name), `${table}.${name}`, name);

const pages = [
  {
    name: "Executive Overview",
    visuals: [
      ["shipments_card", "cardVisual", "Shipments", [20, 20, 232, 105], { Data: [measureProjection("Shipments")] }],
      ["delivered_card", "cardVisual", "Delivered Shipments", [272, 20, 232, 105], { Data: [measureProjection("Delivered Shipments")] }],
      ["ontime_card", "cardVisual", "On-Time Delivery %", [524, 20, 232, 105], { Data: [measureProjection("On-Time Delivery %")] }],
      ["delay_card", "cardVisual", "Average Delay Hours", [776, 20, 232, 105], { Data: [measureProjection("Average Delay Hours")] }],
      ["p90_card", "cardVisual", "P90 Transit Hours", [1028, 20, 232, 105], { Data: [measureProjection("P90 Transit Hours")] }],
      ["weekly_line", "lineChart", "Created-date delivery performance", [20, 145, 780, 260], {
        Category: [columnProjection("dim_date", "calendar_date")],
        Y: [measureProjection("On-Time Delivery %"), measureProjection("Shipments")],
      }],
      ["service_chart", "clusteredColumnChart", "On-time rate by service level", [820, 145, 440, 260], {
        Category: [columnProjection("dim_service_level", "service_level_name")],
        Y: [measureProjection("On-Time Delivery %")],
        Tooltips: [measureProjection("Shipments"), measureProjection("Average Delay Hours")],
      }],
      ["service_slicer", "listSlicer", "Service Level", [20, 425, 135, 265], {
        Values: [columnProjection("dim_service_level", "service_level_name")],
      }],
      ["region_slicer", "listSlicer", "Customer Region", [165, 425, 135, 265], {
        Values: [columnProjection("fact_shipments", "customer_region")],
      }],
      ["route_late_chart", "clusteredBarChart", "Late shipments by route", [320, 425, 940, 265], {
        Category: [columnProjection("dim_route", "route_label")],
        Y: [measureProjection("Late Shipments")],
        Tooltips: [measureProjection("Shipments"), measureProjection("On-Time Delivery %")],
      }],
    ],
  },
  {
    name: "Hub Route Performance",
    visuals: [
      ["events_card", "cardVisual", "Valid Events", [20, 20, 260, 105], { Data: [measureProjection("Events")] }],
      ["late_arrival_card", "cardVisual", "Late-Arriving Event %", [300, 20, 260, 105], { Data: [measureProjection("Late-Arriving Event %")] }],
      ["exception_card", "cardVisual", "Exception Shipments", [580, 20, 260, 105], { Data: [measureProjection("Exception Shipments")] }],
      ["route_slicer", "listSlicer", "Route", [860, 20, 400, 105], { Values: [columnProjection("dim_route", "route_label")] }],
      ["route_matrix", "pivotTable", "Route performance detail", [20, 145, 750, 545], {
        Rows: [columnProjection("dim_route", "route_label")],
        Values: [
          measureProjection("Shipments"),
          measureProjection("On-Time Delivery %"),
          measureProjection("Average Delay Hours"),
          measureProjection("P90 Transit Hours"),
          measureProjection("Late Shipments"),
        ],
      }],
      ["hub_bar", "clusteredBarChart", "Late-arriving events by hub", [790, 145, 470, 260], {
        Category: [columnProjection("dim_hub", "hub_name")],
        Y: [measureProjection("Late-Arriving Events")],
        Tooltips: [measureProjection("Events"), measureProjection("Late-Arriving Event %")],
      }],
      ["event_line", "lineChart", "Event volume and late arrivals", [790, 425, 470, 265], {
        Category: [columnProjection("dim_date", "calendar_date")],
        Y: [measureProjection("Events"), measureProjection("Late-Arriving Events")],
      }],
    ],
  },
  {
    name: "Data Quality Exceptions",
    visuals: [
      ["quarantine_card", "cardVisual", "Quarantined Records", [20, 20, 290, 105], { Data: [measureProjection("Quarantined Records")] }],
      ["quality_card", "cardVisual", "First-Pass Quality %", [330, 20, 290, 105], { Data: [measureProjection("First-Pass Quality %")] }],
      ["files_card", "cardVisual", "Affected Source Files", [640, 20, 290, 105], { Data: [measureProjection("Affected Source Files")] }],
      ["late_events_card", "cardVisual", "Late-Arriving Events", [950, 20, 310, 105], { Data: [measureProjection("Late-Arriving Events")] }],
      ["rule_bar", "clusteredBarChart", "Failed records by quality rule", [20, 145, 600, 260], {
        Category: [columnProjection("fact_data_quality", "rule_code")],
        Y: [measureProjection("Quarantined Records")],
        Tooltips: [measureProjection("Affected Source Files")],
      }],
      ["entity_donut", "donutChart", "Failures by entity", [640, 145, 300, 260], {
        Category: [columnProjection("fact_data_quality", "record_type")],
        Y: [measureProjection("Quarantined Records")],
      }],
      ["entity_slicer", "listSlicer", "Entity Type", [960, 145, 300, 260], {
        Values: [columnProjection("fact_data_quality", "record_type")],
      }],
      ["dq_matrix", "pivotTable", "Rule and source-file impact", [20, 425, 1240, 265], {
        Rows: [columnProjection("fact_data_quality", "record_type"), columnProjection("fact_data_quality", "rule_code")],
        Values: [measureProjection("Quarantined Records"), measureProjection("Affected Source Files")],
      }],
    ],
  },
];

const pageOrder = pages.map((page) => pageId(page.name));
write(
  path.join(reportDefinition, "pages", "pages.json"),
  {
    $schema:
      "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
    pageOrder,
    activePageName: pageOrder[0],
  },
);

let tabOrder = 1000;
for (const page of pages) {
  const id = pageId(page.name);
  write(
    path.join(reportDefinition, "pages", id, "page.json"),
    {
      $schema:
        "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json",
      name: id,
      displayName: page.name,
      displayOption: "FitToPage",
      height: 720,
      width: 1280,
    },
  );
  for (const [name, type, title, [x, y, width, height], roles] of page.visuals) {
    const visual = makeVisual(
      page.name,
      name,
      type,
      title,
      { x, y, width, height, tabOrder },
      Object.fromEntries(
        Object.entries(roles).map(([role, projections]) => [role, { projections }]),
      ),
      title,
    );
    write(
      path.join(reportDefinition, "pages", id, "visuals", visual.name, "visual.json"),
      visual,
    );
    tabOrder += 1000;
  }
}

console.log(
  JSON.stringify(
    {
      project: path.join(outputRoot, `${projectName}.pbip`),
      dataRoot,
      pages: pages.length,
      visuals: pages.reduce((sum, page) => sum + page.visuals.length, 0),
    },
    null,
    2,
  ),
);
