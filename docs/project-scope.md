# Project scope

## Scenario

Nordlicht Logistics GmbH is a fictional parcel network with German hubs, routes, service
levels, shipments, and scan events. I use the dataset to answer three questions:

1. Where are delivery promises being missed?
2. Which hubs and routes contribute most to late deliveries?
3. Are data-quality problems affecting the operational picture?

## Why I built it

I wanted one project that combines the work normally split across data engineering,
analytics engineering, and BI: generating source data, building reliable transformations,
testing the result, defining business measures, and presenting the findings.

The design reflects recurring requirements in German data roles: SQL, Python, ETL/ELT,
dimensional modelling, data-quality testing, documentation, CI, and Power BI. I kept the
dependency set small so the complete pipeline can be run locally before any cloud setup is
needed.

## Boundaries

- All organisations, shipments, events, and business outcomes are fictional.
- This is a personal project, not employment, consulting, or production experience.
- Local execution does not demonstrate Fabric service behaviour or Delta transaction
  semantics.
- The repository contains no generated bulk data, credentials, tokens, or tenant identifiers.
- Predictive delay modelling is outside the current scope; the focus is reliable descriptive
  analytics.
