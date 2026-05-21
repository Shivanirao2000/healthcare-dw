# Power BI Docs

Store Power BI report definitions, dataset schemas, and DAX measure libraries here.

## Conventions

- One folder per report domain (e.g. `claims/`, `quality_measures/`, `utilization/`)
- Include a `schema.md` per domain describing tables and relationships
- DAX measures live in `measures/<domain>.dax`
- Do **not** commit `.pbix` binaries — export as `.pbit` (template) for version control
