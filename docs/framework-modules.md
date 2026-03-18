# Framework Modules

This document focuses on the shared framework inside `personalization-internal-sources/datamart`.

It describes the main framework modules, what they appear to do, and how they relate to one another.

> Note: these module descriptions are an engineering interpretation based on the current module structure, dependencies, package names, and resource layouts visible in the repository.

---

## Framework at a glance

The `datamart` project is a multi-module Maven build with these main framework areas:

- `common`
- `generator/api`
- `generator/impl`
- `etl`
- `ctl-rest`
- `ctl-reporter`
- `release`
- `doc`

A useful mental model is:

1. `common` supplies shared assets.
2. `generator/api` defines generation contracts and builders.
3. `generator/impl` turns configs/resources into generated workflow artifacts.
4. `etl` provides source/domain-specific packaged workflow resources.
5. `ctl-rest` talks to the CTL service.
6. `ctl-reporter` turns CTL data into reports.
7. `release` assembles distributable outputs.

---

## Module-by-module description

### 1. `common`

#### What it does
`common` looks like the shared base layer for the rest of the framework. It packages reusable resources and utilities that other modules consume.

From the repository contents, this module especially appears to hold:
- shared Oozie schemas/XSDs,
- common utility workflow resources,
- reusable production-side scripts and support files,
- packaged resource bundles produced during build.

#### Typical changes that belong here
- adding or updating shared workflow templates/resources,
- changing common shell helpers used by multiple generated workflows,
- adjusting reusable Oozie-related schemas or packaged static resources,
- updating shared assembly/resource packaging behavior.

---

### 2. `generator/api`

#### What it does
`generator/api` appears to define the workflow-generation contracts and reusable builders.

Based on class names, it contains:
- workflow generator interfaces,
- action/build parameter mappers,
- Oozie action/property builders,
- file writing / workflow writing abstractions,
- property name resolution and resource generation helpers.

This module looks like the abstraction layer that describes *how generation is modeled* rather than *which exact workflows are produced*.

#### Typical changes that belong here
- introducing a new builder abstraction,
- extending parameter models for generated actions,
- adding new generic generation interfaces,
- updating shared generation contracts used by multiple implementations.

---

### 3. `generator/impl`

#### What it does
`generator/impl` looks like the operational heart of the framework. It contains the concrete logic that reads config, fills parameters, generates workflow artifacts, copies resources, transforms XML, and builds production-ready workflow outputs.

The package structure suggests several responsibilities:
- config reading and parameter setting,
- workflow generation runners,
- workflow file copying and production transformation,
- replica/history/datamart runtime generation,
- workflow scheduling and CTL workflow generation,
- production/test generation helpers.

#### Typical changes that belong here
- changing how workflow XML is generated,
- adding support for a new workflow pattern,
- adjusting config parsing and parameter filling,
- changing production transformation/copying rules,
- adding support for a new runtime/history/replica generation flow,
- tuning how CTL-related workflow artifacts are emitted.

---

### 4. `etl`

#### What it does
`etl` is the resource/package side of the framework for concrete datamart/source implementations.

The parent ETL module lists many source/domain modules, while the current checkout visibly contains at least `erib` and `greenplum`. The resource layout shows:
- packaged workflow XML,
- shell runners,
- Spark/system property templates,
- JAAS/security configs,
- entity metadata YAML.

This suggests the ETL layer stores the *ready-to-package workflow resources* used for specific source systems or domains.

#### Typical changes that belong here
- adding or updating a concrete workflow resource,
- changing templates for one source/domain,
- adjusting per-domain shell scripts or security configs,
- adding a new ETL module/resource package,
- updating entity metadata YAML for a particular workflow family.

---

### 5. `ctl-rest`

#### What it does
`ctl-rest` is the framework’s CTL integration client. It packages:
- REST API interfaces,
- DTO/request/response objects,
- loading/status/filter models,
- API-versioned client logic,
- Retrofit/OkHttp execution helpers.

This module is the adapter layer between the framework and the external CTL service.

#### Typical changes that belong here
- adding support for a new CTL endpoint,
- changing DTOs when CTL payloads evolve,
- updating API version handling,
- improving request execution, error handling, or filtering logic.

---

### 6. `ctl-reporter`

#### What it does
`ctl-reporter` appears to be a small reporting application built on top of `ctl-rest` and `datamart-common`.

It contains:
- CTL information readers,
- report model classes,
- CSV report builders,
- reporter entry points / executables,
- tests around report formatting and reading behavior.

#### Typical changes that belong here
- adding a new report format,
- changing CSV/report output structure,
- modifying how CTL data is presented to users,
- adding new reporting commands or report types.

---

### 7. `release`

#### What it does
`release` looks like the assembly/distribution layer. It pulls together framework resources and ETL artifacts into installable/releasable outputs.

The files suggest responsibilities such as:
- packaging distributions,
- carrying install metadata (`dependencies.json`, `devops.json`, dataflow lists),
- storing release-time workflow configs,
- validating packaged datamart properties in tests.

#### Typical changes that belong here
- adjusting packaging structure,
- adding/removing release dependencies,
- changing release-time config bundles,
- updating installation metadata,
- adding validation rules for release artifacts.

---

### 8. `doc`

#### What it does
`doc` appears to be the documentation/support area inside the framework project.

#### Typical changes that belong here
- explanatory docs,
- framework usage notes,
- operational support references.

---

## Final takeaway

The framework appears to separate **shared assets**, **generation logic**, **integration clients**, **reporting**, **source-specific ETL resources**, and **release packaging** reasonably well.

That makes it easier to choose the right layer for future changes, especially when separating framework-wide behavior from source-specific or business-specific changes.
