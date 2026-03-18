# Repository Overview

This repository contains three top-level project folders that serve different roles in the same general data-platform ecosystem:

1. `personalization-internal-sources` – the shared internal framework/platform.
2. `custom_rb_evk` – a business-specific ETL/datamart package for the `evk` domain.
3. `custom_rb_vnv` – a business-specific ETL/datamart package for the `vnv` domain.

This document is intentionally high level. It explains what each folder is for and what kinds of files live there, without documenting every file one by one.

---

## 1. `personalization-internal-sources`

### Purpose
This is the framework side of the repository. It contains the shared Maven modules, workflow-generation logic, CTL integration code, ETL resource packages, and release assembly logic that support the custom projects.

### Main contents
- `pom.xml` – parent Maven build for the internal source tree.
- `datamart/` – the main multi-module framework project.
- `settings-alpha.xml` – Maven/environment profile settings.
- `masspers-idea-config.xml`, `masspers-internal-dashboard.json` – local IDE/dashboard support files.

### Main subfolders
- `datamart/common` – shared resources and reusable common assets.
- `datamart/generator/api` – workflow-generation APIs, builders, and interfaces.
- `datamart/generator/impl` – concrete workflow/config generation logic.
- `datamart/etl` – ETL resource modules and source-specific workflow packages.
- `datamart/ctl-rest` – CTL REST client code and DTO models.
- `datamart/ctl-reporter` – reporting tools built on top of CTL data.
- `datamart/release` – packaging, installation, and release assembly logic.
- `datamart/doc` – framework documentation/support materials.

### Typical file purposes
- `pom.xml` files define module structure, dependencies, and packaging.
- `src/main/java/...` contains framework logic and clients.
- `src/test/java/...` contains module tests.
- `src/main/resources/...` contains workflow XML, templates, scripts, and packaged resource files.
- `src/test/resources/...` contains fixtures and expected outputs.

### Summary
If you need the shared platform/framework logic, this is the folder to inspect first.

---

## 2. `custom_rb_evk`

### Purpose
This folder is a deployable custom ETL/datamart package for the `evk` domain. It contains SQL, workflow configuration, and runtime metadata needed to run domain-specific flows on top of the shared framework.

### Main contents
- `pom.xml` – Maven descriptor for the custom package.
- `etl/` – DDL and workflow-specific ETL assets.
- `external/` – extension points for custom functions/UDFs.
- `resources/` – environment, CTL, security, and DevOps configuration.

### Main subfolders
- `etl/ddl` – SQL definitions for external staging tables.
- `etl/workflows` – business workflows split into `conf/` JSON configs and `dml/threads/` SQL logic.
- `external/functions`, `external/udfs` – paths reserved for extensions.
- `resources/mart.yml` – stand-specific mart and Spark settings.
- `resources/ctl/ctl.yml` – CTL entity/workflow registration metadata.
- `resources/external/external.conf` – external function/UDF loader config.
- `resources/secman/` – security/secret management config.
- `resources/devops/` – CI/CD and deployment metadata.
- `resources/docs/` – supporting business-side documentation artifacts.

### Typical file purposes
- Workflow `.json` files describe execution stages and dependencies.
- Thread `.sql` files implement the real load/truncate logic.
- DDL `.sql` files define external staging tables.
- `.yml` and `.conf` files define runtime, deployment, CTL, and security settings.

### Summary
This folder is where the `evk` business logic is configured and deployed.

---

## 3. `custom_rb_vnv`

### Purpose
This folder is another deployable custom ETL/datamart package, but for the `vnv` domain. Structurally it is very similar to `custom_rb_evk`, with its own workflows, SQL, and environment settings.

### Main contents
- `pom.xml` – Maven descriptor for the package.
- `etl/` – DDL and workflow implementation files.
- `external/` – extension points for custom functions/UDFs.
- `resources/` – runtime, CTL, security, and DevOps configuration.
- `custom_rb_vnv.iml` – IDE metadata.
- `readme.md` – local template note from the project source itself.

### Main subfolders
- `etl/ddl` – SQL for external staging tables and source schemas.
- `etl/workflows` – workflow packages for fact/reference loads.
- `external/functions`, `external/udfs` – reserved extension locations.
- `resources/mart.yml` – environment and Spark settings.
- `resources/ctl/ctl.yml` – CTL registration and metadata.
- `resources/external/external.conf` – function/UDF loader config.
- `resources/devops/` – deployment metadata.
- `resources/secman/` – security config.

### Typical file purposes
- `etl/ddl/*.sql` creates required external tables.
- `etl/workflows/*/conf/*.json` describes workflow execution.
- `etl/workflows/*/dml/threads/*.sql` contains transformation/truncation SQL.
- `resources/**/*.yml` and `resources/**/*.conf` store runtime and deployment configuration.

### Summary
This folder is where the `vnv` domain’s business ETL package is defined.

---

## Quick comparison

| Folder | Main role | Main kinds of files |
|---|---|---|
| `personalization-internal-sources` | Shared platform/framework | Java modules, resources, templates, release assets, tests |
| `custom_rb_evk` | Custom ETL package for `evk` | SQL DDL, workflow JSON, SQL threads, CTL/mart/security config |
| `custom_rb_vnv` | Custom ETL package for `vnv` | SQL DDL, workflow JSON, SQL threads, CTL/mart/security config |

## Reading guide

- Start with `personalization-internal-sources` if you need to understand the shared framework or workflow generation logic.
- Start with `custom_rb_evk/etl/workflows` if you need the `evk` business flow definitions.
- Start with `custom_rb_vnv/etl/workflows` if you need the `vnv` business flow definitions.
- Check each project's `resources/` folder for deployment, CTL, security, and environment settings.
