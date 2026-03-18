# Framework Change Areas

This document complements `docs/framework-modules.md` by focusing specifically on **where changes are most likely to belong** inside the shared framework.

> Note: the guidance below is an engineering interpretation based on the current module structure, dependencies, and resource layout visible in this checkout.

---

## Change sensitivity by module

### `common`
- **Change possibility:** high.
- **Risk level:** moderate to high.
- **Why:** shared resources from `common` appear to feed multiple downstream modules, so even small edits can affect generation, ETL packaging, and release outputs.
- **Use it when:** the behavior or resource is genuinely shared across multiple datamarts or framework modules.

### `generator/api`
- **Change possibility:** medium to high.
- **Risk level:** medium to high.
- **Why:** it defines reusable contracts and builders used by implementation code.
- **Use it when:** the abstraction itself must evolve, not just one concrete workflow-generation behavior.

### `generator/impl`
- **Change possibility:** very high.
- **Risk level:** high.
- **Why:** this is the main behavior layer where generation logic, config interpretation, XML emission, and production transformations appear to live.
- **Use it when:** framework behavior must change.

### `etl`
- **Change possibility:** high.
- **Risk level:** medium when isolated, higher when shared or structural.
- **Why:** it stores source/domain-specific workflow resources, templates, runners, and entity metadata.
- **Use it when:** a workflow/resource change is source-specific rather than framework-wide.

### `ctl-rest`
- **Change possibility:** medium.
- **Risk level:** high integration sensitivity.
- **Why:** the code depends on external CTL contracts and payloads.
- **Use it when:** CTL endpoints, DTOs, or API-version behavior must change.

### `ctl-reporter`
- **Change possibility:** medium.
- **Risk level:** low to medium.
- **Why:** this area appears more isolated and presentation/reporting-oriented.
- **Use it when:** the way CTL data is presented or exported needs to change.

### `release`
- **Change possibility:** medium.
- **Risk level:** medium to high.
- **Why:** release packaging controls what actually gets shipped and installed.
- **Use it when:** the issue is packaging, distribution, install metadata, or release validation.

### `doc`
- **Change possibility:** high.
- **Risk level:** low.
- **Why:** it is documentation/support material.
- **Use it when:** the task is explanatory rather than behavioral.

---

## Blast-radius view

A practical way to think about the framework is by blast radius:

- **Lowest blast radius:** `doc`, many `ctl-reporter` changes, source-specific `etl` resource changes.
- **Medium blast radius:** `ctl-rest`, ETL parent/module wiring, release packaging rules.
- **Highest blast radius:** `common`, `generator/api`, `generator/impl`.

---

## Where a change probably belongs

| Need to change | Most likely place |
|---|---|
| Shared workflow resource used across many modules | `common` |
| Generation abstraction or reusable builder contract | `generator/api` |
| Actual workflow/config generation behavior | `generator/impl` |
| Source-specific Oozie templates, runners, or workflow resources | `etl` |
| CTL DTO/API integration | `ctl-rest` |
| CTL-based report output | `ctl-reporter` |
| Packaging/distribution/install metadata | `release` |
| Framework explanatory documentation | `doc` |

---

## Where a change probably should not go

To reduce coupling:

- **Business-specific SQL/config changes** should usually stay out of `personalization-internal-sources` and instead go into business packages such as `custom_rb_evk` or `custom_rb_vnv`.
- **One-off workflow tweaks** should not go into `common` unless multiple domains truly reuse them.
- **Reporting-only formatting changes** should not require edits to `ctl-rest` unless the CTL contract itself changes.
- **Packaging-only fixes** should not be implemented as generator logic changes when a `release` change is enough.

---

## Practical guidance

If the change is:
- **framework-wide behavior**, prefer `generator/impl`;
- **source/domain-specific resources**, prefer `etl`;
- **shared reusable assets**, prefer `common`;
- **external CTL communication**, prefer `ctl-rest`;
- **reporting output**, prefer `ctl-reporter`;
- **what gets shipped**, prefer `release`.
