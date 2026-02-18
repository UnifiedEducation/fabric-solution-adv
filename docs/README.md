# Solution Documentation

This page covers the key aspects of the Advanced Fabric Data Platform. For full video walkthroughs, deep-dive explanations, and build-along sprints, see the [Fabric Dojo](https://skool.com/fabricdojo) community (premium).

## Architecture

The solution uses a 9-workspace layout across 3 areas (Datastores, Processing, Consumption) and 3 environments (DEV, TEST, PROD). Each architecture version has 6 dedicated Fabric Capacities with automated pause/resume to minimise cost. Ephemeral feature workspaces are created via GitHub Actions for isolated development.

| Topic                       | Training                                                                                                                                                                                                             |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Capacity & workspace design | [PRJ101 Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=4f6b678e31eb4d299f721aaf13d43fab) / [Sprint 1](https://www.skool.com/fabricdojo/classroom/41bb7437?md=3d8ff744317044a88a3713a630cee964) |

## CI/CD & Automation

All Fabric infrastructure is defined declaratively as Infrastructure-as-Code (IAC) templates, deployed via the `fabric-cicd` Python library and GitHub Actions. Git integration connects each workspace area to a folder in this repo (`solution/datastores/`, `solution/processing/`, `solution/consumption/`). A Variable Library (`vl-av01-variables`) parameterises all environment-specific values (workspace IDs, notebook IDs, lakehouse names) with value sets for DEV, TEST, and PROD.

Capacity automation is built into all workflows - PROD and TEST capacities are resumed before work and suspended afterwards (with `always()` clauses to guarantee suspension even on failure). Feature workspace creation is fully automated via a dedicated GitHub Action.

| Topic                       | Training                                                                                                                                                                                                             |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| CI/CD & automation strategy | [PRJ102 Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=156bab2cf2004cc1866ef44e77eba536) / [Sprint 2](https://www.skool.com/fabricdojo/classroom/41bb7437?md=05574946f9594f6aa556b1d28edd3377) |
| Further CI/CD automation    | [PRJ103 Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=c71901f6fb0a49e6824adc99e4f86ca8) / [Sprint 3](https://www.skool.com/fabricdojo/classroom/41bb7437?md=6d7da7d919d44cdc9d4864641e8cb972) |

## Data Ingestion

YouTube Data API v3 is the source. The ingest notebook (`nb-av01-0-ingest-api`) pulls channel info, playlist items, and video statistics, writing raw JSON to the Bronze Lakehouse Files area. API keys are stored in Azure Key Vault.

| Topic                                 | Training                                                                                                                                                                                                             |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Initial data extraction (YouTube API) | [PRJ104 Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=ce1641114acf4ff688ea38c98fff7903) / [Sprint 4](https://www.skool.com/fabricdojo/classroom/41bb7437?md=ebbcf3db2e4742d1b8f304d919acdfef) |

## Lakehouse Development

Three lakehouses follow the medallion architecture: Bronze (raw), Silver (cleaned), Gold (modelled). A SQL Database (`fs-av01-admin`) provides the metadata and logging layer with 12 tables across `instructions`, `log`, and `metadata` schemas - driving the metadata-driven approach to ingestion, transformation, and validation.

| Topic                                   | Training                                                                                                                                                                                                             |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Lakehouse structure & medallion pattern | [PRJ105 Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=86921850620d4fc9b53c94723be515f3) / [Sprint 5](https://www.skool.com/fabricdojo/classroom/41bb7437?md=da4d4472653040e08722ffbc44d7183e) |

## Metadata Framework & Data Transformation

The entire ELTL pipeline is metadata-driven: ingestion instructions, column mappings, transformation rules, and validation expectations are all stored in the SQL Database and resolved at runtime. PySpark notebooks move data through the medallion layers: Load (JSON to Bronze tables), Clean (null filtering, deduplication via Window functions to Silver), and Model (dimensional modelling with surrogate key management to Gold). A shared generic functions library (`nb-av01-generic-functions`) centralises reusable transform and utility functions.

| Topic                               | Training                                                                                                                                                                                                             |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Metadata framework & implementation | [PRJ106 Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=a0e7162df34048889836a0309868b487) / [Sprint 6](https://www.skool.com/fabricdojo/classroom/41bb7437?md=3d8b83c9c67241d9a60bae5c5bbd6ed6) |

## Orchestration & Data Validation

The orchestration notebook (`nb-av01-run`) executes all 5 ELTL notebooks sequentially. Great Expectations validates Gold layer tables using expectations defined in the `metadata.expectation_store`, with per-expectation results written to `log.validation_results` for trend analysis. GitHub Actions triggers the daily refresh on a cron schedule, automatically resuming and suspending PROD capacity.

| Topic                          | Training                                                                                                       |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| Production readiness & go-live | [PRJ107 Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=ac3e8938d78f449ab095ce1d6a73fc12) |

## Naming Convention

All Fabric items (where possible) follow the pattern: `[type]-[project]-[stage]-[description]`

| Prefix | Item Type        |
| ------ | ---------------- |
| `nb-`  | Notebook         |
| `pp-`  | Data Pipeline    |
| `lh_`  | Lakehouse        |
| `vl-`  | Variable Library |
| `env-` | Environment      |
| `fs-`  | SQL Database     |

Project code: `av01` (Advanced, Architecture Version 01). Environments: `dev`, `test`, `prod`.

Project code: Examples: `nb-av01-0-ingest-api`, `lh_av01_bronze`, `pp-av01-run`.
