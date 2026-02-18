# fabric-solution-adv

[![Orchestrate Daily Refresh](https://github.com/UnifiedEducation/fabric-solution-adv/actions/workflows/orchestrate-daily-refresh.yml/badge.svg)](https://github.com/UnifiedEducation/fabric-solution-adv/actions/workflows/orchestrate-daily-refresh.yml)
[![Sync to DEV Environment](https://github.com/UnifiedEducation/fabric-solution-adv/actions/workflows/sync_git_content_to_fabric.yml/badge.svg)](https://github.com/UnifiedEducation/fabric-solution-adv/actions/workflows/sync_git_content_to_fabric.yml)
[![Deploy to TEST](https://github.com/UnifiedEducation/fabric-solution-adv/actions/workflows/deploy-to-test.yml/badge.svg)](https://github.com/UnifiedEducation/fabric-solution-adv/actions/workflows/deploy-to-test.yml)
[![Deploy to PROD](https://github.com/UnifiedEducation/fabric-solution-adv/actions/workflows/deploy-to-prod.yml/badge.svg)](https://github.com/UnifiedEducation/fabric-solution-adv/actions/workflows/deploy-to-prod.yml)

The Advanced real-world Fabric Data Platform built at [Fabric Dojo](https://skool.com/fabricdojo/about).

_'Advanced'_ - meaning it's designed for teams with Data Platform experience looking to implement enterprise-grade patterns including Infrastructure-as-Code, metadata-driven pipelines, and comprehensive CI/CD automation. If you're transitioning from Power BI-centric architectures, the [Intermediate-level project](https://github.com/UnifiedEducation/fabric-solution-int) will be best for you.

This solution ingests YouTube API data through a Bronze/Silver/Gold lakehouse architecture, with metadata-driven PySpark notebooks, Great Expectations validation, SQL-based logging, and fully automated GitHub Actions orchestration with capacity management.

## Repository Structure

```
solution/
  datastores/              Lakehouses (Bronze, Silver, Gold)
  processing/
    notebooks/             PySpark ELTL notebooks (ingest, load, clean, model, validate)
    orchestration/         Orchestration notebook, SQL metadata database
    env-av01-dataeng/      Spark environment with Great Expectations
    vl-av01-variables/     Variable Library (DEV/TEST/PROD configs)
  consumption/             Consumption layer
config/                    Infrastructure-as-Code scripts & templates
docs/                      Architecture docs, naming conventions, design decisions
.github/workflows/         CI/CD automation (6 active workflows)
```

## Quick Start

1. Clone the repo and copy `.env.template` to `.env`, filling in your Service Principal credentials, Azure subscription/tenant IDs, and GitHub PAT
2. Complete the IAC template at `config/templates/v01/v01-template.yml` with your Entra ID security group Object IDs, Azure resource group, region, and capacity SKU
3. Configure GitHub **Secrets** (`SPN_CLIENT_ID`, `SPN_CLIENT_SECRET`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `SPN_OBJECT_ID`, `GH_PAT`) and **Variables** (workspace IDs and notebook IDs for TEST/PROD) in your repository settings
4. Create an Azure Key Vault and store your YouTube API key, then grant your Service Principal access to the vault
5. Run the `deploy-solution-from-template` workflow to deploy the infrastructure
6. Connect your DEV workspaces to the repo via Fabric Git Integration
7. See [PRJ108 - Complete Solution Walkthrough](https://www.skool.com/fabricdojo/classroom/41bb7437?md=f68ae132771a401cba5759ae34bf5a7e) for the full walkthrough

## Video Walkthroughs

| Module | Topic | Links |
|--------|-------|-------|
| PRJ101 | Capacity & Workspace Design | [Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=4f6b678e31eb4d299f721aaf13d43fab) / [Sprint 1](https://www.skool.com/fabricdojo/classroom/41bb7437?md=3d8ff744317044a88a3713a630cee964) |
| PRJ102 | CI/CD & Automation Strategy | [Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=156bab2cf2004cc1866ef44e77eba536) / [Sprint 2](https://www.skool.com/fabricdojo/classroom/41bb7437?md=05574946f9594f6aa556b1d28edd3377) |
| PRJ103 | Further CI/CD Automation | [Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=c71901f6fb0a49e6824adc99e4f86ca8) / [Sprint 3](https://www.skool.com/fabricdojo/classroom/41bb7437?md=6d7da7d919d44cdc9d4864641e8cb972) |
| PRJ104 | Initial Data Extraction (YouTube API) | [Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=ce1641114acf4ff688ea38c98fff7903) / [Sprint 4](https://www.skool.com/fabricdojo/classroom/41bb7437?md=ebbcf3db2e4742d1b8f304d919acdfef) |
| PRJ105 | Lakehouse Development | [Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=86921850620d4fc9b53c94723be515f3) / [Sprint 5](https://www.skool.com/fabricdojo/classroom/41bb7437?md=da4d4472653040e08722ffbc44d7183e) |
| PRJ106 | Metadata Framework & Implementation | [Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=a0e7162df34048889836a0309868b487) / [Sprint 6](https://www.skool.com/fabricdojo/classroom/41bb7437?md=3d8b83c9c67241d9a60bae5c5bbd6ed6) |
| PRJ107 | Prepare for Go-LIVE | [Requirements](https://www.skool.com/fabricdojo/classroom/41bb7437?md=ac3e8938d78f449ab095ce1d6a73fc12) |
| PRJ108 | Complete Solution Walkthrough | [Video](https://www.skool.com/fabricdojo/classroom/41bb7437?md=f68ae132771a401cba5759ae34bf5a7e) |
