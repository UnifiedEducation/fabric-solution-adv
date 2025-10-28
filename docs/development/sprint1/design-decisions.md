> ℹ️ This document provides justification for architectural decisions, in response to PRJ101 ⬛ Capacity & Workspace Design (Sprint 1), as part of the Advanced-Level Fabric Project, in [Fabric Dojo](https://skool.com/fabricdojo/about).

## Specifics on how the design meets each specific client requirement

#### Infrastructure-as-Code

> **[RB001] All Fabric infrastructure in your solution must be defined declaratively, as an Infrastructure as Code (IAC) template. Implemented using the Fabric CLI, deployed using GitHub Actions.**

- Careful consideration has been given to the client requirements for a infrastructure-as-code deployment methodology on the project. In Sprint 2 of the project, development of the template will begin, and it will be developed throughout the first stages of the project.
- The pros and cons of the IAC deployment method will be made clear the client:
  - Pros: ability to run parallel architectures to measure the efficiency of new features/ ETL methods (as requested), it will also form a major part of the Business Continuity and Disaster Recovery (BCDR) strategy.
  - Cons: it will require more advanced skills to edit and maintain the Fabric CLI scripts & GitHub Actions. It could also slow down the initial development speed, as it takes longer to configure a template that just a build a single solution.

#### Capacities

> **[RB002]** **For each 'Version' of an architecture (represented by a specific IAC template), six separate capacities are required** > **[RB003]** **The creation of these Capacities will be done through an IAC template (Fabric CLI + GitHub Actions)** > **[RB004] Detailed capacity automation requirements to minimize Capacity spend**

- As requested, the capacity design accounts for six Fabric Capacities, per Version, and it is understood that capacity creation (and then automation, see details below) will be managed entirely through the IAC Template (GitHub Actions).
- Workspace assignment to Capacity will be done through the IAC template as the requirements.
- Note: Fabric Capacities must be lower-case and not contain hyphens, so that restriction is taken into account in the naming convention.
- **Capacity Automation Strategy**: the following Capacity Automation Strategy has been designed to meet the client requirements on capacity automation and minimizing operational cost of the data platform:
  - To meet the requirements for the Production data engineering capacity `fcav0xprodengineering` - to have it active during the daily loading process, and then deactivated - it is proposed that we will write this into a GitHub Action, and the daily loading process be triggered through the GitHub Action. Pseudocode for the GitHub Action will look like this:
    - Trigger: Schedule trigger (based on CRON schedule, to be provided by client).
    - Turn on Capacity
    - Execute Daily ETL Pipeline: we will orchestrate the pipelines in such a way that a single pipeline orchestrates the entire process. This master pipeline will be executed via the REST API (call in the GitHub Action).
    - Turn off Capacity
  - Next, the TEST capacities `fcav0xtestengineering` and `fcav0xtestconsumption` - to meet the requirement to have the capacities Active during automated testing, and then paused, it is proposed that this be included within the GitHub Action that runs the automated testing. Pseudocode for the GitHub Action will look like this:
    - Trigger: When a pull request is accepted into the DEV branch
    - TEST capacity is resumed
    - Content is deployed into TEST branch
    - Automated tested is run (including logging results)
    - TEST Capacity is paused.
  - For `fcav0xdevengineering`, `fcav0xdevconsumption` and `fcav0xprodconsumption` - no automation required; these capacities will be paused and resumed manually within the Azure Portal by developers.

#### Workspaces

> **[RB005] Your IAC template (and therefore, your Fabric Solution) must declare separate areas for Processing, Data Stores & Consumption**: & \*\*[RB006] Each of the workspaces above will have a DEV, TEST and PRODUCTION version (so 9 in total)

- The design accounts for 9 separate workspaces, to separate Processing, Data Stores, and Consumption workloads, through three different Deployment stages (DEV, TEST and PROD) - this results in 9 workspaces in total. These workspaces will be created as part of the IAC deployment template.
- The high-level architecture diagram does not include ephemeral, "feature" workspaces, which will be created for development of new features. The creation of these workspaces will be done through a GitHub Action automation, to be described in more detail, in the next Sprint.


#### Access

> **[RB007] - Access will be granted automatically, through the IAC template, it will automatically provide access at the Workspace Level, through three Entra ID security groups: `Engineers`, `Analysts`, `Consumers`**

- As requested, access-control will primarily be given at the workspace-level, and Entra ID security groups will be added to the Workspaces.
  - However, the client will be notified on the Auditability tradeoffs of adopting such an approach. Entra ID Security groups make access control _easier_, but it you need to know exactly who was added to a group when, this can become tricky, and will rely on extracting regular security group membership lists.
- However, the client will be informed that if access control requirements change in the future (for example, more granular permissions are required, or the requirement to implement OneLake Security for RLS/ CLS), this approach will need to be thoroughly designed & planned, before implementation.
- It should also be noted that access will be controlled & automated, as part of the IAC deployment approach. It is recommended that this also forms part of the detailed logging & monitoring system that will be built, to ensure that access control is correctly implemented after new deployments are made.

#### Naming Conventions

> **[RB008] The client has requested a solid naming convention strategy for: capacities, workspaces, deployment pipelines.**

The following naming conventions have been suggested for client review, based on the items found in the High-Level Architecture Diagram are described below:

The general naming convention is described below:
AA_BB_CC_DD
AA = Item Type
BB = Project Code
CC = Deployment Stage
DD = Short Description

Part 1: Item Types:

- FC: Fabric Capacity
- WS: Workspace
- SG: Entra ID Security Group
- _More will be added to this list as we progress the project & implementation_

Part 2: Project Code:

- AV01: AV - related to the Advanced-level project. 01 - relates to the Architectural version number of the IAC template that deployed the solution.
- AV02: therefore, AV02 will be solution deployed from a second IAC template (as part of the Adv). The second version of the architecture.
- It is assumed the two-digit number code (giving room for 99 architecture versions), will be sufficient 'headroom' in the naming convention, but this will be confirmed with the client.

Part 3: Deployment Stage:

- DEV: Development stage
- TEST: Test stage
- PROD: Production stage
- _Note: the deployment stage is optional, and only needs to be applied if the item goes through a deployment process. For example, in our architecture Security Groups will not be deployed, and so do not need a Deployment Stage in the name_

Part 4: Short Description:

- One or two word description to give people a better idea about the item.

Some examples, including their plain language description:

- **SG_INT_Analysts** - Entra ID Security Group, for the INT project, for Analysts
- **fcav01devengineering** - a Fabric Capacity, for the Advanced project (first Version), for use in Data Engineering-related workspaces, at the DEV deployment stage. Note: Fabric Capacities must be lower-case and not contain hyphens, so they are somewhat of an exception to the overall rule.
- **WS_AV01_TEST_Processing** - a Fabric Workspace, for the Advanced project (V01 of the Architecture), TEST Deployment stage, and inside will be Processing items.
