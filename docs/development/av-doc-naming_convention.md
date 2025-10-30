#### Overall naming structure
The general naming convention is described below:

aa-bb-cc-dd
aa = Item Type
bb = Project Code
cc = Deployment Stage
dd = Short Description

#### Part 1: Item Types:

- fc: Fabric Capacity
- sg: Entra ID Security Group
- _More will be added to this list as we progress the project & implementation_
- *Note: for Workspace naming, they will not include the i* 

#### Part 2: Project Code:

- av01: AV - related to the Advanced-level project. 01 - relates to the Architectural version number of the IAC template that deployed the solution.
- av02: therefore, AV02 will be solution deployed from a second IAC template (as part of the Adv). The second version of the architecture.
- It is assumed the two-digit number code (giving room for 99 architecture versions), will be sufficient 'headroom' in the naming convention, but this will be confirmed with the client.

#### Part 3: Deployment Stage:

- dev: Development stage
- test: Test stage
- prod: Production stage
- _Note: the deployment stage is optional, and only needs to be applied if the item goes through a deployment process. For example, in our architecture Security Groups will not be deployed, and so_

#### Part 4: Short Description:

- One or two word description to give people a better idea about the item.

#### Examples
Some examples, including their plain language description:

- **sg-av-analysts** - Entra ID Security Group, for the INT project, for Analysts
- **fcav01devengineering** - a Fabric Capacity, for the Advanced project (first Version), for use in Data Engineering-related workspaces, at the DEV deployment stage. Note: Fabric Capacities cannot contain hyphens, so they are somewhat of an exception to the overall rule.
- **av01-test-processing** - a Fabric Workspace, for the Advanced project (V01 of the Architecture), TEST Deployment stage, and inside will be Processing items. Because it's a workspace, the Item Type is negated. 