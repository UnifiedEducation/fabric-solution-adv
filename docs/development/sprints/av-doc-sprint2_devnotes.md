# Development notes from Sprint 2

A summary of development efforts/ changes made to the repo, and the overall solution in Sprint 2. 
## Overview of development work done this Sprint

#### Azure configuration
- Created a new Azure subscription for the project (and registered the Microsoft.Fabric provider)
- Created a resource group for AV01 solution items.
- Created a Service Principal in Entra ID, created a Client Secret, and applied API permissions. Also made sure the relevant Service Principal Admin Settings have been configured in Fabric Admin Portal.
- Created three Entra ID security groups as per the brief, and added users to groups. Noting the Object IDs for each group, which is used in the workspace permissions assignment automation.

#### GitHub/ development setup
- Created a new repository, based on the Starter project repository template
- Cloned the repository locally
- Created a Python virtual environment to enable a local development workflow.
- Added the requirements.txt file, and installed the requirements into the virtual environment.
- Created the .env file from the .env.template file, and then populated it with variables- for local development. *Important: always ensure your .env file is part of your .gitignore file, so that it never gets committed to the Git repository*
- Added the GitHub secrets into the Repository Secrets (the same as the .env file variables)

#### GitHub automation development
In this Sprint, we added a few new files to the Solution: 

**.github/workflows/deploy-solution-from-template.yml** - this is the GitHub Action (based on the `starter-workflow.yml` in the starter repo). It's mostly the same as the starter workflow, but at the end I call the `generate_solution_from_yml_simple.py` file. All logic has been moved to that Python file. 

**config/generate_solution_from_yml_simple.py** - for now, I've put all the logic for generating Fabric infrastructure into a single Python file. This file contains a number of functions that are used and orchestrated via the `main()` function. This function reads the YML template file (see below), and creates Fabric Infrastructure defined by the template file. 

**config/templates/v01/v01-template.yml** - I developed a YML template file that declaritively defines the Fabric Infrastructure that we want to build. 

**config/requirements.txt** - added a requirements.txt file that defines which Python packages we need to install for the code to run. 


## Decisions made during this Sprint that affect design/ architecture
- Changed the naming convention slightly - I went with all lower-case (so that the Capacities names match the rest of the infrastructure), also switched to using hyphens, rather than underscores to keep in alignment with Azure infrastructure. 
- I have further developed the CI/CD strategy, and updated the brief to reflect the changes.  
- I've updated the high-level architecture diagram to reflect these changes. 

## Tips/ key learnings from this Sprint
- Use security groups to make workspace-level access 
- Use service principals for automation (this means the service principal will become the owner of anything it creates, rather than a user. )
- Keep GitHub Actions readable, create separate Python files/ modules (we'll modularize the code in future sprints)
- Develop Fabric Infrastructure using a infrastructure-as-code approach. Defining a YML template from which you'll generate the Fabric Infrastructure. This will be expanded in the future Highly scalable, keeps codebase clean, and checks into version control. 
- Use virtualenv for installing python packages locally, for repeatability
- Use .env to store environment variables locally, and ensured that the .env file is in the .gitignore file. 
- Manage and maintain a .gitignore file in your repository, and ensure WITH EVERY COMMIT that nothing secretive is committed into your repository (even if it's private!). 


## CI/CD list of outstanding tasks

The following tasks are required to complete the CI/CD, but we were not developed during this Sprint. We'll further develop the CI/CD automations as we move further into the project.

- Improve code quality (in this sprint the focus was on readability, and logic) - modularize, add error-handling, more documentation, 
- Develop a GitHub Action to generate a feature workspace, when a new feature branch is created in the workspace. 
- Develop a GitHub Action for syncing the contents of the main branch to the 
- Develop GitHub Action(s) for the Build & Release to TEST and PROD environments. 
- Develop a GitHub Action to generate an entirely new solution on a new trunk branch, based on a template. 
- Expand the YML template file to include other Fabric infrastructure (pipelines, notebooks, datastores, testing, logging) etc. 
