#!/usr/bin/env python3
"""
Script to create an Argo Workflow Template from a parameterized Jupyter notebook.

This script:
1. Clones a git repository
2. Finds and parses a Jupyter notebook
3. Extracts parameters from the notebook's 'parameters' cell (Papermill convention)
4. Creates and registers an Argo Workflow Template using Hera
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, List

import nbformat
from git import Repo
from hera.workflows import (
    WorkflowTemplate,
    Container,
    Parameter,
    GitArtifact,
    ArtifactoryArtifact
)
from hera.shared import global_config

# Load environment variables from .env file (optional)
try:
    import dotenv  # type: ignore
    dotenv.load_dotenv()
except ImportError:
    # dotenv not installed, will rely on system environment variables
    pass

# Configuration values from environment or defaults
GIT_REPO_URL = os.getenv("GIT_REPO_URL", "https://github.com/lucianocastro-oc/hello-notebook-poc.git")
GIT_BRANCH = os.getenv("GIT_BRANCH", "main")  # Branch to clone
NOTEBOOK_PATH = os.getenv("NOTEBOOK_PATH", "example_notebook.ipynb")  # Path relative to repo root
RUNNER_IMAGE = os.getenv("RUNNER_IMAGE", "europe-west1-docker.pkg.dev/beeapp-terraform-deployment/app/notebook-runner:0.3.10")
TEMPLATE_NAME = os.getenv("TEMPLATE_NAME", "lucianocastro-notebook-workflow-template")
NAMESPACE = os.getenv("NAMESPACE", "argo")  # Kubernetes namespace
ARGO_SERVER_HOST = os.getenv("ARGO_SERVER_HOST", "https://test.argoworkflows.o-c.space")
ARGO_TOKEN = os.getenv("ARGO_TOKEN")


def clone_repository(repo_url: str, target_dir: str, branch: str = None) -> str:
    """
    Clone a git repository to a target directory.
    
    Args:
        repo_url: URL of the git repository
        target_dir: Directory to clone into
        branch: Optional branch name to checkout
        
    Returns:
        Path to the cloned repository
    """
    print(f"Cloning repository: {repo_url}")
    if branch:
        print(f"Branch: {branch}")
    repo = Repo.clone_from(repo_url, target_dir, **({'branch': branch} if branch else {}))
    print(f"Repository cloned to: {target_dir}")
    return target_dir


def find_parameters_cell(notebook_path: str) -> Dict[str, Any]:
    """
    Parse a Jupyter notebook and extract parameters from the 'parameters' cell.
    
    According to Papermill convention, a parameters cell is identified by
    having 'parameters' in its cell tags.
    
    Args:
        notebook_path: Path to the Jupyter notebook file
        
    Returns:
        Dictionary of parameter names and their default values
    """
    print(f"Reading notebook: {notebook_path}")
    with open(notebook_path, 'r') as f:
        nb = nbformat.read(f, as_version=4)
    
    parameters = {}
    
    # Search for the parameters cell
    for cell in nb.cells:
        if cell.cell_type == 'code':
            # Check if this cell has the 'parameters' tag
            tags = cell.get('metadata', {}).get('tags', [])
            if 'parameters' in tags:
                print("Found parameters cell!")
                # Parse the cell source to extract variable assignments
                # This is a simple parser that looks for assignment statements
                for line in cell.source.split('\n'):
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    # Look for simple assignments (variable = value)
                    if '=' in line and not line.startswith('='):
                        parts = line.split('=', 1)
                        if len(parts) == 2:
                            var_name = parts[0].strip()
                            var_value = parts[1].strip()
                            # Store parameter with its default value
                            parameters[var_name] = var_value
                            print(f"  Found parameter: {var_name} = {var_value}")
                break
    
    if not parameters:
        print("Warning: No parameters cell found in notebook")
    
    return parameters


def create_workflow_template(
    template_name: str,
    git_repo_url: str,
    git_branch: str,
    notebook_path: str,
    parameters: Dict[str, Any],
    runner_image: str, # <-- Make sure this is "your-username/papermill-runner:latest"
    namespace: str
) -> WorkflowTemplate:
    """
    Create an Argo Workflow Template using Hera.
    
    This template is self-contained:
    1. It fetches its own code via GitArtifact.
    2. It installs its own dependencies.
    3. It saves its own output via ArtifactoryArtifact.

    Args:
        template_name: Name for the workflow template
        notebook_path: Path to the notebook within the repository
        git_repo_url: Jupyter notebook repository url
        git_branch: Jupyter notebook repository branch
        parameters: Dictionary of parameters extracted from the notebook
        runner_image: Docker image to use for running the notebook
        namespace: Kubernetes namespace
        
    Returns:
        WorkflowTemplate object
    """
    print(f"\nCreating Workflow Template: {template_name}")
    
    # Convert notebook parameters to Hera Parameter objects
    workflow_parameters = []
    for param_name, default_value in parameters.items():
        # Clean up the default value (which is a string from your parser)
        clean_default = default_value.strip("'\" ") 
        workflow_parameters.append(
            Parameter(name=param_name, default=clean_default)
        )
        print(f"  Added parameter: {param_name} (default: {clean_default})")
    
    with WorkflowTemplate(
        name=template_name,
        namespace=namespace,
        entrypoint="run-notebook",
        arguments=workflow_parameters,
    ) as wt:
        
        # This container defines the notebook execution step
        Container(
            name="run-notebook",
            image=runner_image,
            inputs=[
                # 1. Get the code at runtime
                GitArtifact(
                    name="repo",
                    path="/mnt/repo",
                    repo=git_repo_url,
                    revision=git_branch,
                ),
                # 2. Inherit all workflow parameters
                *workflow_parameters,
            ],
            outputs=[
                # 3. Save the executed notebook as an artifact
                ArtifactoryArtifact(
                    name="executed-notebook",
                    path="/mnt/outputs/output_notebook.ipynb",
                )
            ],
            command=["/bin/sh", "-c"],
            args=[
                # This multi-line string is the entrypoint
                f"""
                set -ex  # Exit on error, print commands
                
                # Ensure output directory exists
                mkdir -p /mnt/outputs
                
                # Install dependencies from the repo
                # (Add error handling if file doesn't exist)
                if [ -f /mnt/repo/requirements.txt ]; then
                    pip install -r /mnt/repo/requirements.txt
                else
                    echo "No requirements.txt found, skipping."
                fi
                
                # Build the papermill command with parameters
                papermill_args=""
                for param_name in {' '.join(parameters.keys())}; do
                    # This is how we get the value of the Argo parameter
                    param_value=$(echo "{{{{inputs.parameters.${{param_name}}}}}}")
                    papermill_args="$papermill_args -p $param_name \"$param_value\""
                done
                
                echo "Running papermill..."
                
                # Execute papermill
                papermill \
                    /mnt/repo/{notebook_path} \
                    /mnt/outputs/output_notebook.ipynb \
                    $papermill_args
                """
            ],
        )
    
    print("Workflow Template created successfully!")
    return wt


def main():
    """
    Main function to orchestrate the workflow template creation and registration.
    """
    print("=" * 60)
    print("Argo Workflow Template Generator & Registrar")
    print("=" * 60)
    print()
    
    # Configure Hera to connect to your Argo server
    # Assumes 'kubectl -n argo port-forward svc/argo-server 2746:2746' is running
    global_config.host = ARGO_SERVER_HOST
    global_config.namespace = NAMESPACE
    global_config.verify_ssl = True  # Set to True in production
    # global_config.token = "Bearer ..." # Uncomment if your server needs auth
    
    # Create a temporary directory for cloning
    temp_dir = tempfile.mkdtemp(prefix="notebook-workflow-")
    
    try:
        # Step 1: Clone the repository locally
        repo_path = clone_repository(GIT_REPO_URL, temp_dir, GIT_BRANCH)
        
        # Step 2: Find and parse the notebook
        notebook_full_path = os.path.join(repo_path, NOTEBOOK_PATH)
        if not os.path.exists(notebook_full_path):
            raise FileNotFoundError(f"Notebook not found: {notebook_full_path}")
        
        parameters = find_parameters_cell(notebook_full_path)
        if not parameters:
             print(f"Warning: No parameters found in {NOTEBOOK_PATH}. Proceeding anyway.")
        
        # Step 3: Create the workflow template object
        workflow_template = create_workflow_template(
            template_name=TEMPLATE_NAME,
            git_repo_url=GIT_REPO_URL,
            git_branch=GIT_BRANCH,
            notebook_path=NOTEBOOK_PATH,
            parameters=parameters,
            runner_image=RUNNER_IMAGE,
            namespace=NAMESPACE
        )
        
        # Step 4: Display the generated workflow YAML
        print("\n" + "=" * 60)
        print("Generated Workflow Template YAML:")
        print("=" * 60)
        print(workflow_template.to_yaml())
        
        # Step 5: Register the template with the Argo cluster
        print("\n" + "=" * 60)
        print(f"Registering template '{TEMPLATE_NAME}' with {global_config.host}...")
        print("=" * 60)
        
        try:
            workflow_template.create() # This is the API call
            print(f"\n✅ SUCCESS!")
            print(f"WorkflowTemplate '{TEMPLATE_NAME}' was created/updated in namespace '{NAMESPACE}'.")
            print("You can now submit it from the Argo UI.")
        except Exception as e:
            print(f"\n❌ FAILED TO REGISTER TEMPLATE:")
            print(e)
            print("\nPlease check:\n"
                  "1. Is your Argo server running?\n"
                  "2. Is 'kubectl port-forward' active?\n"
                  "3. Do you have RBAC permissions to create WorkflowTemplates?")
            sys.exit(1)
            
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        raise
    finally:
        # Clean up temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"\nCleaned up temporary directory: {temp_dir}")


if __name__ == "__main__":
    main()
