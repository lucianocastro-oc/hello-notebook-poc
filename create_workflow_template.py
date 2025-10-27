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
)


# Hardcoded configuration values
GIT_REPO_URL = "https://github.com/lucianocastro-oc/hello-notebook-poc.git"
GIT_BRANCH = "copilot/create-argo-workflow-template"  # Branch to clone
NOTEBOOK_PATH = "example_notebook.ipynb"  # Path relative to repo root
RUNNER_IMAGE = "jupyter/minimal-notebook:latest"
TEMPLATE_NAME = "notebook-workflow-template"
NAMESPACE = "argo"  # Kubernetes namespace


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
        repo = Repo.clone_from(repo_url, target_dir, branch=branch)
    else:
        repo = Repo.clone_from(repo_url, target_dir)
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
    notebook_path: str,
    parameters: Dict[str, Any],
    runner_image: str,
    namespace: str
) -> WorkflowTemplate:
    """
    Create an Argo Workflow Template using Hera.
    
    Args:
        template_name: Name for the workflow template
        notebook_path: Path to the notebook within the repository
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
        workflow_parameters.append(
            Parameter(name=param_name, value=str(default_value))
        )
        print(f"  Added parameter: {param_name}")
    
    # Create the workflow template with an entrypoint
    with WorkflowTemplate(
        name=template_name,
        namespace=namespace,
        entrypoint="run-notebook",
        arguments=workflow_parameters,
    ) as wt:
        # Create a container that runs papermill to execute the notebook
        # Note: The container assumes the notebook is available in the container
        # In a real scenario, you would need to add a volume mount or git-sync init container
        Container(
            name="run-notebook",
            image=runner_image,
            command=["papermill"],
            args=[
                f"/workspace/{notebook_path}",
                "/output/output_notebook.ipynb",
                # Add parameter arguments
                *[f"-p {name} {{{{{name}}}}}" for name in parameters.keys()],
            ],
        )
    
    print("Workflow Template created successfully!")
    return wt


def main():
    """
    Main function to orchestrate the workflow template creation.
    """
    print("=" * 60)
    print("Argo Workflow Template Generator for Jupyter Notebooks")
    print("=" * 60)
    print()
    
    # Create a temporary directory for cloning
    temp_dir = tempfile.mkdtemp(prefix="notebook-workflow-")
    
    try:
        # Step 1: Clone the repository
        repo_path = clone_repository(GIT_REPO_URL, temp_dir, GIT_BRANCH)
        
        # Step 2: Find and parse the notebook
        notebook_full_path = os.path.join(repo_path, NOTEBOOK_PATH)
        if not os.path.exists(notebook_full_path):
            raise FileNotFoundError(f"Notebook not found: {notebook_full_path}")
        
        parameters = find_parameters_cell(notebook_full_path)
        
        # Step 3: Create the workflow template
        workflow_template = create_workflow_template(
            template_name=TEMPLATE_NAME,
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
        
        print("\n" + "=" * 60)
        print("SUCCESS!")
        print("=" * 60)
        print(f"\nWorkflow template '{TEMPLATE_NAME}' has been created.")
        print(f"Namespace: {NAMESPACE}")
        print(f"Parameters found: {len(parameters)}")
        print("\nTo apply this template to your Kubernetes cluster, you would run:")
        print(f"  kubectl apply -f <yaml-file>")
        print("\nNote: This script generates the template but does not apply it to the cluster.")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        raise
    finally:
        # Clean up temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"\nCleaned up temporary directory: {temp_dir}")


if __name__ == "__main__":
    main()
