# hello-notebook-poc
POC for creating an Argo workflow that runs a Jupyter notebook

## Overview

This repository demonstrates how to create an Argo Workflow Template from a parameterized Jupyter notebook using:
- **Papermill** convention for notebook parameters
- **Hera** for generating Argo Workflow Templates
- **GitPython** for repository cloning

## Files

- `example_notebook.ipynb` - Sample Jupyter notebook with a parameters cell
- `create_workflow_template.py` - Main script that generates the Argo Workflow Template
- `requirements.txt` - Python dependencies

## How It Works

1. **Clone Repository**: The script clones a git repository containing a Jupyter notebook
2. **Parse Parameters**: It identifies the notebook's parameters by finding cells tagged with `parameters` (Papermill convention)
3. **Generate Template**: It creates an Argo Workflow Template using Hera that can execute the notebook with Papermill

## Usage

### Prerequisites

```bash
pip install -r requirements.txt
```

### Run the Script

```bash
python3 create_workflow_template.py
```

The script will:
1. Clone the configured git repository
2. Find and parse the Jupyter notebook
3. Extract parameters from the `parameters` cell
4. Generate an Argo Workflow Template YAML
5. Display the generated YAML

### Configuration

The script uses hardcoded configuration values (as per requirements):

```python
GIT_REPO_URL = "https://github.com/lucianocastro-oc/hello-notebook-poc.git"
GIT_BRANCH = "copilot/create-argo-workflow-template"
NOTEBOOK_PATH = "example_notebook.ipynb"
RUNNER_IMAGE = "jupyter/minimal-notebook:latest"
TEMPLATE_NAME = "notebook-workflow-template"
NAMESPACE = "argo"
```

## Papermill Parameters Convention

The notebook must have a cell tagged with `parameters` containing variable assignments:

```python
# Default parameters
input_data = "default_input.csv"
output_path = "output/"
threshold = 0.5
max_iterations = 100
```

The script extracts these parameters and makes them available as workflow inputs.

## Generated Workflow Template

The script generates an Argo Workflow Template that:
- Uses Papermill to execute the notebook
- Accepts parameters from the workflow invocation
- Outputs the executed notebook to `/output/output_notebook.ipynb`

To apply the generated template to your Kubernetes cluster:

```bash
python3 create_workflow_template.py > workflow-template.yaml
kubectl apply -f workflow-template.yaml
```

## Notes

- The script generates the template but does not apply it to the cluster
- The generated template assumes the notebook is available at `/workspace/` in the container
- In a production setup, you would need to add volume mounts or init containers to fetch the notebook

