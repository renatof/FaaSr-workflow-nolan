#!/usr/bin/env python3
import argparse
import os
import sys
import logging
from pathlib import Path

from FaaSr_py import FaaSrPayload
from FaaSr_py import Scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)


def get_workflow_file():
    """Gets workflow file from command line arguments"""
    parser = argparse.ArgumentParser(
        description="Invoke a workflow defined in a JSON file"
    )
    parser.add_argument(
        "--workflow-file", required=True, help="Path to the workflow JSON file"
    )

    workflow_path = parser.parse_args().workflow_file

    # Verify provided workflow file is valid
    if not Path(workflow_path).is_file():
        logger.error(f"Workflow file {workflow_path} not found")
        sys.exit(1)

    return workflow_path


def add_secrets_to_server(server, faas_type):
    """Adds secrets to compute server based on FaaS type"""
    
    match faas_type:
        case "GitHubActions":
            token = os.getenv("GH_PAT")
            if not token:
                logger.error(
                    "GH_PAT environment variable must be set for GitHub Action invocation"
                )
                sys.exit(1)
            server["Token"] = token

        case "Lambda":
            aws_access_key = os.getenv("AWS_AccessKey")
            aws_secret_key = os.getenv("AWS_SecretKey")

            if not aws_access_key or not aws_secret_key:
                logger.error(
                    "AWS_AccessKey and AWS_SecretKey environment variables must be set for Lambda invocation"
                )
                sys.exit(1)

            server["AWS_AccessKey"] = aws_access_key
            server["AWS_SecretKey"] = aws_secret_key

        case "OpenWhisk":
            ow_api_key = os.getenv("OW_APIkey")
            if not ow_api_key:
                logger.error(
                    "OW_APIkey environment variable must be set for OpenWhisk invocation"
                )
                sys.exit(1)
            server["OW_APIkey"] = ow_api_key

        case "GoogleCloud":
            gcp_secret_key = os.getenv("GCP_SecretKey")
            if not gcp_secret_key:
                logger.error(
                    "GCP_SecretKey environment variable must be set for Google Cloud Functions invocation"
                )
                sys.exit(1)
            server["SecretKey"] = gcp_secret_key

            token = os.getenv("GH_PAT")
            if not token:
                logger.warning(
                    "GH_PAT environment variable must be set for GitHub Action invocation"
                )
                sys.exit(1)
            server["Token"] = token

        case "SLURM":
            slurm_token = os.getenv("SLURM_Token")
            if not slurm_token:
                logger.error(
                    "SLURM_Token environment variable must be set for SLURM invocation"
                )
                sys.exit(1)
            server["SLURM_Token"] = slurm_token


def main():
    """Function invocation script"""
    
    # Step 1: Get workflow file
    workflow_path = get_workflow_file()

    # Step 2: Get environment variables
    github_repo = os.getenv("GITHUB_REPOSITORY")
    ref = os.getenv("GITHUB_REF_NAME", "main")
    token = os.getenv("GH_PAT")

    # Step 3: Construct file path
    file_path = f"{github_repo}/{ref}/{workflow_path}"

    if not token:
        logger.warning(
            "GH_PAT environment variable not set. Invocation will fail if repository is private"
        )

    # Step 4: Create payload object
    try:
        workflow = FaaSrPayload(url=file_path, token=token)
    except Exception as e:
        import traceback
        logger.error(f"  - Full traceback:\n{traceback.format_exc()}")
        sys.exit(1)

    # Step 5: Extract workflow info
    workflow_name = workflow.get("WorkflowName")
    
    if not workflow_name:
        logger.error("WorkflowName not found in payload")
        sys.exit(1)

    entry_action_name = workflow.get("FunctionInvoke")
    
    if not entry_action_name:
        logger.error("FunctionInvoke not found in payload")
        sys.exit(1)

    # Step 6: Get server configuration
    try:
        server_name = workflow["ActionList"][entry_action_name]["FaaSServer"]
        
        server = workflow["ComputeServers"][server_name]
        
        faas_type = server["FaaSType"]
        
        use_secret_store = server.get("UseSecretStore", False)
    except KeyError as e:
        sys.exit(1)

    # Step 7: Verify UseSecretStore
    if not use_secret_store:
        logger.error("DEBUG Step 7: UseSecretStore must be true for initial action")
        logger.error(f"  - Current value: {use_secret_store}")
        sys.exit(1)

    # Step 8: Add secrets
    add_secrets_to_server(server, faas_type)

    # Step 9: Trigger workflow
    try:
        faasr_scheduler = Scheduler(workflow)

        faasr_scheduler.trigger_func(workflow_name, entry_action_name)
    except Exception as e:
        import traceback
        logger.error(f"  - Full traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()