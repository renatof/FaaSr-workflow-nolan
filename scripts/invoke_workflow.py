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
    
    logger.info(f"DEBUG: Workflow file argument: {workflow_path}")

    # Verify provided workflow file is valid
    if not Path(workflow_path).is_file():
        logger.error(f"Workflow file {workflow_path} not found")
        sys.exit(1)

    logger.info(f"DEBUG: Workflow file exists locally: {workflow_path}")
    return workflow_path


def add_secrets_to_server(server, faas_type):
    """Adds secrets to compute server based on FaaS type"""
    logger.info(f"DEBUG: Adding secrets for FaaS type: {faas_type}")
    logger.info(f"DEBUG: Server config before adding secrets: {server}")
    
    match faas_type:
        case "GitHubActions":
            token = os.getenv("GH_PAT")
            if not token:
                logger.error(
                    "GH_PAT environment variable must be set for GitHub Action invocation"
                )
                sys.exit(1)
            server["Token"] = token
            logger.info("DEBUG: Added GH_PAT to server config")

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
            logger.info("DEBUG: Added AWS credentials to server config")

        case "OpenWhisk":
            ow_api_key = os.getenv("OW_APIkey")
            if not ow_api_key:
                logger.error(
                    "OW_APIkey environment variable must be set for OpenWhisk invocation"
                )
                sys.exit(1)
            server["OW_APIkey"] = ow_api_key
            logger.info("DEBUG: Added OW_APIkey to server config")

        case "GoogleCloud":
            gcp_secret_key = os.getenv("GCP_SecretKey")
            logger.info(f"DEBUG: GCP_SecretKey present: {gcp_secret_key is not None}")
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
            logger.info("DEBUG: Added GH_PAT to server config")

        case "SLURM":
            slurm_token = os.getenv("SLURM_Token")
            if not slurm_token:
                logger.error(
                    "SLURM_Token environment variable must be set for SLURM invocation"
                )
                sys.exit(1)
            server["SLURM_Token"] = slurm_token
            logger.info("DEBUG: Added SLURM_Token to server config")


def main():
    """Function invocation script"""
    logger.info("=== INVOKE WORKFLOW DEBUG START ===")
    
    # Step 1: Get workflow file
    workflow_path = get_workflow_file()
    logger.info(f"DEBUG Step 1: Workflow file path: {workflow_path}")

    # Step 2: Get environment variables
    github_repo = os.getenv("GITHUB_REPOSITORY")
    ref = os.getenv("GITHUB_REF_NAME", "main")
    token = os.getenv("GH_PAT")
    
    logger.info(f"DEBUG Step 2: Environment variables:")
    logger.info(f"  - GITHUB_REPOSITORY: {github_repo}")
    logger.info(f"  - GITHUB_REF_NAME: {ref}")
    logger.info(f"  - GH_PAT present: {token is not None}")
    logger.info(f"  - GCP_SecretKey present: {os.getenv('GCP_SecretKey') is not None}")

    # Step 3: Construct file path
    file_path = f"{github_repo}/{ref}/{workflow_path}"
    logger.info(f"DEBUG Step 3: Constructed GitHub URL path: {file_path}")
    logger.info(f"DEBUG Step 3: Full URL will be: https://raw.githubusercontent.com/{file_path}")

    if not token:
        logger.warning(
            "GH_PAT environment variable not set. Invocation will fail if repository is private"
        )

    # Step 4: Create payload object
    logger.info("DEBUG Step 4: Attempting to fetch and parse workflow JSON from GitHub...")
    try:
        workflow = FaaSrPayload(url=file_path, token=token)
        logger.info("DEBUG Step 4: Successfully created FaaSrPayload object")
    except Exception as e:
        logger.error(f"DEBUG Step 4: FAILED to create FaaSrPayload")
        logger.error(f"  - Exception type: {type(e).__name__}")
        logger.error(f"  - Exception message: {str(e)}")
        logger.error(f"  - File path used: {file_path}")
        import traceback
        logger.error(f"  - Full traceback:\n{traceback.format_exc()}")
        sys.exit(1)

    # Step 5: Extract workflow info
    logger.info("DEBUG Step 5: Extracting workflow information...")
    workflow_name = workflow.get("WorkflowName")
    logger.info(f"  - WorkflowName: {workflow_name}")
    
    if not workflow_name:
        logger.error("WorkflowName not found in payload")
        sys.exit(1)

    entry_action_name = workflow.get("FunctionInvoke")
    logger.info(f"  - FunctionInvoke: {entry_action_name}")
    
    if not entry_action_name:
        logger.error("FunctionInvoke not found in payload")
        sys.exit(1)

    # Step 6: Get server configuration
    logger.info("DEBUG Step 6: Getting server configuration...")
    try:
        server_name = workflow["ActionList"][entry_action_name]["FaaSServer"]
        logger.info(f"  - Server name: {server_name}")
        
        server = workflow["ComputeServers"][server_name]
        logger.info(f"  - Server config: {server}")
        
        faas_type = server["FaaSType"]
        logger.info(f"  - FaaS type: {faas_type}")
        
        use_secret_store = server.get("UseSecretStore", False)
        logger.info(f"  - UseSecretStore: {use_secret_store}")
    except KeyError as e:
        logger.error(f"DEBUG Step 6: FAILED - Missing key: {e}")
        logger.error(f"  - Available ActionList keys: {list(workflow.get('ActionList', {}).keys())}")
        logger.error(f"  - Available ComputeServers keys: {list(workflow.get('ComputeServers', {}).keys())}")
        sys.exit(1)

    # Step 7: Verify UseSecretStore
    if not use_secret_store:
        logger.error("DEBUG Step 7: UseSecretStore must be true for initial action")
        logger.error(f"  - Current value: {use_secret_store}")
        sys.exit(1)
    
    logger.info("DEBUG Step 7: UseSecretStore validation passed")

    # Step 8: Add secrets
    logger.info("DEBUG Step 8: Adding secrets to server configuration...")
    add_secrets_to_server(server, faas_type)
    logger.info("DEBUG Step 8: Secrets added successfully")

    # Step 9: Trigger workflow
    logger.info("DEBUG Step 9: Creating Scheduler and triggering workflow...")
    try:
        faasr_scheduler = Scheduler(workflow)
        logger.info(f"  - Scheduler created successfully")
        logger.info(f"  - Triggering entry action: {entry_action_name}")

        faasr_scheduler.trigger_func(workflow_name, entry_action_name)
        
        logger.info("DEBUG Step 9: Workflow triggered successfully")
    except Exception as e:
        logger.error(f"DEBUG Step 9: FAILED to trigger workflow")
        logger.error(f"  - Exception type: {type(e).__name__}")
        logger.error(f"  - Exception message: {str(e)}")
        import traceback
        logger.error(f"  - Full traceback:\n{traceback.format_exc()}")
        sys.exit(1)

    logger.info("=== INVOKE WORKFLOW DEBUG END - SUCCESS ===")


if __name__ == "__main__":
    main()
