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


def main():
    """Main entry point for the migration adapter."""
    workflow_path = get_workflow_file()

    github_repo = os.getenv("GITHUB_REPOSITORY")

    file_path = f"{github_repo}/{workflow_path}"

    token = os.getenv("GH_PAT")

    # debug
    print(file_path)

    if not token:
        logger.warning("GH_PAT environment variable not set. Inovcation will fail if repository is private")

    # Create payload object
    try:
        workflow = FaaSrPayload(url=file_path, token=token)
    except Exception as e:
        logger.error(f"Error initializing FaaSr payload: {e}")
        sys.exit(1)

    workflow_name = FaaSrPayload.get("WorkflowName")
    if not workflow_name:
        logger.error("Error: WorkflowName not found in payload")
        sys.exit(1)

    entry_func = FaaSrPayload.get("FunctionInvoke")

    if not entry_func:
        logger.error("Error: FunctionInvoke not found in payload")
        sys.exit(1)

    faasr_scheduler = Scheduler(workflow)

    # Trigger workflow
    try:
        logger.info(f"Triggering entry action: {entry_func}")
        faasr_scheduler.trigger_func(workflow_name, entry_func)
    except Exception as e:
        logger.error(f"\nTrigger failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
