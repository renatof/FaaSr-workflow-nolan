#!/usr/bin/env python3

import argparse
import json
import logging
import os
import subprocess
import sys
import textwrap
import time

import boto3
from FaaSr_py import graph_functions as faasr_gf
from github import Github

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Deploy FaaSr functions to specified platform"
    )
    parser.add_argument(
        "--workflow-file", required=True, help="Path to the workflow JSON file"
    )
    return parser.parse_args()


def read_workflow_file(file_path):
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Error: Workflow file {file_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error(f"Error: Invalid JSON in workflow file {file_path}")
        sys.exit(1)


def verify_containers(workflow_data):
    """Check if custom containers are specified via environment variable"""
    custom_container = os.getenv("CUSTOM_CONTAINER", "false").lower() == "true"

    if custom_container:
        logger.info("Using custom containers")
        return

    # Get set of native containers
    with open("native_containers.txt", "r") as f:
        native_containers = {line.strip() for line in f.readlines()}

    for container in workflow_data.get("ActionContainers", {}).values():
        if container not in native_containers:
            logger.error(
                f"Custom container {container} not in native_containers.txt -- to use it, you must enable custom containers" # noqa E501
            )
            sys.exit(1)


def generate_github_secret_imports(faasr_payload):
    """Generate GitHub Actions secret import commands from FaaSr payload."""
    import_statements = []

    # Add secrets for compute servers
    for faas_name, compute_server in faasr_payload.get("ComputeServers", {}).items():
        faas_type = compute_server.get("FaaSType", "")
        match (faas_type):
            case "GitHubActions":
                pat_secret = f"{faas_name}_PAT"
                import_statements.append(
                    f"{pat_secret}: ${{{{ secrets.{pat_secret}}}}}"
                )
            case "Lambda":
                access_key = f"{faas_name}_AccessKey"
                secret_key = f"{faas_name}_SecretKey"
                import_statements.extend(
                    [
                    f"{access_key}: ${{{{ secrets.{access_key}}}}}",
                    f"{secret_key}: ${{{{ secrets.{secret_key}}}}}",
                    ]
                )
            case "OpenWhisk":
                api_key = f"{faas_name}_APIkey"
                import_statements.append(f"{api_key}: ${{{{ secrets.{api_key}}}}}")
            case "GoogleCloud":
                secret_key = f"{faas_name}_SecretKey"
                import_statements.append(
                    f"{secret_key}: ${{{{ secrets.{secret_key}}}}}"
                )
            case "SLURM":
                token = f"{faas_name}_Token"
                import_statements.append(f"{token}: ${{{{ secrets.{token}}}}}")
            case _:
                logger.error(
                    f"Unknown FaaSType ({faas_type}) for compute server: {faas_name} - cannot generate secrets"  # noqa E501
                )
                sys.exit(1)

    # Add secrets for data stores
    for s3_name in faasr_payload.get("DataStores", {}).keys():
        secret_key = f"{s3_name}_SecretKey"
        access_key = f"{s3_name}_AccessKey"
        import_statements.extend(
            [
                f"{access_key}: ${{{{ secrets.{access_key}}}}}",
                f"{secret_key}: ${{{{ secrets.{secret_key}}}}}",
            ]
        )

    # Indent each line for YAML formatting
    indent = " " * 28
    import_statements = "\n".join(f"{indent}{s}" for s in import_statements)

    return import_statements


def deploy_to_github(workflow_data):
    """Deploys GH functions to GitHub Actions"""
    github_token = os.getenv("GH_PAT")

    if not github_token:
        logger.error("GH_PAT environment variable not set")
        sys.exit(1)

    g = Github(github_token)

    # Get the workflow name for prefixing
    workflow_name = workflow_data.get("WorkflowName")

    # Ensure workflow name is specified
    if not workflow_name:
        logger.error("WorkflowName not specified in workflow file")
        sys.exit(1)

    json_prefix = workflow_name

    # Get the current repository
    repo_name = os.getenv("GITHUB_REPOSITORY")

    # Filter actions to be deployed to GitHub Actions
    github_actions = {}
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()
        if faas_type == "githubactions":
            github_actions[action_name] = action_data

    if not github_actions:
        logger.info("No actions found for GitHub Actions deployment")
        return

    try:
        repo = g.get_repo(repo_name)

        # Get the default branch name
        default_branch = repo.default_branch
        logger.info(f"Using branch: {default_branch}")

        # Deploy each action
        for action_name, action_data in github_actions.items():
            # Create prefixed action name using workflow_name-action_name format
            prefixed_action_name = f"{json_prefix}-{action_name}"

            # Create workflow file
            # Get container image, with fallback to default
            container_image = workflow_data.get("ActionContainers", {}).get(action_name)

            # Ensure container image is specified
            if not container_image:
                logger.error(f"No container specified for action: {action_name}")
                sys.exit(1)

            # Dynamically set required secrets and variables
            secret_imports = generate_github_secret_imports(workflow_data)

            workflow_content = textwrap.dedent(
                f"""\
                name: {prefixed_action_name}

                on:
                    workflow_dispatch:
                        inputs:
                            OVERWRITTEN:
                                description: "Overwritten fields"
                                required: true
                            PAYLOAD_URL:
                                description: "URL to payload"
                                required: true

                jobs:
                    run_docker_image:
                        runs-on: ubuntu-latest
                        container: {container_image}

                        env:
{secret_imports}
                            OVERWRITTEN: ${{{{ github.event.inputs.OVERWRITTEN }}}}
                            PAYLOAD_URL: ${{{{ github.event.inputs.PAYLOAD_URL }}}}

                        steps:
                          - name: Run Python entrypoint
                            run: |
                                cd /action
                                python3 faasr_entry.py
            """
            )

            # Create or update the workflow file
            workflow_path = f".github/workflows/{prefixed_action_name}.yml"
            try:
                # Try to get the file first
                contents = repo.get_contents(workflow_path)

                # Update YAML file
                repo.update_file(
                    path=workflow_path,
                    message=f"Update workflow for {prefixed_action_name}",
                    content=workflow_content,
                    sha=contents.sha,
                    branch=default_branch,
                )
                logger.info(f"Successfully updated {workflow_path}")
            except Exception as e:
                if "Not Found" in str(e) or "404" in str(e):
                    # If file doesn't exist, create it
                    logger.info(f"File {workflow_path} doesn't exist, creating...")
                    repo.create_file(
                        path=workflow_path,
                        message=f"Add workflow for {prefixed_action_name}",
                        content=workflow_content,
                        branch=default_branch,
                    )
                    logger.info(f"Successfully created {workflow_path}")
                else:
                    logger.info(f"Error updating/creating {workflow_path}: {str(e)}")
                    # Try to get more details about the error
                    if hasattr(e, "data"):
                        logger.error(f"Error details: {e.data}")
                    if hasattr(e, "status"):
                        logger.error(f"HTTP status: {e.status}")
                    raise e

            logger.info(f"Successfully deployed {prefixed_action_name} to GitHub")

    except Exception as e:
        logger.error(f"Error deploying to GitHub: {str(e)}")
        sys.exit(1)


def get_lambda_credentials(workflow_data):
    """Fetches AWS Lambda credentials from environment variables"""
    # Get AWS credentials
    aws_access_key, aws_secret_key = os.getenv("AWS_AccessKey"), os.getenv(
        "AWS_SecretKey"
    )

    # Fail if AWS creds not set
    if not aws_access_key or not aws_secret_key:
        logger.error(
            "AWS_AccessKey and AWS_SecretKey environment variables must be set"
        )
        sys.exit(1)

    aws_region = workflow_data.get("ComputeServers", {}).get("AWS", {}).get("Region")

    if not aws_region:
        logger.warning("AWS region not specified, defaulting to us-east-1")
        aws_region = "us-east-1"

    return (aws_access_key, aws_secret_key, aws_region)


def deploy_to_aws(workflow_data):
    """Deploys functions to AWS Lambda"""
    # Filter actions that should be deployed to AWS Lambda
    lambda_actions = {}
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()
        if faas_type in ["lambda", "aws_lambda", "aws"]:
            lambda_actions[action_name] = action_data

    if not lambda_actions:
        logger.info("No actions found for AWS Lambda deployment")
        return

    # Get the workflow name to prepend to function names
    workflow_name = workflow_data.get("WorkflowName")

    if not workflow_name:
        logger.error("WorkflowName is not specified in workflow file")
        sys.exit(1)

    # Get AWS credentials
    aws_access_key, aws_secret_key, aws_region = get_lambda_credentials(workflow_data)

    # Get the AWS ARN for the user
    iam_client = boto3.client(
        "iam",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region,
    )
    try:
        aws_arn = iam_client.get_role()["User"]["Arn"]
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Error fetching AWS IAM user ARN: {str(e)}")
        sys.exit(1)

    lambda_client = boto3.client(
        "lambda",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region,
    )

    # Process each action in the workflow
    for action_name, action_data in lambda_actions.items():
        try:
            # Create prefixed function name using workflow_name-action_name format
            prefixed_func_name = f"{workflow_name}-{action_name}"

            # Get container image for AWS Lambda (must be an Amazon ECR image URI)
            container_image = workflow_data.get("ActionContainers", {}).get(action_name)
            if not container_image:
                logger.error(f"No container specified for action: {action_name}")
                sys.exit(1)

            # TODO: remove this
            # Check payload size before deployment
            # payload_size = len(workflow_data.encode("utf-8"))
            # if payload_size > 4000:  # Lambda env var limit is ~4KB
            #    logger.error(
            #        f"Warning: SECRET_PAYLOAD size ({payload_size} bytes) may exceed Lambda environment variable limits"
            #    )

            # Check if function already exists first
            try:
                lambda_client.get_function(FunctionName=prefixed_func_name)
                logger.info(
                    f"Function {prefixed_func_name} already exists, updating..."
                )
                # Update existing function
                lambda_client.update_function_code(
                    FunctionName=prefixed_func_name, ImageUri=container_image
                )

                # Wait for the function update to complete
                logger.info(
                    f"Waiting for {prefixed_func_name} code update to complete..."
                )
                max_attempts = 60  # Wait up to 5 minutes
                attempt = 0
                while attempt < max_attempts:
                    try:
                        response = lambda_client.get_function(
                            FunctionName=prefixed_func_name
                        )
                        state = response["Configuration"]["State"]
                        last_update_status = response["Configuration"][
                            "LastUpdateStatus"
                        ]

                        if state == "Active" and last_update_status == "Successful":
                            break
                        elif state == "Failed" or last_update_status == "Failed":
                            sys.exit(1)
                        else:
                            time.sleep(5)
                            attempt += 1
                    except Exception as e:
                        logger.info(f"Error checking function state: {str(e)}")
                        time.sleep(5)
                        attempt += 1

                if attempt >= max_attempts:
                    logger.error(
                        f"Timeout waiting for {prefixed_func_name} update to complete"
                    )
                    sys.exit(1)

                # Now update environment variables
                lambda_client.update_function_configuration(
                    FunctionName=prefixed_func_name,
                )
                logger.info(f"Successfully updated {prefixed_func_name} on AWS Lambda")

            except lambda_client.exceptions.ResourceNotFoundException:
                # Function doesn't exist, create it
                logger.info(f"Creating new Lambda function: {prefixed_func_name}")

                # TODO: is minimal function necessary here?
                try:
                    lambda_client.create_function(
                        FunctionName=prefixed_func_name,
                        PackageType="Image",
                        Code={"ImageUri": container_image},
                        Role=aws_arn,
                        Timeout=300,
                        MemorySize=128,
                    )

                    # Wait for the function to become active before updating
                    logger.info(f"Waiting for {prefixed_func_name} to become active...")
                    max_attempts = 120  # Wait up to 10 minutes
                    attempt = 0
                    while attempt < max_attempts:
                        try:
                            response = lambda_client.get_function(
                                FunctionName=prefixed_func_name
                            )
                            state = response["Configuration"]["State"]

                            if state == "Active":
                                logger.info(
                                    f"Function {prefixed_func_name} is now active"
                                )
                                break
                            elif state == "Failed":
                                logger.error(
                                    f"Function {prefixed_func_name} creation failed"
                                )
                                sys.exit(1)
                            else:
                                logger.info(f"Function state: {state}, waiting...")
                                time.sleep(5)
                                attempt += 1
                        except Exception as e:
                            logger.error(f"Error checking function state: {str(e)}")
                            time.sleep(5)
                            attempt += 1

                    if attempt >= max_attempts:
                        logger.error(
                            f"Timeout while waiting for {prefixed_func_name} to become active"
                        )
                        sys.exit(1)

                    # Now update with full configuration
                    # TODO: fetch timeout and memory size from workflow file
                    lambda_client.update_function_configuration(
                        FunctionName=prefixed_func_name,
                        Timeout=900,
                        MemorySize=1024,
                    )
                    logger.info(f"Updated {prefixed_func_name} with full configuration")

                except Exception as minimal_error:
                    logger.error(f"Minimal creation failed: {minimal_error}")
                    raise minimal_error
        except Exception as e:
            logger.error(f"Error deploying {prefixed_func_name} to AWS: {str(e)}")
            # logger.error additional debugging information
            if "RequestEntityTooLargeException" in str(e):
                logger.error(f"Payload too large - size: {len(workflow_data)} bytes")
                logger.error(
                    "Consider reducing workflow complexity or using external storage"
                )
            elif "InvalidParameterValueException" in str(e):
                logger.error(
                    "Check Lambda configuration parameters (memory, timeout, role)"
                )
            sys.exit(1)


def get_openwhisk_credentials(workflow_data):
    # Get OpenWhisk server configuration from workflow data
    for server_config in workflow_data["ComputeServers"].values():
        if server_config["FaaSType"].lower() == "openwhisk":
            return (
                server_config["Endpoint"],
                server_config["Namespace"],
            )

    logger.error("Error: No OpenWhisk server configuration found in workflow data")
    sys.exit(1)


def deploy_to_ow(workflow_data):
    # Get OpenWhisk credentials
    # TODO: AllowSelfSignedCertifcate
    api_host, namespace = get_openwhisk_credentials(workflow_data)

    # Get the workflow name for prefixing
    workflow_name = workflow_data.get("WorkflowName", "default")
    json_prefix = workflow_name

    # Filter actions that should be deployed to OpenWhisk
    ow_actions = {}
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()
        if faas_type == "openwhisk":
            ow_actions[action_name] = action_data

    if not ow_actions:
        logger.info("No actions found for OpenWhisk deployment")
        return

    # Set up wsk properties
    subprocess.run(f"wsk property set --apihost {api_host}", shell=True)

    # Set authentication using API key from environment variable
    ow_api_key = os.getenv("OW_APIkey")
    if ow_api_key:
        subprocess.run(f"wsk property set --auth {ow_api_key}", shell=True)
        logger.info("Using OpenWhisk with API key authentication")
    else:
        logger.info("Using OpenWhisk without authentication")

    # Always use insecure flag to bypass certificate issues
    subprocess.run("wsk property set --insecure", shell=True)

    # TODO: why is this necessary?
    # Set environment variable to handle certificate issue
    env = os.environ.copy()
    env["GODEBUG"] = "x509ignoreCN=0"

    # Process each action in the workflow
    for action_name, action_data in ow_actions.items():
        try:
            # Create prefixed function name using workflow_name-action_name format
            prefixed_func_name = f"{json_prefix}-{action_name}"

            # Create or update OpenWhisk action using wsk CLI
            try:
                # First check if action exists (add --insecure flag)
                check_cmd = f"wsk action get {prefixed_func_name} --insecure >/dev/null 2>&1"
                exists = subprocess.run(check_cmd, shell=True, env=env).returncode == 0

                # Get container image, with fallback to default
                container_image = workflow_data.get("ActionContainers", {}).get(
                    action_name
                )

                if not container_image:
                    logger.error(f"No container specified for action: {action_name}")
                    sys.exit(1)

                if exists:
                    # Update existing action (add --insecure flag)
                    cmd = f"wsk action update {prefixed_func_name} --docker {container_image} --insecure"  # noqa E501
                else:
                    # Create new action (add --insecure flag)
                    cmd = f"wsk action create {prefixed_func_name} --docker {container_image} --insecure"  # noqa E501

                result = subprocess.run(
                    cmd, shell=True, capture_output=True, text=True, env=env
                )

                if result.returncode != 0:
                    raise Exception(
                        f"Failed to {'update' if exists else 'create'} action: {result.stderr}"
                    )

                logger.info(f"Successfully deployed {prefixed_func_name} to OpenWhisk")

            except Exception as e:
                logger.error(
                    f"Error deploying {prefixed_func_name} to OpenWhisk: {str(e)}"
                )
                sys.exit(1)

        except Exception as e:
            logger.error(f"Error processing {prefixed_func_name}: {str(e)}")
            sys.exit(1)


def main():
    args = parse_arguments()
    workflow_data = read_workflow_file(args.workflow_file)

    # Store the workflow file path in the workflow data
    workflow_data["_workflow_file"] = args.workflow_file

    # Validate workflow for cycles and unreachable states
    logger.info("Validating workflow for cycles and unreachable states...")
    try:
        faasr_gf.check_dag(workflow_data)
        logger.info("Workflow validation passed")
    except SystemExit:
        logger.info("Workflow validation failed - check logs for details")

    # Verify if custom containers are specified correctly
    verify_containers(workflow_data)

    # Get all unique FaaSTypes from workflow data
    faas_types = set()
    for server in workflow_data.get("ComputeServers", {}).values():
        if "FaaSType" in server:
            faas_types.add(server["FaaSType"].lower())

    if not faas_types:
        logger.error("Error: No FaaSType found in workflow file")
        sys.exit(1)

    logger.info(f"Found FaaS platforms: {', '.join(faas_types)}")

    # Deploy to each platform found
    for faas_type in faas_types:
        logger.info(f"\nDeploying to {faas_type}...")
        if faas_type == "lambda":
            deploy_to_aws(workflow_data)
        elif faas_type == "githubactions":
            deploy_to_github(workflow_data)
        elif faas_type == "openwhisk":
            deploy_to_ow(workflow_data)
        else:
            logger.error(f"Unsupported FaaSType: {faas_type}")
            sys.exit(1)


if __name__ == "__main__":
    main()
