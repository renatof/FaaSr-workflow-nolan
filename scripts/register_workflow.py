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
import requests
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
        print(f"Error: Workflow file {file_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in workflow file {file_path}")
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
                    f"{access_key}: ${{{{ secrets.{access_key}}}}}",
                    f"{secret_key}: ${{{{ secrets.{secret_key}}}}}",
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
                    f"Unknown FaaSType ({faas_type}) for compute server: {faas_name} - cannot generate secrets"
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
                        print(f"Error details: {e.data}")
                    if hasattr(e, "status"):
                        print(f"HTTP status: {e.status}")
                    raise e

            logger.info(f"Successfully deployed {prefixed_action_name} to GitHub")

    except Exception as e:
        print(f"Error deploying to GitHub: {str(e)}")
        sys.exit(1)


def deploy_to_aws(workflow_data):
    # Get AWS credentials
    aws_access_key, aws_secret_key, aws_region, role_arn = get_aws_credentials()

    lambda_client = boto3.client(
        "lambda",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region,
    )

    # Get the workflow name for function naming
    workflow_name = workflow_data.get("WorkflowName", "default")
    json_prefix = workflow_name

    # Filter actions that should be deployed to AWS Lambda
    lambda_actions = {}
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()
        if faas_type in ["lambda", "aws_lambda", "aws"]:
            lambda_actions[action_name] = action_data

    if not lambda_actions:
        print("No actions found for AWS Lambda deployment")
        return

    # Process each action in the workflow
    for action_name, action_data in lambda_actions.items():
        try:
            # Create prefixed function name using workflow_name-action_name format
            prefixed_func_name = f"{json_prefix}-{action_name}"

            # Get container image for AWS Lambda (must be an Amazon ECR image URI)
            container_image = workflow_data.get("ActionContainers", {}).get(action_name)
            if not container_image:
                logger.error(f"No container specified for action: {action_name}")
                sys.exit(1)

            # Check payload size before deployment
            payload_size = len(workflow_data.encode("utf-8"))
            if payload_size > 4000:  # Lambda env var limit is ~4KB
                logger.error(
                    f"Warning: SECRET_PAYLOAD size ({payload_size} bytes) may exceed Lambda environment variable limits"
                )

            # Check if function already exists first
            try:
                existing_func = lambda_client.get_function(
                    FunctionName=prefixed_func_name
                )
                print(f"Function {prefixed_func_name} already exists, updating...")
                # Update existing function
                lambda_client.update_function_code(
                    FunctionName=prefixed_func_name, ImageUri=container_image
                )

                # Wait for the function update to complete
                print(f"Waiting for {prefixed_func_name} code update to complete...")
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
                        print(f"Error checking function state: {str(e)}")
                        time.sleep(5)
                        attempt += 1

                if attempt >= max_attempts:
                    print(
                        f"Timeout waiting for {prefixed_func_name} update to complete"
                    )
                    sys.exit(1)

                # Now update environment variables
                lambda_client.update_function_configuration(
                    FunctionName=prefixed_func_name,
                )
                print(f"Successfully updated {prefixed_func_name} on AWS Lambda")

            except lambda_client.exceptions.ResourceNotFoundException:
                # Function doesn't exist, create it
                print(f"Creating new Lambda function: {prefixed_func_name}")

                # Create function with minimal parameters first, then update
                print("Creating with minimal parameters...")
                try:
                    lambda_client.create_function(
                        FunctionName=prefixed_func_name,
                        PackageType="Image",
                        Code={"ImageUri": container_image},
                        Role=role_arn,
                        Timeout=300,  # Shorter timeout
                        MemorySize=128,  # Minimal memory
                    )
                    print(
                        f"Successfully created {prefixed_func_name} with minimal parameters"
                    )

                    # Wait for the function to become active before updating
                    print(f"Waiting for {prefixed_func_name} to become active...")
                    max_attempts = 60  # Wait up to 5 minutes
                    attempt = 0
                    while attempt < max_attempts:
                        try:
                            response = lambda_client.get_function(
                                FunctionName=prefixed_func_name
                            )
                            state = response["Configuration"]["State"]

                            if state == "Active":
                                print(f"Function {prefixed_func_name} is now active")
                                break
                            elif state == "Failed":
                                print(f"Function {prefixed_func_name} creation failed")
                                sys.exit(1)
                            else:
                                print(f"Function state: {state}, waiting...")
                                time.sleep(5)
                                attempt += 1
                        except Exception as e:
                            print(f"Error checking function state: {str(e)}")
                            time.sleep(5)
                            attempt += 1

                    if attempt >= max_attempts:
                        print(
                            f"Timeout waiting for {prefixed_func_name} to become active"
                        )
                        sys.exit(1)

                    # Now update with full configuration
                    lambda_client.update_function_configuration(
                        FunctionName=prefixed_func_name,
                        Timeout=900,
                        MemorySize=1024,
                    )
                    print(f"Updated {prefixed_func_name} with full configuration")

                except Exception as minimal_error:
                    print(f"Minimal creation failed: {minimal_error}")
                    raise minimal_error

        except Exception as e:
            print(f"Error deploying {prefixed_func_name} to AWS: {str(e)}")
            # Print additional debugging information
            if "RequestEntityTooLargeException" in str(e):
                print(f"Payload too large - size: {len(workflow_data)} bytes")
                print("Consider reducing workflow complexity or using external storage")
            elif "InvalidParameterValueException" in str(e):
                print("Check Lambda configuration parameters (memory, timeout, role)")
            sys.exit(1)


def get_openwhisk_credentials(workflow_data):
    # Get OpenWhisk server configuration from workflow data
    for server_name, server_config in workflow_data["ComputeServers"].items():
        if server_config["FaaSType"].lower() == "openwhisk":
            return (
                server_config["Endpoint"],
                server_config["Namespace"],
                server_config["SSL"].lower() == "true",
            )

    print("Error: No OpenWhisk server configuration found in workflow data")
    sys.exit(1)


def deploy_to_ow(workflow_data):
    # Get OpenWhisk credentials
    api_host, namespace, ssl = get_openwhisk_credentials(workflow_data)

    # Get the workflow name for prefixing
    workflow_name = workflow_data.get("WorkflowName", "default")
    json_prefix = workflow_name

    # Filter actions that should be deployed to OpenWhisk
    ow_actions = {}
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()
        if faas_type in ["openwhisk", "open_whisk", "ow"]:
            ow_actions[action_name] = action_data

    if not ow_actions:
        print("No actions found for OpenWhisk deployment")
        return

    # Set up wsk properties
    subprocess.run(f"wsk property set --apihost {api_host}", shell=True)

    # Set authentication using API key from environment variable
    ow_api_key = os.getenv("OW_APIkey")
    if ow_api_key:
        subprocess.run(f"wsk property set --auth {ow_api_key}", shell=True)
        print("Using OpenWhisk with API key authentication")
    else:
        print("Using OpenWhisk without authentication")

    # Always use insecure flag to bypass certificate issues
    subprocess.run("wsk property set --insecure", shell=True)

    # Set environment variable to handle certificate issue
    env = os.environ.copy()
    env["GODEBUG"] = "x509ignoreCN=0"

    # Process each action in the workflow
    for action_name, action_data in ow_actions.items():
        try:
            actual_func_name = action_data["FunctionName"]

            # Create prefixed function name using workflow_name-action_name format
            prefixed_func_name = f"{json_prefix}-{action_name}"

            # Create or update OpenWhisk action using wsk CLI
            try:
                # First check if action exists (add --insecure flag)
                check_cmd = (
                    f"wsk action get {prefixed_func_name} --insecure >/dev/null 2>&1"
                )
                exists = subprocess.run(check_cmd, shell=True, env=env).returncode == 0

                # Get container image, with fallback to default
                container_image = workflow_data.get("ActionContainers", {}).get(
                    action_name, "ghcr.io/faasr/openwhisk-tidyverse"
                )

                if exists:
                    # Update existing action (add --insecure flag)
                    cmd = f"wsk action update {prefixed_func_name} --docker {container_image} --insecure"
                else:
                    # Create new action (add --insecure flag)
                    cmd = f"wsk action create {prefixed_func_name} --docker {container_image} --insecure"

                result = subprocess.run(
                    cmd, shell=True, capture_output=True, text=True, env=env
                )

                if result.returncode != 0:
                    raise Exception(
                        f"Failed to {'update' if exists else 'create'} action: {result.stderr}"
                    )

                print(f"Successfully deployed {prefixed_func_name} to OpenWhisk")

            except Exception as e:
                print(f"Error deploying {prefixed_func_name} to OpenWhisk: {str(e)}")
                sys.exit(1)

        except Exception as e:
            print(f"Error processing {prefixed_func_name}: {str(e)}")
            sys.exit(1)

def get_gcp_resource_requirements(workflow_data, action_name, server_config):
    """
    Extract resource requirements with fallback hierarchy:
    Function-level → Server-level → Default values
    
    Args:
        workflow_data: Full workflow JSON
        action_name: Name of the action
        server_config: ComputeServers[server_name] config
        
    Returns:
        dict: Resource configuration for GCP
    """
    action_list = workflow_data.get("ActionList", {})
    action_config = action_list.get(action_name, {})
    
    function_resources = action_config.get("Resources", {})
    
    max_memory = action_config.get("MaxMemory")
    max_runtime = action_config.get("MaxRuntime")
    
    config = {
        "cpu": str(
            function_resources.get("CPUsPerTask")
            or server_config.get("CPUsPerTask")
            or 1
        ),
        "memory_mb": (
            function_resources.get("Memory")
            or max_memory
            or server_config.get("Memory")
            or 512
        ),
        "timeout_seconds": (
            function_resources.get("TimeLimit")
            or max_runtime
            or server_config.get("TimeLimit")
            or 3600
        )
    }
    
    return config

def create_gcp_job_definition(container_image,service_account, resources):
    """
    Creates a Cloud Run Job definition following GCP's API v2 schema.
    
    Args:
        container_image: Container image URL
        service_account: Service account email (REQUIRED)
        resources: Dict with cpu, memory_mb, timeout_seconds
    """
    return {
        "template": {
            "template": {
                "containers": [
                    {
                        "image": container_image,
                        "resources": {
                            "limits": {
                                "cpu": resources["cpu"],
                                "memory": f"{resources['memory_mb']}Mi"
                            }
                        }
                    }
                ],
                "timeout": f"{resources['timeout_seconds']}s",
                "serviceAccount": service_account
            }
        }
    }


def deploy_to_gcp(workflow_data):
    
    gcp_secret_key = os.getenv("GCP_SecretKey")
    
    if not gcp_secret_key:
        logger.error("GCP_SecretKey environment variable not set")
        sys.exit(1)
    
    from FaaSr_py.helpers.gcp_auth import refresh_gcp_access_token
    
    workflow_name = workflow_data.get("WorkflowName")
    
    if not workflow_name:
        logger.error("WorkflowName not specified in workflow file")
        sys.exit(1)
    
    gcp_actions = {}
    gcp_server_config = None
    gcp_server_name = None
    
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config.get("FaaSType", "")
        
        if faas_type.lower() == "googlecloud":
            gcp_actions[action_name] = action_data
            if not gcp_server_config:
                gcp_server_config = server_config.copy()
                gcp_server_name = server_name
    
    if not gcp_actions:
        logger.info("No actions found for GCP deployment")
        return
    
    gcp_server_config["SecretKey"] = gcp_secret_key
    
    temp_payload = {
        "ComputeServers": {
            gcp_server_name: gcp_server_config
        }
    }
    
    try:
        access_token = refresh_gcp_access_token(temp_payload, gcp_server_name)
        logger.info("Successfully authenticated with GCP")
    except Exception as e:
        logger.error(f"Failed to authenticate with GCP: {e}")
        sys.exit(1)
    
    endpoint = gcp_server_config.get("Endpoint", "run.googleapis.com/v2/projects/")
    namespace = gcp_server_config["Namespace"]
    region = gcp_server_config["Region"]
    
    if not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint}"
    
    base_url = f"{endpoint}{namespace}/locations/{region}/jobs"
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    for action_name, action_data in gcp_actions.items():
        job_name = f"{workflow_name}-{action_name}"
        
        logger.info(f"Registering GCP Cloud Run Job: {job_name}")
        
        container_image = workflow_data.get("ActionContainers", {}).get(action_name)
        
        if not container_image:
            logger.error(f"No container specified for action: {action_name}")
            sys.exit(1)

        service_account = gcp_server_config.get("ClientEmail")
        if not service_account:
            logger.error(
                f"ClientEmail (service account) is required for GoogleCloud server "
                f"but not found in ComputeServers configuration"
            )
            sys.exit(1)

        resources = get_gcp_resource_requirements(
            workflow_data=workflow_data,
            action_name=action_name,
            server_config=gcp_server_config
        )
        
        job_body = create_gcp_job_definition(
            container_image=container_image,
            service_account=service_account,
            resources=resources
        )
        
        create_url = base_url
        create_params = {"jobId": job_name}
        
        response = requests.post(
            create_url,
            json=job_body,
            headers=headers,
            params=create_params
        )
        
        if response.status_code in [200, 201]:
            logger.info(f"Successfully created Cloud Run Job: {job_name}")
        elif response.status_code == 409:
            logger.info(f"Job {job_name} already exists, updating...")
            update_url = f"{base_url}/{job_name}"
            
            response = requests.patch(
                update_url,
                json=job_body,
                headers=headers
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully updated Cloud Run Job: {job_name}")
            else:
                logger.error(f"Failed to update job {job_name}: {response.text}")
                sys.exit(1)
        else:
            logger.error(f"Failed to create job {job_name}: {response.text}")
            sys.exit(1)
    
    logger.info(f"Successfully registered {len(gcp_actions)} GCP Cloud Run Jobs")


def main():
    args = parse_arguments()
    workflow_data = read_workflow_file(args.workflow_file)

    # Store the workflow file path in the workflow data
    workflow_data["_workflow_file"] = args.workflow_file

    # Validate workflow for cycles and unreachable states
    print("Validating workflow for cycles and unreachable states...")
    try:
        faasr_gf.check_dag(workflow_data)
        print("Workflow validation passed")
    except SystemExit:
        print("Workflow validation failed - check logs for details")

    # Get all unique FaaSTypes from workflow data
    faas_types = set()
    for server in workflow_data.get("ComputeServers", {}).values():
        if "FaaSType" in server:
            faas_types.add(server["FaaSType"].lower())

    if not faas_types:
        print("Error: No FaaSType found in workflow file")
        sys.exit(1)

    print(f"Found FaaS platforms: {', '.join(faas_types)}")

    # Deploy to each platform found
    for faas_type in faas_types:
        print(f"\nDeploying to {faas_type}...")
        if faas_type == "lambda":
            deploy_to_aws(workflow_data)
        elif faas_type == "githubactions":
            deploy_to_github(workflow_data)
        elif faas_type == "openwhisk":
            deploy_to_ow(workflow_data)
        elif faas_type == "googlecloud":
            deploy_to_gcp(workflow_data)
        else:
            logger.error(f"Unsupported FaaSType: {faas_type}")
            #sys.exit(1) #todo: remove later


if __name__ == "__main__":
    main()
