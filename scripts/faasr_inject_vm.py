#!/usr/bin/env python3
"""
FaaSr VM Injection Tool

Augments workflows with VM start/stop actions based on strategy.
Strategy 1: Start at beginning, stop after all leaves complete.
"""

import argparse
import json
import sys
import logging
from pathlib import Path
from copy import deepcopy
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


class VMInjectionTool:
    """Tool to inject VM orchestration actions into FaaSr workflows."""
    
    def __init__(self, workflow_path, output_path=None):
        """
        Initialize tool.
        
        Args:
            workflow_path: Path to input workflow JSON
            output_path: Path for output (defaults to input_augmented.json)
        """
        self.workflow_path = Path(workflow_path)
        
        if output_path:
            self.output_path = Path(output_path)
        else:
            stem = self.workflow_path.stem
            suffix = self.workflow_path.suffix
            self.output_path = self.workflow_path.parent / f"{stem}_augmented{suffix}"
        
        self.workflow = None
        self.original_workflow = None
    
    def load_workflow(self):
        """Load and validate input workflow."""
        logger.info(f"Loading workflow from: {self.workflow_path}")
        
        if not self.workflow_path.exists():
            raise FileNotFoundError(f"Workflow file not found: {self.workflow_path}")
        
        with open(self.workflow_path, 'r') as f:
            self.workflow = json.load(f)
        
        self.original_workflow = deepcopy(self.workflow)
        
        logger.info("Workflow loaded successfully")
    
    def needs_vm(self):
        """Check if workflow has any VM-requiring actions."""
        if "ActionList" not in self.workflow:
            return False
        
        for action_name, action_config in self.workflow["ActionList"].items():
            if action_config.get("RequiresVM", False):
                logger.info(f"Found VM-requiring action: {action_name}")
                return True
        
        return False
    
    def find_entry_action(self):
        """
        Find the entry point action (no predecessors).
        
        Returns:
            str: Name of entry action
        """
        action_list = self.workflow.get("ActionList", {})
        
        # Build predecessor count
        predecessor_count = {name: 0 for name in action_list.keys()}
        
        for action_name, action_config in action_list.items():
            invoke_next = action_config.get("InvokeNext", [])
            
            # Handle different InvokeNext formats
            if isinstance(invoke_next, str):
                invoke_next = [invoke_next]
            elif isinstance(invoke_next, dict):
                # Conditional invocation - collect all possible next actions
                next_actions = []
                for condition, actions in invoke_next.items():
                    if isinstance(actions, list):
                        next_actions.extend(actions)
                    else:
                        next_actions.append(actions)
                invoke_next = next_actions
            
            for next_action in invoke_next:
                if next_action in predecessor_count:
                    predecessor_count[next_action] += 1
        
        # Find action(s) with zero predecessors
        entry_actions = [name for name, count in predecessor_count.items() if count == 0]
        
        if len(entry_actions) == 0:
            raise ValueError("No entry action found (cycle in workflow?)")
        elif len(entry_actions) > 1:
            raise ValueError(f"Multiple entry actions found: {entry_actions}. Workflow must have single entry point.")
        
        return entry_actions[0]
    
    def find_leaf_actions(self):
        """
        Find all leaf actions (empty InvokeNext).
        
        Returns:
            list: Names of leaf actions
        """
        action_list = self.workflow.get("ActionList", {})
        leaves = []
        
        for action_name, action_config in action_list.items():
            invoke_next = action_config.get("InvokeNext", [])
            
            # Check if empty
            if not invoke_next or invoke_next == []:
                leaves.append(action_name)
        
        if not leaves:
            raise ValueError("No leaf actions found - workflow must have terminal nodes")
        
        return leaves
    
    def find_github_server(self):
        """
        Find a GitHub Actions server for injected actions.
        
        Returns:
            str: Server name
        """
        if "ComputeServers" not in self.workflow:
            raise ValueError("No ComputeServers defined in workflow")
        
        for server_name, server_config in self.workflow["ComputeServers"].items():
            if server_config.get("FaaSType") == "GitHubActions":
                return server_name
        
        raise ValueError("No GitHub Actions server found. VM workflows require GitHub Actions.")
    
    def find_container_for_server(self, server_name):
        """
        Find a container image for the given server.
        
        Args:
            server_name: Server name
            
        Returns:
            str: Container image or None
        """
        action_containers = self.workflow.get("ActionContainers", {})
        action_list = self.workflow.get("ActionList", {})
        
        # Find any action using this server
        for action_name, action_config in action_list.items():
            if action_config.get("FaaSServer") == server_name:
                container = action_containers.get(action_name)
                if container:
                    return container
        
        return None
    
    def inject_vm_actions_strategy1(self):
        """
        Inject VM start at beginning and stop after all leaves.
        
        Strategy 1: Start-at-beginning, stop-at-end
        """
        logger.info("Applying Strategy 1: Start at beginning, stop at end")
        
        # Validate prerequisites
        if "VMConfig" not in self.workflow:
            raise ValueError("VMConfig required for VM workflows")
        
        # Find graph structure
        entry_action = self.find_entry_action()
        leaf_actions = self.find_leaf_actions()
        github_server = self.find_github_server()
        container = self.find_container_for_server(github_server)
        
        logger.info(f"Entry action: {entry_action}")
        logger.info(f"Leaf actions: {leaf_actions}")
        logger.info(f"GitHub server: {github_server}")
        
        # Define injected action names
        vm_start_name = "faasr-vm-start"
        vm_stop_name = "faasr-vm-stop"
        
        # Check for name conflicts
        if vm_start_name in self.workflow["ActionList"]:
            raise ValueError(f"Action name conflict: {vm_start_name} already exists")
        if vm_stop_name in self.workflow["ActionList"]:
            raise ValueError(f"Action name conflict: {vm_stop_name} already exists")
        
        # Create VM start action
        self.workflow["ActionList"][vm_start_name] = {
            "FunctionName": "vm_start",
            "FaaSServer": github_server,
            "Type": "Python",
            "RequiresVM": False,
            "InvokeNext": [entry_action],
            "_faasr_builtin": True
        }
        
        # Create VM stop action
        self.workflow["ActionList"][vm_stop_name] = {
            "FunctionName": "vm_stop",
            "FaaSServer": github_server,
            "Type": "Python",
            "RequiresVM": False,
            "InvokeNext": [],
            "_faasr_builtin": True
        }
        
        # Modify leaf actions to point to stop
        for leaf_name in leaf_actions:
            self.workflow["ActionList"][leaf_name]["InvokeNext"] = [vm_stop_name]
            logger.info(f"Modified leaf '{leaf_name}' to invoke VM stop")
        
        # Add containers for injected actions
        if "ActionContainers" not in self.workflow:
            self.workflow["ActionContainers"] = {}
        
        if container:
            self.workflow["ActionContainers"][vm_start_name] = container
            self.workflow["ActionContainers"][vm_stop_name] = container
        
        # Update FunctionInvoke to point to vm_start
        self.workflow["FunctionInvoke"] = vm_start_name
        logger.info(f"Updated FunctionInvoke to: {vm_start_name}")
        
        logger.info("VM actions injected successfully")
    
    def save_workflow(self):
        """Save augmented workflow to output file."""
        logger.info(f"Saving augmented workflow to: {self.output_path}")
        
        with open(self.output_path, 'w') as f:
            json.dump(self.workflow, f, indent=4)
        
        logger.info("Augmented workflow saved")
    
    def run(self):
        """Execute full injection process."""
        try:
            # Load and validate input
            self.load_workflow()
            
            # Check if VM injection needed
            if not self.needs_vm():
                logger.info("Workflow does not require VM - no injection needed")
                logger.info("Copying original workflow to output unchanged")
                self.save_workflow()
                return True
            
            # Inject VM actions
            self.inject_vm_actions_strategy1()
            
            # Save
            self.save_workflow()
            
            logger.info("=" * 60)
            logger.info("SUCCESS: Workflow augmented with VM orchestration")
            logger.info(f"Input:  {self.workflow_path}")
            logger.info(f"Output: {self.output_path}")
            logger.info("=" * 60)
            logger.info("Next steps:")
            logger.info(f"1. Review: {self.output_path}")
            logger.info(f"2. Register: python scripts/register_workflow.py --workflow-file {self.output_path}")
            logger.info(f"3. Invoke: python scripts/invoke_workflow.py --workflow-file {self.output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to inject VM actions: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Inject VM orchestration actions into FaaSr workflows"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input workflow JSON file"
    )
    parser.add_argument(
        "--output",
        help="Path to output workflow JSON file (default: input_augmented.json)"
    )
    parser.add_argument(
        "--strategy",
        type=int,
        default=1,
        choices=[1],
        help="VM orchestration strategy (currently only 1 supported)"
    )
    
    args = parser.parse_args()

    if os.getenv("GITHUB_ACTIONS") == "true":
          logger.info("Running in GitHub Actions environment")
    
    if args.strategy != 1:
        logger.error(f"Strategy {args.strategy} not yet implemented")
        sys.exit(1)
    
    tool = VMInjectionTool(args.input, args.output)
    success = tool.run()

    if os.getenv("GITHUB_ACTIONS") == "true":
          output_file = tool.output_path
          print(f"::set-output name=augmented_file::{output_file}")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
