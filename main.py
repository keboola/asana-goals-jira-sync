#!/usr/bin/env python3
"""
Jira-Asana Synchronization Component for Keboola
Goal-oriented synchronization between Jira tickets and Asana tasks.
"""

import sys
import os
import pathlib
import logging
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.join(pathlib.Path(__file__).parent.parent))

from keboola.component.interface import CommonInterface
from keboola.component import UserException
from sync_manager import SyncManager

# Required configuration parameters
REQUIRED_PARAMETERS = [
    'jira_base_url',
    'jira_project_key', 
    'jira_email',
    'jira_token',
    'asana_workspace_gid',
    'asana_team_gid',
    'asana_token',
    'asana_jira_ticket_field'
]


def validate_configuration(cfg: CommonInterface) -> None:
    """
    Validate required configuration parameters
    """
    params = cfg.get_parameters()
    missing_params = [param for param in REQUIRED_PARAMETERS if param not in params]
    
    if missing_params:
        raise UserException(f"Missing required parameters: {', '.join(missing_params)}")



def run_sync(cfg: CommonInterface) -> None:
    """
    Main execution code for Jira-Asana synchronization
    """
    # Validate required configuration parameters
    validate_configuration(cfg)
    
    params = cfg.get_parameters()
    logging.info("🚀 Starting Jira-Asana synchronization")

    try:
        # Build configuration objects from Keboola parameters
        jira_config = {
            'base_url': params['jira_base_url'],
            'project_key': params['jira_project_key'],
            'email': params['jira_email'],
            'token': params['jira_token']
        }

        asana_config = {
            'workspace_gid': params['asana_workspace_gid'],
            'team_gid': params['asana_team_gid'],
            'token': params['asana_token'],
            'jira_ticket_field': params['asana_jira_ticket_field']
        }

        # Load status mapping (with defaults if not provided)
        status_mapping = params.get('status_mapping', {
            'To Do': 'New',
            'In Progress': 'In Progress',
            'Done': 'Complete',
            'Blocked': 'On Hold'
        })

        # Create sync manager
        sync_manager = SyncManager(
            jira_config=jira_config,
            asana_config=asana_config,
            status_mapping=status_mapping
        )

        # Determine sync mode from configuration
        goal_name = params.get('goal_name')
        
        if goal_name:
            # Sync specific goal
            logging.info(f"📍 Synchronizing specific goal: {goal_name}")
            success_count = sync_manager.sync_goal(goal_name)
        else:
            # Sync all goals (default)
            logging.info("📍 Synchronizing all goals in team")
            success_count = sync_manager.sync_all_goals()

        # Log results
        if success_count > 0:
            logging.info(f"✅ Synchronization completed successfully: {success_count} goals processed")
        else:
            logging.warning("⚠️ No goals were synchronized - check configuration")

        # Save state for next run (optional)
        cfg.write_state_file({
            "last_sync_goals": success_count,
            "last_sync_timestamp": datetime.now().isoformat()
        })

    except Exception as e:
        logging.error(f"❌ Synchronization failed: {str(e)}")
        raise UserException(f"Sync failed: {str(e)}")


if __name__ == "__main__":
    """
    Main entrypoint for Keboola component
    """
    try:
        # Initialize common interface
        cfg = CommonInterface()
        
        # Run synchronization
        run_sync(cfg)
            
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2) 