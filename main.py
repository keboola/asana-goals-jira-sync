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
from src.sync_manager import SyncManager

# Required configuration parameters
REQUIRED_PARAMETERS = [
    'jira_base_url',
    'jira_email',
    '#jira_token',
    '#asana_token'
]

# Asana scope parameters (at least one must be specified)
ASANA_SCOPE_PARAMETERS = [
    'asana_goal_gids',
    'asana_project_gids',
    'asana_team_gids',
    'asana_workspace_gids'
]


def normalize_to_list(value):
    """Convert string or list to list"""
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    return value or []


def validate_asana_scope(params: dict) -> None:
    """
    Validate that at least one Asana scope is specified
    """
    has_scope = False
    for scope_param in ASANA_SCOPE_PARAMETERS:
        scope_values = normalize_to_list(params.get(scope_param))
        if scope_values:
            has_scope = True
            break
    
    if not has_scope:
        raise UserException(
            f"Must specify at least one Asana scope: {', '.join(ASANA_SCOPE_PARAMETERS)}"
        )


def run_sync(cfg: CommonInterface) -> None:
    """
    Main execution code for Jira-Asana synchronization
    """
    # Validate required configuration parameters  
    cfg.validate_configuration_parameters(REQUIRED_PARAMETERS)
    
    params = cfg.configuration.parameters
    
    # Validate Asana scope
    validate_asana_scope(params)
    
    logging.info("🚀 Starting Jira-Asana synchronization")

    try:
        # Build configuration objects from Keboola parameters
        jira_config = {
            'base_url': params['jira_base_url'],
            'email': params['jira_email'],
            'token': params['#jira_token']
        }

        asana_config = {
            'token': params['#asana_token']
        }

        # Load status mapping (with defaults if not provided)
        status_mapping = params.get('status_mapping', {
            'green': 'on_track',
            'amber': 'at_risk',
            'red': 'off_track',
            "Temporary paused": "dropped"
        })
        
        # Check dry run mode
        dry_run = params.get('dry_run', False)
        if dry_run:
            logging.info("🔍 DRY RUN MODE - No changes will be made")

        # Create sync manager
        sync_manager = SyncManager(
            jira_config=jira_config,
            asana_config=asana_config,
            status_mapping=status_mapping,
            dry_run=dry_run
        )

        # Process Asana scopes in priority order
        total_processed = 0
        
        # 1. Specific goals (highest priority)
        goal_gids = normalize_to_list(params.get('asana_goal_gids'))
        for goal_gid in goal_gids:
            logging.info(f"📍 Synchronizing goal ID: {goal_gid}")
            count = sync_manager.sync_goal_by_id(goal_gid)
            total_processed += count

        # 2. Projects (medium priority)  
        project_gids = normalize_to_list(params.get('asana_project_gids'))
        for project_gid in project_gids:
            logging.info(f"📍 Synchronizing project ID: {project_gid}")
            count = sync_manager.sync_goals_in_project(project_gid)
            total_processed += count

        # 3. Teams (lower priority)
        team_gids = normalize_to_list(params.get('asana_team_gids'))
        for team_gid in team_gids:
            logging.info(f"📍 Synchronizing team ID: {team_gid}")
            count = sync_manager.sync_goals_in_team(team_gid)
            total_processed += count

        # 4. Workspaces (lowest priority)
        workspace_gids = normalize_to_list(params.get('asana_workspace_gids'))
        for workspace_gid in workspace_gids:
            logging.info(f"📍 Synchronizing workspace ID: {workspace_gid}")
            count = sync_manager.sync_goals_in_workspace(workspace_gid)
            total_processed += count

        # Log results
        if total_processed > 0:
            if dry_run:
                logging.info(f"✅ Dry run completed: Found {total_processed} goals that would be processed")
            else:
                logging.info(f"✅ Synchronization completed successfully: {total_processed} goals processed")
        else:
            logging.warning("⚠️ No goals were found for synchronization - check configuration")

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
