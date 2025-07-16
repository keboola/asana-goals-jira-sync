"""
Asana API client for interacting with Asana workspace
Handles authentication, project/task queries, and updates
"""

import requests
import re


class AsanaAPI:
    """Asana API client using Personal Access Token"""
    
    def __init__(self, config: dict[str, any]):
        self.token = config.get('token')
        self.team_gid = config.get('team_gid')
        self.project_gid = config.get('project_gid')
        
        if not self.token:
            raise ValueError("Asana token is required")
        
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        self.base_url = 'https://app.asana.com/api/1.0'
    
    def _make_request(self, method: str, endpoint: str, params: dict[str, any] = None, data: dict[str, any] = None) -> dict[str, any] | None:
        """Make HTTP request to Asana API"""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=self.headers, params=params)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=self.headers, json=data)
            elif method.upper() == 'PUT':
                response = requests.put(url, headers=self.headers, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"❌ Asana API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    print(f"❌ Error details: {error_data}")
                except:
                    print(f"❌ Response text: {e.response.text}")
            return None
    
    def get_goal_by_id(self, goal_gid: str) -> dict[str, any] | None:
        """Get specific goal by ID"""
        endpoint = f"goals/{goal_gid}"
        params = {
            'opt_fields': 'gid,name,status,team,project'
        }
        
        response = self._make_request('GET', endpoint, params=params)
        if response and 'data' in response:
            return response['data']
        return None

    def get_goals_in_team(self, team_gid: str) -> list[dict[str, any]]:
        """Get all goals in team"""
        endpoint = f"goals"
        params = {
            'team': team_gid,
            'opt_fields': 'gid,name,status'
        }
        
        response = self._make_request('GET', endpoint, params=params)
        if response and 'data' in response:
            return response['data']
        return []

    def get_goals_in_project(self, project_gid: str) -> list[dict[str, any]]:
        """Get all goals linked to project"""
        endpoint = f"goals"
        params = {
            'project': project_gid,
            'opt_fields': 'gid,name,status'
        }
        
        response = self._make_request('GET', endpoint, params=params)
        if response and 'data' in response:
            return response['data']
        return []

    def get_goals_in_workspace(self, workspace_gid: str) -> list[dict[str, any]]:
        """Get all goals in workspace"""
        endpoint = f"goals"
        params = {
            'workspace': workspace_gid,
            'opt_fields': 'gid,name,status'
        }
        
        response = self._make_request('GET', endpoint, params=params)
        if response and 'data' in response:
            return response['data']
        return []

    # Legacy method - keeping for backward compatibility
    def get_team_goals(self, team_gid: str) -> list[dict[str, any]]:
        """Get all goals in team (legacy method)"""
        return self.get_goals_in_team(team_gid)

    def get_goal_by_name(self, team_gid: str, goal_name: str) -> dict[str, any] | None:
        """Find goal by name in team"""
        goals = self.get_goals_in_team(team_gid)
        for goal in goals:
            if goal.get('name') == goal_name:
                return goal
        return None
    
    def get_project_tasks_with_jira_field(self, project_gid: str) -> list[dict[str, any]]:
        """Get tasks from project that have Jira ticket field filled"""
        # Get all tasks from project
        endpoint = f"projects/{project_gid}/tasks"
        params = {
            'opt_fields': f'gid,name,completed,custom_fields.gid,custom_fields.name,custom_fields.text_value,custom_fields.number_value'
        }
        
        response = self._make_request('GET', endpoint, params=params)
        if not response or 'data' not in response:
            return []
        
        all_tasks = response['data']
        jira_tasks = []
        
        for task in all_tasks:
            # Check attachments for Jira ticket
            task_details = self.get_task_details(task['gid'])
            if task_details and task_details.get('jira_ticket'):
                task['jira_ticket'] = task_details['jira_ticket']
                jira_tasks.append(task)
        
        return jira_tasks
    

    
    def get_latest_sync_comment(self, task_gid: str) -> dict[str, any] | None:
        """Get the latest sync comment from task (to determine last sync time)"""
        endpoint = f"tasks/{task_gid}/stories"
        params = {
            'opt_fields': 'gid,text,created_at,type'
        }
        
        response = self._make_request('GET', endpoint, params=params)
        if not response or 'data' not in response:
            return None
        
        # Look for comments that contain sync markers
        sync_comments = []
        for story in response['data']:
            if (story.get('type') == 'comment' and 
                story.get('text') and 
                '🔄 **Jira Sync Update**' in story.get('text')):
                sync_comments.append(story)
        
        # Return the most recent sync comment
        if sync_comments:
            return max(sync_comments, key=lambda x: x['created_at'])
        
        return None
    
    def get_goal_tasks(self, goal_gid: str) -> list[dict[str, any]]:
        """Get tasks associated with a goal using Goal Relationships API"""
        endpoint = "goal_relationships"
        
        # Request with supported_goal parameter to get relationships for this goal
        params = {
            'supported_goal': goal_gid,
            'opt_fields': 'gid,resource_subtype,supporting_resource.gid,supporting_resource.resource_type,supporting_resource.name,supporting_resource.custom_fields.gid,supporting_resource.custom_fields.name,supporting_resource.custom_fields.text_value,supporting_resource.custom_fields.number_value'
        }
        
        print(f"      🔍 Fetching goal relationships for goal {goal_gid}")
        
        response = self._make_request('GET', endpoint, params=params)
        
        if not response or 'data' not in response:
            return []
        
        goal_relationships = response['data']
        
        # Filter and extract tasks from relationships
        tasks = []
        for relationship in goal_relationships:
            supporting_resource = relationship.get('supporting_resource', {})
            if supporting_resource.get('resource_type') == 'task':
                tasks.append({
                    'gid': supporting_resource.get('gid'),
                    'name': supporting_resource.get('name'),
                    'custom_fields': supporting_resource.get('custom_fields', [])
                })
        
        return tasks
    
    def get_goal_projects(self, goal_gid: str) -> list[dict[str, any]]:
        """Get projects associated with a goal using Goal Relationships API"""
        endpoint = "goal_relationships"
        
        # Request with supported_goal parameter
        params = {
            'supported_goal': goal_gid,
            'opt_fields': 'gid,resource_subtype,supporting_resource.gid,supporting_resource.resource_type,supporting_resource.name'
        }
        
        response = self._make_request('GET', endpoint, params=params)
        
        if not response or 'data' not in response:
            return []
        
        goal_relationships = response['data']
        
        # Filter and extract projects from relationships
        projects = []
        for relationship in goal_relationships:
            supporting_resource = relationship.get('supporting_resource', {})
            if supporting_resource.get('resource_type') == 'project':
                projects.append({
                    'gid': supporting_resource.get('gid'),
                    'name': supporting_resource.get('name')
                })
        
        return projects
    
    def get_task_details(self, task_gid: str) -> dict[str, any] | None:
        """Get full task details including custom fields and check for Jira ticket"""
        endpoint = f"tasks/{task_gid}"
        
        # Get task with custom fields
        params = {
            'opt_fields': 'gid,name,custom_fields.gid,custom_fields.name,custom_fields.text_value,custom_fields.number_value'
        }
        
        response = self._make_request('GET', endpoint, params=params)
        
        if not response or 'data' not in response:
            return None
        
        task_data = response['data']
        
        # Check attachments for Jira ticket
        jira_ticket = self._get_jira_ticket_from_attachments(task_gid)
        
        # Add jira_ticket to task data if found
        if jira_ticket:
            task_data['jira_ticket'] = jira_ticket
        
        return task_data
    
    def _get_jira_ticket_from_attachments(self, task_gid: str) -> str | None:
        """Check task attachments for Jira ticket numbers"""
        endpoint = f"tasks/{task_gid}/attachments"
        
        response = self._make_request('GET', endpoint)
        
        if not response or 'data' not in response:
            return None
        
        attachments = response['data']
        
        # Look for Jira ticket pattern in attachment names
        jira_ticket_pattern = r'([A-Z]+-\d+)'
        
        for attachment in attachments:
            attachment_name = attachment.get('name', '')
            match = re.search(jira_ticket_pattern, attachment_name)
            if match:
                return match.group(1)
        
        return None
    
    def create_goal_status_update(self, goal_gid: str, title: str, text: str, status_type: str = "on_track") -> bool:
        """Create a status update for a goal"""
        endpoint = "status_updates"
        
        data = {
            'data': {
                'parent': goal_gid,
                'title': title,
                'text': text,
                'status_type': status_type
            }
        }
        
        response = self._make_request('POST', endpoint, data=data)
        return response is not None
    
    def get_latest_goal_status_update(self, goal_gid: str) -> dict[str, any] | None:
        """Get the most recent status update for a goal"""
        endpoint = "status_updates"
        params = {
            'parent': goal_gid,
            'opt_fields': 'gid,title,text,created_at,status_type'
        }
        
        response = self._make_request('GET', endpoint, params=params)
        
        if not response or 'data' not in response:
            return None
        
        status_updates = response['data']
        
        if status_updates:
            # Sort by created_at to ensure we get the latest one
            status_updates.sort(key=lambda x: x['created_at'], reverse=True)
            return status_updates[0]
        
        return None 