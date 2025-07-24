"""
Asana API client for interacting with Asana workspace
Handles authentication, project/task queries, and updates
"""
import json
import re
import asana
from asana.rest import ApiException

from bs4 import BeautifulSoup


class AsanaAPI:
    """Asana API client using Personal Access Token and official asana Python client (5.x style)"""

    def __init__(self, config: dict[str, any]):
        self.token = config.get('token')
        self.team_gid = config.get('team_gid')
        self.project_gid = config.get('project_gid')

        if not self.token:
            raise ValueError("Asana token is required")

        configuration = asana.Configuration()
        configuration.access_token = self.token
        self.api_client = asana.ApiClient(configuration)
        # API objects
        self.tasks_api = asana.TasksApi(self.api_client)
        self.goals_api = asana.GoalsApi(self.api_client)
        self.goal_relationships_api = asana.GoalRelationshipsApi(self.api_client)
        self.attachments_api = asana.AttachmentsApi(self.api_client)
        self.stories_api = asana.StoriesApi(self.api_client)
        self.status_updates_api = asana.StatusUpdatesApi(self.api_client)

    def get_goal_by_id(self, goal_gid: str) -> dict[str, any] | None:
        try:
            goal = self.goals_api.get_goal(goal_gid, opts={"opt_fields": "gid,name,status,team,project"})
            return goal
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get goal by ID {goal_gid}: {e}")

    def get_goals_in_team(self, team_gid: str) -> list[dict[str, any]]:
        try:
            goals = self.goals_api.get_goals({"team": team_gid, "opt_fields": "gid,name,status"})
            return list(goals)
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get goals in team {team_gid}: {e}")

    def get_goals_in_project(self, project_gid: str) -> list[dict[str, any]]:
        try:
            goals = self.goals_api.get_goals({"project": project_gid, "opt_fields": "gid,name,status"})
            return list(goals)
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get goals in project {project_gid}: {e}")

    def get_goals_in_workspace(self, workspace_gid: str) -> list[dict[str, any]]:
        try:
            goals = self.goals_api.get_goals({"workspace": workspace_gid, "opt_fields": "gid,name,status"})
            return list(goals)
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get goals in workspace {workspace_gid}: {e}")

    def get_team_goals(self, team_gid: str) -> list[dict[str, any]]:
        return self.get_goals_in_team(team_gid)

    def get_goal_by_name(self, team_gid: str, goal_name: str) -> dict[str, any] | None:
        goals = self.get_goals_in_team(team_gid)
        for goal in goals:
            if goal.get('name') == goal_name:
                return goal
        return None

    def get_project_tasks_with_jira_field(self, project_gid: str) -> list[dict[str, any]]:
        try:
            tasks = self.tasks_api.get_tasks({"project": project_gid,
                                              "opt_fields": "gid,name,completed,custom_fields.gid,custom_fields.name,custom_fields.text_value,custom_fields.number_value"})
            tasks = list(tasks)
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get project tasks with Jira field for project {project_gid}: {e}")
        jira_tasks = []
        for task in tasks:
            task_details = self.get_task_details(task['gid'])
            if task_details and task_details.get('jira_ticket'):
                task['jira_ticket'] = task_details['jira_ticket']
                jira_tasks.append(task)
        return jira_tasks

    def get_latest_sync_comment(self, task_gid: str) -> dict[str, any] | None:
        try:
            stories = self.stories_api.get_stories_for_task(task_gid, opts={"opt_fields": "gid,text,created_at,type"})
            stories = list(stories)
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get latest sync comment for task {task_gid}: {e}")
        sync_comments = [story for story in stories if
                         story.get('type') == 'comment' and story.get('text') and '🔄 **Jira Sync Update**' in story.get(
                             'text')]
        if sync_comments:
            return max(sync_comments, key=lambda x: x['created_at'])
        return None

    def get_goal_tasks(self, goal_gid: str) -> list[dict[str, any]]:
        try:
            relationships = self.goal_relationships_api.get_goal_relationships(goal_gid, {
                "opt_fields": "gid,resource_subtype,supporting_resource.gid,supporting_resource.resource_type,supporting_resource.name,supporting_resource.custom_fields.gid,supporting_resource.custom_fields.name,supporting_resource.custom_fields.text_value,supporting_resource.custom_fields.number_value"})
            relationships = list(relationships)
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get goal tasks for goal {goal_gid}: {e}")
        tasks = []
        for relationship in relationships:
            supporting_resource = relationship.get('supporting_resource', {})
            if supporting_resource.get('resource_type') == 'task':
                tasks.append({
                    'gid': supporting_resource.get('gid'),
                    'name': supporting_resource.get('name'),
                    'custom_fields': supporting_resource.get('custom_fields', [])
                })
        return tasks

    def get_goal_projects(self, goal_gid: str) -> list[dict[str, any]]:
        try:
            relationships = self.goal_relationships_api.get_goal_relationships(goal_gid, {
                "opt_fields": "gid,resource_subtype,supporting_resource.gid,supporting_resource.resource_type,supporting_resource.name"})
            relationships = list(relationships)
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get goal projects for goal {goal_gid}: {e}")
        projects = []
        for relationship in relationships:
            supporting_resource = relationship.get('supporting_resource', {})
            if supporting_resource.get('resource_type') == 'project':
                projects.append({
                    'gid': supporting_resource.get('gid'),
                    'name': supporting_resource.get('name')
                })
        return projects

    def get_task_details(self, task_gid: str) -> dict[str, any] | None:
        try:
            task_data = self.tasks_api.get_task(task_gid, opts={
                "opt_fields": "gid,name,custom_fields.gid,custom_fields.name,custom_fields.text_value,custom_fields.number_value"})
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get task details for task {task_gid}: {e}")
        jira_ticket = self._get_jira_ticket_from_attachments(task_gid)
        if jira_ticket:
            task_data['jira_ticket'] = jira_ticket
        return task_data

    def _get_jira_ticket_from_attachments(self, task_gid: str) -> str | None:
        try:
            attachments = self.attachments_api.get_attachments_for_object(task_gid, {})
            attachments = list(attachments)
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get attachments for task {task_gid}: {e}")
        jira_ticket_pattern = r'([A-Z]+-\d+)'
        for attachment in attachments:
            attachment_name = attachment.get('name', '')
            match = re.search(jira_ticket_pattern, attachment_name)
            if match:
                return match.group(1)
        return None

    def create_goal_status_update(self, goal_gid: str, title: str, text: str, status_type: str = "on_track") -> bool:
        asana_text = self._convert_html_to_asana_format(text)
        print("Original text:", text)
        print("Converted to Asana format:", asana_text)
        print('--------------------------------')
        try:
            body = {
                "data": {
                    'parent': goal_gid,
                    'title': title,
                    'html_text': asana_text,
                    'status_type': status_type
                }
            }
            opts = {}
            api_response = self.status_updates_api.create_status_for_object(body, opts)
            print(api_response)
            return True
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to create Asana goal status update: {e}")

    def update_goal_metric(self, goal_gid: str, value: int) -> bool:
        try:
            body = {
                "data": {
                    "current_number_value": value/100.0,  # Asana expects a float value
                }
            }
            print(f"Updating goal metric for goal {goal_gid} with {body}")
            self.goals_api.update_goal_metric(goal_gid=goal_gid, body=body, opts={})
            return True
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to update goal metric for goal {goal_gid}: {e}")

    def get_latest_goal_status_update(self, goal_gid: str) -> dict[str, any] | None:
        try:
            status_updates = self.status_updates_api.get_statuses_for_object(
                goal_gid,
                opts={"opt_fields": "gid,title,text,created_at,status_type,html_text"},
                header_params={'asana-enable': 'status_updates_html'}
            )
            status_updates = list(status_updates)
        except ApiException as e:
            print(f"❌ Asana API request failed: {e}")
            raise RuntimeError(f"Failed to get latest goal status update for goal {goal_gid}: {e}")
        if status_updates:
            status_updates.sort(key=lambda x: x['created_at'], reverse=True)
            return status_updates[0]
        return None

    def _convert_html_to_asana_format(self, html_content: str) -> str:
        """
        Convert HTML content to Asana-compatible rich text format for goal status updates.
        Goal status updates support only: <p>, <strong>, <em>, <a>, <ul>, <ol>, <li>
        """
        return self._convert_html_to_asana_format_impl(html_content)
    
    def _convert_html_to_asana_format_impl(self, html_content: str) -> str:
        """
        Convert HTML content using BeautifulSoup for reliable parsing.
        """
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove unsupported tags but keep their content
        unsupported_tags = ['script', 'style', 'meta', 'head', 'html', 'title', 'span', 'img']
        for tag in unsupported_tags:
            for element in soup.find_all(tag):
                element.unwrap()  # Remove tag but keep content
        
        # Convert headers to strong text
        for header in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            new_tag = soup.new_tag('strong')
            new_tag.string = header.get_text()
            header.replace_with(new_tag)
        
        # Convert <b> to <strong>
        for b_tag in soup.find_all('b'):
            new_tag = soup.new_tag('strong')
            new_tag.string = b_tag.get_text()
            b_tag.replace_with(new_tag)
        
        # Convert <i> to <em>
        for i_tag in soup.find_all('i'):
            new_tag = soup.new_tag('em')
            new_tag.string = i_tag.get_text()
            i_tag.replace_with(new_tag)
        
        # Convert <br> to newlines
        for br in soup.find_all('br'):
            br.replace_with('\n')
        
        # Clean up links - keep only href, remove links without href
        for a_tag in soup.find_all('a'):
            href = a_tag.get('href')
            if not href or not href.strip():
                # Remove link tag but keep content
                a_tag.unwrap()
            else:
                # Keep only href attribute, remove all others
                a_tag.attrs = {'href': href}
        
        # Remove all attributes except href from all tags
        for tag in soup.find_all():
            if tag.name == 'a':
                # Keep href for links
                href = tag.get('href')
                tag.attrs = {'href': href} if href else {}
            else:
                # Remove all attributes for other tags
                tag.attrs = {}
        
        # Remove <p> and <div> tags but keep content
        for tag in soup.find_all(['p', 'div']):
            tag.unwrap()
        
        # Clean up excessive newlines and whitespace
        text = str(soup)
        # Remove body tags if present
        text = text.replace('<body>', '').replace('</body>', '')
        # Clean up whitespace
        text = '\n'.join(line.strip() for line in text.split('\n') if line.strip())
        
        # Wrap in body tag as required by Asana
        return f'<body>{text}</body>'
