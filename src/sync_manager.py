"""
Main synchronization manager for Jira-Asana integration
Coordinates the synchronization logic between both platforms.
"""
import re
from datetime import datetime, timedelta
from dateutil import parser
from bs4 import BeautifulSoup

from .jira_api import JiraAPI
from .asana_api import AsanaAPI


class SyncManager:
    """Goal-oriented synchronization manager"""

    def __init__(self, jira_config: dict[str, any], asana_config: dict[str, any],
                 status_mapping: dict[str, str], dry_run: bool = False):
        self.jira = JiraAPI(jira_config)
        self.asana = AsanaAPI(asana_config)
        self.status_mapping = status_mapping
        self.dry_run = dry_run

        if self.dry_run:
            print("🔍 DRY RUN MODE - No changes will be made")

    def create_goal_status_update(self, goal_gid: str, goal_name: str, jira_tickets_info: list[dict[str, any]]) -> bool:
        """Create a status update for a goal with Jira ticket information"""
        print(f"\n🎯 Checking goal status for: {goal_name}")

        # Get the latest goal status update to determine since when to fetch comments
        latest_status = self.asana.get_latest_goal_status_update(goal_gid)
        since_date = datetime.now() - timedelta(days=7)  # Default to 7 days ago

        if latest_status:
            created_at_str = latest_status['created_at']
            # Keep Asana time in UTC for comparison
            since_date = parser.parse(created_at_str)
            if since_date.tzinfo is not None:
                since_date = since_date.replace(tzinfo=None)  # Make naive for comparison
        else:
            print(f"   📅 Creating initial status")

        all_new_comments, current_ticket_statuses = self.get_comments_and_statuses(jira_tickets_info, since_date)

        # Determine if we need to create a new status update
        has_new_activity = len(all_new_comments) > 0  # Any new comments trigger update
        is_first_status = latest_status is None

        if not has_new_activity and not is_first_status:
            print(f"   ℹ️  No changes since last update")
            return True  # Return True because no update was needed (success case)

        print(f"   ✅ Creating status update")

        status_type = self.set_status_type(jira_tickets_info)

        # Create simple title
        title = "Status Sync"

        status_text = self._build_status_text_html(all_new_comments, jira_tickets_info)
        goal_metric_value = jira_tickets_info[0].get('jira_goal_completion')
        # Create status update using Asana API
        if self.dry_run:
            print(f"   🔍 DRY RUN: Would create status update:")
            print(f"      Title: {title}")
            print(f"      Status: {status_type}")
            print(f"      Text preview: {status_text[:200]}...")
            return True
        else:
            # This will raise an exception if it fails
            self.asana.create_goal_status_update(
                goal_gid=goal_gid,
                title=title,
                text=status_text,
                status_type=status_type
            )
            if goal_metric_value:
                self.asana.update_goal_metric(
                    goal_gid=goal_gid,
                    value=int(goal_metric_value)
                )
            print(f"   ✅ Goal status update created")
            return True

    def set_status_type(self, jira_tickets_info):
        ticket_health_indicator = jira_tickets_info[0].get('jira_health_indicator', 'on_track')
        # status_mapping from config is a dict mapping Jira statuses to Asana status types
        status_type = self.status_mapping.get(ticket_health_indicator)
        if not status_type:
            # Default to "On Track" if no mapping found
            print(f"   ⚠️  No status mapping found for '{ticket_health_indicator}', defaulting to 'on_track'")
            status_type = 'on_track'
        else:
            print(f"   → Mapped Jira status '{ticket_health_indicator}' to Asana status type '{status_type}'")
        return status_type

    def get_comments_and_statuses(self, jira_tickets_info, since_date):
        # Collect new comments and status changes from all Jira tickets
        all_new_comments = []
        current_ticket_statuses = {}
        for ticket_info in jira_tickets_info:
            jira_ticket = ticket_info['jira_ticket']
            current_ticket_statuses[jira_ticket] = ticket_info['jira_health_indicator']

            # Get new comments from Jira
            comments = self.jira.get_comments_since(jira_ticket, since_date)
            for comment in comments:
                all_new_comments.append({
                    'jira_ticket': jira_ticket,
                    'task_name': ticket_info['task_name'],
                    'author': comment['author'],
                    'created': comment['created'],
                    'text': comment['rendered_body']  # self._extract_text_from_jira_comment(comment['body'])
                })
        # Sort comments by date (newest first)
        all_new_comments.sort(key=lambda x: x['created'], reverse=True)
        return all_new_comments, current_ticket_statuses

    @staticmethod
    def _build_status_text_html(all_new_comments, jira_tickets_info) -> str:
        """
        Build HTML status text using BeautifulSoup for reliable HTML generation.
        """
        # Create BeautifulSoup object
        soup = BeautifulSoup('<body></body>', 'html.parser')
        body = soup.body
        
        # Add header
        header = soup.new_tag('strong')
        header.string = "🚀 Automatic Activity Update"
        body.append(header)
        body.append(soup.new_string('\n'))
        
        for ticket_info in jira_tickets_info:
            jira_ticket = ticket_info['jira_ticket']
            jira_status = ticket_info['jira_health_indicator']
            task_name = ticket_info['task_name']
            jira_url = f"https://keboola.atlassian.net/browse/{jira_ticket}"
            
            # Create task link
            task_link = soup.new_tag('a', href=jira_url)
            task_link.string = task_name
            body.append(task_link)
            body.append(soup.new_string('\n'))
            
            # Add status
            status_label = soup.new_tag('strong')
            status_label.string = "Status: "
            body.append(status_label)
            body.append(soup.new_string(f"{jira_status}\n"))
            
            # Add comments section
            comments_label = soup.new_tag('strong')
            comments_label.string = "New comments:"
            body.append(comments_label)
            body.append(soup.new_string('\n'))
            
            ticket_comments = [c for c in all_new_comments if c['jira_ticket'] == jira_ticket]
            if ticket_comments:
                for comment in ticket_comments[:5]:  # Limit to 5 comments
                    date_str = comment['created'].strftime('%m/%d %H:%M')
                    
                    # Author and date
                    author_em = soup.new_tag('em')
                    author_em.string = f"{comment['author']} ({date_str})"
                    body.append(author_em)
                    body.append(soup.new_string('\n'))
                    
                    # Comment text (already HTML from Jira)
                    comment_soup = BeautifulSoup(comment['text'], 'html.parser')
                    body.append(comment_soup)
                    body.append(soup.new_string('\n'))
                
                # Add separator
                body.append(soup.new_string('----------\n'))
            else:
                body.append(soup.new_string(' No new comments.\n'))
            
            body.append(soup.new_string('\n'))
        
        body.append(soup.new_string('\n'))
        
        return str(soup)

    def sync_goal_by_id(self, goal_gid: str) -> int:
        """Synchronize specific goal by ID"""
        print(f"🎯 Synchronizing goal ID: {goal_gid}")

        # Get goal details
        goal = self.asana.get_goal_by_id(goal_gid)
        if not goal:
            print(f"❌ Goal with ID '{goal_gid}' not found")
            return 0

        goal_name = goal['name']
        print(f"📍 Found goal: {goal_name} ({goal_gid})")

        # Process this goal using existing logic
        return self._process_single_goal(goal_gid, goal_name)

    def sync_goals_in_project(self, project_gid: str) -> int:
        """Synchronize all goals in a project"""
        print(f"🎯 Synchronizing goals in project ID: {project_gid}")

        goals = self.asana.get_goals_in_project(project_gid)
        if not goals:
            print(f"⚠️ No goals found in project '{project_gid}'")
            return 0

        print(f"📍 Found {len(goals)} goals in project")

        total_processed = 0
        for goal in goals:
            goal_gid = goal['gid']
            goal_name = goal['name']
            processed = self._process_single_goal(goal_gid, goal_name)
            total_processed += processed

        return total_processed

    def sync_goals_in_team(self, team_gid: str) -> int:
        """Synchronize all goals in a team"""
        print(f"🎯 Synchronizing goals in team ID: {team_gid}")

        goals = self.asana.get_goals_in_team(team_gid)
        if not goals:
            print(f"⚠️ No goals found in team '{team_gid}'")
            return 0

        print(f"📍 Found {len(goals)} goals in team")

        total_processed = 0
        for goal in goals:
            goal_gid = goal['gid']
            goal_name = goal['name']
            processed = self._process_single_goal(goal_gid, goal_name)
            total_processed += processed

        return total_processed

    def sync_goals_in_workspace(self, workspace_gid: str) -> int:
        """Synchronize all goals in a workspace"""
        print(f"🎯 Synchronizing goals in workspace ID: {workspace_gid}")

        goals = self.asana.get_goals_in_workspace(workspace_gid)
        if not goals:
            print(f"⚠️ No goals found in workspace '{workspace_gid}'")
            return 0

        print(f"📍 Found {len(goals)} goals in workspace")

        total_processed = 0
        for goal in goals:
            goal_gid = goal['gid']
            goal_name = goal['name']
            processed = self._process_single_goal(goal_gid, goal_name)
            total_processed += processed

        return total_processed

    def _process_single_goal(self, goal_gid: str, goal_name: str) -> int:
        """Process a single goal - extracted logic from sync_all_goals"""
        print(f"\n🎯 Processing goal: {goal_name} ({goal_gid})")

        if self.dry_run:
            print(f"   🔍 DRY RUN: Would sync goal '{goal_name}'")

        # Get goal relationships (tasks linked to this goal)
        print(f"      🔍 Fetching goal relationships for goal {goal_gid}")
        tasks = self.asana.get_goal_tasks(goal_gid)

        if not tasks:
            print(f"   ⚠️  No tasks found linked to goal '{goal_name}'")
            return 0

        print(f"   📋 Checking {len(tasks)} tasks from goal relationships")

        # Collect all Jira ticket information for this goal
        jira_tickets_info = []

        for task in tasks:
            task_gid = task.get('gid')
            task_name = task.get('name', 'Unknown Task')
            print(f"   🔗 Task: {task_name}")

            if not task_gid:
                continue

            # Get task details with Jira ticket
            task_details = self.asana.get_task_details(task_gid)
            if task_details and task_details.get('jira_ticket'):
                jira_ticket = task_details['jira_ticket']
                print(f"      → Jira ticket: {jira_ticket}")

                # Get current Jira status
                jira_health_indicator, jira_goal_completion = self.jira.get_ticket_details(jira_ticket)
                if jira_health_indicator:
                    print(f"         Current status: {jira_health_indicator}")
                    jira_tickets_info.append({
                        'task_name': task_name,
                        'jira_ticket': jira_ticket,
                        'jira_health_indicator': jira_health_indicator,
                        'jira_goal_completion': jira_goal_completion,
                        'source': f'goal: {goal_name}'
                    })
                else:
                    raise RuntimeError(f"Could not get Jira status for ticket {jira_ticket}")

        # Create goal status update with all Jira information
        if jira_tickets_info:
            # This will raise an exception if it fails
            self.create_goal_status_update(goal_gid, goal_name, jira_tickets_info)
            print(f"   ✅ Goal '{goal_name}': Processed {len(jira_tickets_info)} Jira tickets")
            return 1  # Return 1 goal processed
        else:
            print(f"   ⚠️  No Jira tickets found for goal '{goal_name}'")
            return 0
