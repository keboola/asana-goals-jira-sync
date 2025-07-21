"""
Main synchronization manager for Jira-Asana integration
Coordinates the synchronization logic between both platforms.
"""
import re
from datetime import datetime, timedelta
from dateutil import parser

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

    def map_jira_status_to_asana(self, jira_status: str) -> str:
        """Map Jira status to Asana status type"""
        mapped_status = self.status_mapping.get(jira_status, jira_status)

        # Convert to Asana status type
        if mapped_status.lower() in ['done', 'complete', 'resolved', 'closed']:
            return 'complete'
        elif mapped_status.lower() in ['blocked', 'on hold', 'waiting']:
            return 'at_risk'
        elif mapped_status.lower() in ['in progress', 'development', 'testing']:
            return 'on_track'
        else:
            return 'on_track'  # Default

    def _extract_text_from_jira_comment(self, jira_comment_body: any) -> dict:
        """Extract readable text from Jira comment body (ADF format)"""
        if isinstance(jira_comment_body, dict):
            # Handle Atlassian Document Format (ADF)
            return self._convert_adf_to_asana_structured(jira_comment_body)
        else:
            return {"type": "text", "text": str(jira_comment_body)}

    @staticmethod
    def _convert_adf_to_asana_structured(adf: dict) -> dict:
        """Convert Atlassian Document Format (ADF) to Asana rich text format (text.content)."""

        def map_marks(adf_marks):
            if not adf_marks:
                return []

            marks = []
            for mark in adf_marks:
                t = mark.get("type")
                if t == "strong":
                    marks.append("bold")
                elif t == "em":
                    marks.append("italic")
                elif t == "underline":
                    marks.append("underline")
                elif t == "strike":
                    marks.append("strikethrough")
                elif t == "code":
                    marks.append("code")
                elif t == "link":
                    href = mark.get("attrs", {}).get("href")
                    if href:
                        marks.append({"type": "link", "href": href})
            return marks

        def extract_text_nodes(content_array):
            result = []
            for node in content_array:
                if node.get("type") == "text":
                    item = {
                        "type": "text",
                        "text": node.get("text", "")
                    }
                    marks = map_marks(node.get("marks", []))
                    if marks:
                        item["marks"] = marks
                    result.append(item)
            return result

        def handle_list(items, ordered=False):
            style = "ordered" if ordered else "unordered"
            return {
                "type": "list",
                "style": style,
                "items": [
                    {
                        "type": "list_item",
                        "text": extract_text_nodes(child.get("content", [])[0].get("content", []))
                    }
                    for child in items
                ]
            }

        structured = []

        for node in adf.get("content", []):
            node_type = node.get("type")

            if node_type == "paragraph":
                structured.append({
                    "type": "paragraph",
                    "text": extract_text_nodes(node.get("content", []))
                })

            elif node_type == "heading":
                level = node.get("attrs", {}).get("level", 1)
                heading_text = extract_text_nodes(node.get("content", []))
                structured.append({
                    "type": "heading",
                    "level": level,
                    "text": heading_text
                })

            elif node_type == "bulletList":
                structured.append(handle_list(node.get("content", []), ordered=False))

            elif node_type == "orderedList":
                structured.append(handle_list(node.get("content", []), ordered=True))

            elif node_type == "blockquote":
                structured.append({
                    "type": "quote",
                    "text": extract_text_nodes(node.get("content", [])[0].get("content", []))
                })

            elif node_type == "codeBlock":
                code_content = "".join(
                    node.get("content", [{}])[0].get("text", "")
                )
                structured.append({
                    "type": "code_block",
                    "text": [{"type": "text", "text": code_content}]
                })

            elif node_type == "panel":
                panel_content = extract_text_nodes(node.get("content", [])[0].get("content", []))
                structured.append({
                    "type": "paragraph",
                    "text": panel_content
                })

            # fallback for unknown types or raw passthrough
            elif "content" in node:
                structured.append({
                    "type": "paragraph",
                    "text": extract_text_nodes(node.get("content", []))
                })

        return {"content": structured}

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

        # Collect new comments and status changes from all Jira tickets
        all_new_comments = []
        current_ticket_statuses = {}

        for ticket_info in jira_tickets_info:
            jira_ticket = ticket_info['jira_ticket']
            current_ticket_statuses[jira_ticket] = ticket_info['jira_status']

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

        # Check if any ticket statuses changed by comparing with previous status update
        status_changes_detected = False
        if latest_status and latest_status.get('text'):
            previous_text = latest_status['text']

            for ticket, current_status in current_ticket_statuses.items():
                # Check if this specific ticket with current status exists in previous text
                ticket_search_pattern = f"[{ticket}]"
                status_search_pattern = f"({current_status} -"

                ticket_present = ticket_search_pattern in previous_text
                status_present = status_search_pattern in previous_text

                if not (ticket_present and status_present):
                    status_changes_detected = True
                    break

        # Sort comments by date (newest first)
        all_new_comments.sort(key=lambda x: x['created'], reverse=True)

        # Determine if we need to create a new status update
        has_new_activity = len(all_new_comments) > 0  # Any new comments trigger update
        is_first_status = latest_status is None

        if not has_new_activity and not status_changes_detected and not is_first_status:
            print(f"   ℹ️  No changes since last update")
            return True  # Return True because no update was needed (success case)

        print(f"   ✅ Creating status update")

        # Determine goal status type based on overall progress
        total_tickets = len(jira_tickets_info)
        done_tickets = sum(1 for ticket in jira_tickets_info
                           if ticket['jira_status'].lower() in ['done', 'complete', 'resolved', 'closed'])
        blocked_tickets = sum(1 for ticket in jira_tickets_info
                              if ticket['jira_status'].lower() in ['blocked', 'on hold', 'waiting'])

        if done_tickets == total_tickets:
            status_type = "complete"
        elif blocked_tickets > 0:
            status_type = "at_risk"
        else:
            status_type = "on_track"

        # Create simple title
        title = "Status Sync"

        status_text = self._build_status_text_html(all_new_comments, jira_tickets_info)

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
            print(f"   ✅ Goal status update created")
            return True

    def _build_status_text_html(self, all_new_comments, jira_tickets_info) -> str:
        html = "<strong>🚀 Automatic Activity Update</strong>\n"

        for ticket_info in jira_tickets_info:
            jira_ticket = ticket_info['jira_ticket']
            jira_status = ticket_info['jira_status']
            task_name = ticket_info['task_name']
            jira_url = f"https://keboola.atlassian.net/browse/{jira_ticket}"

            html += f'<a href="{jira_url}">{task_name}</a>\n'
            html += f"<strong>Status:</strong> {jira_status}\n"

            ticket_comments = [c for c in all_new_comments if c['jira_ticket'] == jira_ticket]
            html += "<strong>New comments:</strong>\n"
            if ticket_comments:
                for comment in ticket_comments[:5]:
                    date_str = comment['created'].strftime('%m/%d %H:%M')
                    html += (
                        f'<em>{comment["author"]} ({date_str})</em>\n'
                        f'{comment["text"]}\n'
                    )
                html += "----------\n"
            else:
                html += ' No new comments.'
            html += "\n"

        html += "\n"
        return f'<body>{html}</body>'

    def sync_goal(self, goal_name: str) -> int:
        """Synchronize specific goal by name"""
        print(f"🎯 Synchronizing goal: {goal_name}")

        team_gid = self.asana.team_gid
        if not team_gid:
            print("❌ No team_gid configured in Asana settings")
            return 0

        # Find the goal
        goal = self.asana.get_goal_by_name(team_gid, goal_name)
        if not goal:
            print(f"❌ Goal '{goal_name}' not found in team")
            return 0

        print(f"📍 Found goal: {goal['name']} ({goal['gid']})")
        return self._sync_single_goal(goal)

    def sync_all_goals(self) -> int:
        """Synchronize all goals in team"""
        print("🎯 Synchronizing all goals in team...")

        team_gid = self.asana.team_gid
        if not team_gid:
            print("❌ No team_gid configured in Asana settings")
            return 0

        # Get all goals in team
        goals = self.asana.get_team_goals(team_gid)
        print(f"📍 Found {len(goals)} goals in team")

        if not goals:
            print("❌ No goals found in team")
            return 0

        total_success = 0
        for goal in goals:
            print(f"\n🎯 Processing goal: {goal['name']}")
            total_success += self._sync_single_goal(goal)

        print(f"\n✅ All goals synchronized: {total_success} tickets synced successfully")
        return total_success

    def _sync_single_goal(self, goal: dict[str, any]) -> int:
        """Internal method to sync a single goal"""
        goal_gid = goal['gid']
        goal_name = goal['name']

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
                jira_status = self.jira.get_ticket_status(jira_ticket)
                if jira_status:
                    print(f"         Current status: {jira_status}")
                    jira_tickets_info.append({
                        'task_name': task_name,
                        'jira_ticket': jira_ticket,
                        'jira_status': jira_status,
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
                jira_status = self.jira.get_ticket_status(jira_ticket)
                if jira_status:
                    print(f"         Current status: {jira_status}")
                    jira_tickets_info.append({
                        'task_name': task_name,
                        'jira_ticket': jira_ticket,
                        'jira_status': jira_status,
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
