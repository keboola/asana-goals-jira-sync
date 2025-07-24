"""
Jira API integration module
Handles all communication with Jira REST API.
"""
import logging

import requests
import base64
from datetime import datetime, timezone
from dateutil import parser


class JiraAPI:
    """Jira API client for ticket synchronization"""

    def __init__(self, config: dict[str, any]):
        # Ensure base_url has protocol
        base_url = config['base_url']
        if not base_url.startswith(('http://', 'https://')):
            base_url = f"https://{base_url}"

        self.base_url = base_url
        self.email = config['email']
        self.token = config['token']
        self.custom_fields = config.get('custom_fields', {})

        # Setup authentication
        auth_string = f"{self.email}:{self.token}"
        auth_bytes = auth_string.encode('ascii')
        self.auth_header = base64.b64encode(auth_bytes).decode('ascii')

        self.headers = {
            'Authorization': f'Basic {self.auth_header}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def get_ticket(self, ticket_key: str) -> dict[str, any] | None:
        """Get ticket details by key"""
        url = f"{self.base_url}/rest/api/3/issue/{ticket_key}"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Jira ticket {ticket_key}: {e}")
            return None

    def get_ticket_details(self, ticket_key: str) -> tuple[str, str] | None:
        """Get current status of a ticket"""
        ticket = self.get_ticket(ticket_key)
        if ticket:
            if not 'customfield_11699' in ticket['fields']:
                print(f"Ticket {ticket_key} does not have custom field 'customfield_11699' (goal_completion_value)")
            goal_completion_value = ticket['fields'].get('customfield_11699', None)
            if not 'customfield_10406' in ticket['fields']:
                print(f"Ticket {ticket_key} does not have custom field 'customfield_10406' (Health Indicator)")
                health_indicator = None
            else:
                health_indicator = ticket['fields'].get('customfield_10406', None).get('value', None)
            return health_indicator, goal_completion_value
        return None

    def get_comments_since(self, ticket_key: str, since_date: datetime) -> list[dict[str, any]]:
        """Get comments from a ticket since a specific date"""
        url = f"{self.base_url}/rest/api/3/issue/{ticket_key}/comment"

        try:
            response = requests.get(url, headers=self.headers, params="expand=renderedBody")
            response.raise_for_status()
            data = response.json()

            comments = []
            for comment in data.get('comments', []):
                # Parse comment creation date
                created_str = comment['created']
                created_date_with_tz = parser.parse(created_str)

                # Convert to UTC for comparison (remove timezone info after conversion)
                if created_date_with_tz.tzinfo is not None:
                    # Convert timezone-aware datetime to UTC, then make naive
                    created_date = created_date_with_tz.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    created_date = created_date_with_tz

                # Only include comments created after since_date (both in UTC now)
                if created_date > since_date:
                    comments.append({
                        'id': comment['id'],
                        'author': comment['author']['displayName'],
                        'body': comment['body'],
                        'rendered_body': comment['renderedBody'],
                        'created': created_date,
                        'created_str': created_str
                    })

            # Sort by creation date
            comments.sort(key=lambda x: x['created'])
            return comments

        except requests.exceptions.RequestException as e:
            print(f"Error fetching comments for Jira ticket {ticket_key}: {e}")
            return []
