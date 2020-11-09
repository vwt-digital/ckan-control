from jira import JIRA
from retry import retry
from requests.exceptions import ConnectionError


def jira_init(user, api_key, server):
    """
    Initializes a Jira client.
    """

    options = {
        'server': server
    }

    client = JIRA(options, basic_auth=(user, api_key))

    return client


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def list_issue_titles(client, jql):
    """
    Lists jira issues based on a jira query.
    """

    issues = client.search_issues(jql, maxResults=None)

    titles = [issue.fields.summary for issue in issues]

    return titles


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def create_issue(client, project, title, description, type='Bug'):
    """
    Creates a jira issue.
    """

    issue = client.create_issue(
        project=project,
        summary=title,
        issuetype={'name': type},
        description=description)

    return issue


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def get_current_sprint(client, board_id):
    """
    Returns the current sprint for a scrum board.
    """

    sprints = client.sprints(board_id)

    current_sprint = list(sprints)[-1]

    return current_sprint.id


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def add_to_sprint(client, sprint_id, issue_key):
    """
    Adds issues to a sprint.
    """

    client.add_issues_to_sprint(sprint_id, [issue_key])
