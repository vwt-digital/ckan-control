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

    current_sprint = None
    sprints = client.sprints(board_id)

    for sprint in reversed(sprints):
        if sprint.state == 'ACTIVE':
            current_sprint = sprint

    if not current_sprint:
        current_sprint = list(sprints)[-1]

    return current_sprint.id


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def add_to_sprint(client, sprint_id, issue_key):
    """
    Adds issues to a sprint.
    """

    client.add_issues_to_sprint(sprint_id, [issue_key])


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def add_to_epic(client, epic_id, issue_key):
    """
    Adds issues to an epic.
    """
    client.add_issues_to_epic(epic_id, [issue_key])


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def list_issue_comment_ids(client, issue_id):
    """
    Get jira issue from its ID
    """

    issue = client.issue(issue_id)

    return issue.fields.comment.comments


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def get_issue_id(client, issue):
    """
    Get issue ids based on a jira query.
    """

    return issue.id


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def add_comment(client, issue, comment):
    """
    Add a comment to an issue
    """

    client.add_comment(issue, comment)


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def update_comment(client, issue, comment_id, comment):
    """
    Update a comment of an issue
    """

    comment_to_edit = client.comment(issue, comment_id)
    comment_to_edit.update(body=comment)


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def get_comment_body(client, issue, comment_id):
    """
    Get body of a comment of an issue
    """

    comment = client.comment(issue, comment_id)
    comment_body = comment.body
    return comment_body


@retry(ConnectionError, tries=3, delay=2, backoff=2)
def list_issues(client, jql):
    """
    Get issues based on a jira query.
    """

    return client.search_issues(jql, maxResults=None)
