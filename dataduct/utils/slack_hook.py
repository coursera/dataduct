"""Action hook for posting a message on slack
"""

from ..config import Config

import logging
logger = logging.getLogger(__name__)


def post_message(message):
    """Post a message on a specified slack channel.
    Will silently skip if there is no etl.slack configuration.
    Will print a help message if etl.slack is misconfigured.

    Args:
        message(str): The message to post with templating
            {user}: The username as specified in the config file
    """

    # If there is no slack configuration, silently skip because the user
    # doesn't know about slack integration or doesn't care
    config = Config()
    slack_config = config.etl.get('slack', None)
    if slack_config is None:
        return

    try:
        import slack
        import slack.chat
        slack.api_token = slack_config['api_token']
        user = slack_config.get('username', 'Unknown User')
        slack.chat.post_message(slack_config['channel_name'],
                                message.format(user=user),
                                username=slack_config.get('bot_username',
                                                          'Dataduct'))
    except Exception:
        message = ['If you want to post a slack message when you activate a pipeline',  # noqa
                   '1) Run: pip install pyslack',
                   '2) Visit https://api.slack/com/web to generate a token',
                   '3) Add ([] denotes optional field):',
                   '       api_token:<your token>',
                   '       channel_name:<channel_name>',
                   '       [username:<your_username>]',
                   '       [bot_username:<bot_username>]',
                   '   to the etl section of your config file']
        for line in message:
            logger.info(line)
