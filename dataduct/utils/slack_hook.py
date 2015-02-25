"""Action hook for posting a message on slack
"""

from ..config import Config

import logging
logger = logging.getLogger(__name__)


def post_message(message):
    """Post a message on a specified slack channel

    Args:
        message(str): The message to post
    """
    # If any of these fail, silently skip because the user doesn't know about
    # the slack integration or doesn't care
    try:
        import slack
        import slack.chat
        config = Config()
        slack_config = config.etl['slack']
    except KeyError:
        return

    # If any of these configs fail, output error message and fail because the
    # user has misconfigured the slack integration
    try:
        slack.api_token = slack_config['api_token']
        slack.chat.post_message(slack_config['channel_name'],
                                message,
                                username=slack_config.get('bot_username',
                                                          'Dataduct'))
    except KeyError:
        message = ['If you want to post a slack message when you activate a pipeline',  # noqa
                   '1) Run: pip install pyslack',
                   '2) Visit https://api.slack/com/web to generate a token',
                   '3) Add:',
                   '       api_token:<your token>',
                   '       channel_name:<channel_name>',
                   '       bot_username:<bot_username>',
                   '   to the etl section of your config file']
        for line in message:
            logger.error(line)
        raise
