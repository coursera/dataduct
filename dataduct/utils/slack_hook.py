"""Action hook for posting a message on slack
"""

from ..config.config import Config

import logging
logger = logging.getLogger(__name__)


def post_message(message):
    """Post a message on a specified slack channel

    Args:
        message(str): The message to post
    """
    try:
        import slack
        import slack.chat
        config = Config()
        slack_config = config.etl['slack']
        slack.api_token = slack_config['api_token']
        slack.chat.post_message(slack_config['channel_name'],
                                message,
                                username=slack_config['bot_username'])
    except Exception:
        logger.info('If you want to post a slack message when you activate a pipeline')  # noqa
        logger.info('1) Run: pip install pyslack')
        logger.info('2) Visit https://api.slack/com/web to generate a token')
        logger.info('   and add:')
        logger.info('       api_token:<your token>')
        logger.info('       channel_name:<channel_name>')
        logger.info('       bot_username:<bot_username>')
        logger.info('   to the etl section of your config file')
