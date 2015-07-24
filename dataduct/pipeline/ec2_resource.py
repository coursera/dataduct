"""
Pipeline object class for ec2 resource
"""

from ..config import Config
from .pipeline_object import PipelineObject
from ..s3 import S3LogPath
from .schedule import Schedule
from ..utils import constants as const
from ..utils.exceptions import ETLInputError

config = Config()
ROLE = config.etl['ROLE']
RESOURCE_ROLE = config.etl['RESOURCE_ROLE']

INSTANCE_TYPE = config.ec2.get('INSTANCE_TYPE', const.M1_LARGE)
ETL_AMI = config.ec2.get('ETL_AMI', const.NONE)
SECURITY_GROUP = config.ec2.get('SECURITY_GROUP', const.NONE)
SECURITY_GROUP_ID = config.ec2.get('SECURITY_GROUP_ID', const.NONE)
SUBNET_ID = config.ec2.get('SUBNET_ID', const.NONE)
KEY_PAIR = config.etl.get('KEY_PAIR', const.NONE)
RETRY_DELAY = config.etl.get('RETRY_DELAY', const.DEFAULT_DELAY)


class Ec2Resource(PipelineObject):
    """EC2 Resource class
    """

    def __init__(self,
                 id,
                 s3_log_dir=None,
                 schedule=None,
                 terminate_after='6 Hours',
                 instance_type=INSTANCE_TYPE,
                 ami=ETL_AMI,
                 security_group=SECURITY_GROUP,
                 security_group_id=SECURITY_GROUP_ID,
                 subnet_id=SUBNET_ID,
                 **kwargs):
        """Constructor for the Ec2Resource class

        Args:
            id(str): id of the object
            s3_log_dir(S3Directory): s3 directory for pipeline logs
            schedule(Schedule): pipeline schedule used for the machine
            terminate_after(str): time to terminate the ec2resource after
            instance_type(str): machine type to be used eg. m1.large
            ami(str): ami id for the ec2 resource
            retry_delay(str): time delay between step retries
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')
        if not isinstance(s3_log_dir, S3LogPath):
            raise ETLInputError(
                's3 log directory must be of type S3LogPath')

        super(Ec2Resource, self).__init__(
            id=id,
            type='Ec2Resource',
            terminateAfter=terminate_after,
            logUri=s3_log_dir,
            schedule=schedule,
            imageId=ami,
            instanceType=instance_type,
            role=ROLE,
            resourceRole=RESOURCE_ROLE,
            keyPair=KEY_PAIR,
            retryDelay=RETRY_DELAY,
            securityGroups=security_group,
            securityGroupIds=security_group_id,
            subnetId=subnet_id
        )
