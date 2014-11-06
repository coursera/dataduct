"""
Pipeline object class for ec2 resource
"""
from ..constants import DEFAULT_RESOURCE_ROLE
from ..constants import DEFAULT_EC2_INSTANCE_TYPE
from ..constants import ETL_AMI
from ..constants import KEY_PAIR
from ..constants import DEFAULT_ROLE
from ..constants import SECURITY_GROUP
from ..constants import RETRY_DELAY

from .pipeline_object import PipelineObject
from ..s3.s3_directory import S3Directory
from .schedule import Schedule
from ..utils.exceptions import ETLInputError


class Ec2Resource(PipelineObject):
    """EC2 Resource class
    """

    def __init__(self,
                 id,
                 s3_log_dir=None,
                 schedule=None,
                 terminate_after='6 Hours',
                 instance_type=DEFAULT_EC2_INSTANCE_TYPE,
                 ami=ETL_AMI,
                 security_group=SECURITY_GROUP,
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
        if not isinstance(s3_log_dir, S3Directory):
            raise ETLInputError(
                's3 log directory must be of type S3Directory')

        super(Ec2Resource, self).__init__(
            id=id,
            type='Ec2Resource',
            terminateAfter=terminate_after,
            logUri=s3_log_dir,
            schedule=schedule,
            imageId=ami,
            instanceType=instance_type,
            role=DEFAULT_ROLE,
            resourceRole=DEFAULT_RESOURCE_ROLE,
            keyPair=KEY_PAIR,
            retryDelay=RETRY_DELAY,
            securityGroups=security_group
        )
