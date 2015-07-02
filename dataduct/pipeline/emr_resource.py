"""
Pipeline object class for emr resource
"""

from ..config import Config
from ..s3 import S3LogPath
from ..utils import constants as const
from ..utils.exceptions import ETLInputError
from .pipeline_object import PipelineObject
from .schedule import Schedule

config = Config()
NUM_CORE_INSTANCES = config.emr.get('NUM_CORE_INSTANCES', const.NONE)
CORE_INSTANCE_TYPE = config.emr.get('CORE_INSTANCE_TYPE', const.M1_LARGE)
TASK_INSTANCE_BID_PRICE = config.emr.get('TASK_INSTANCE_BID_PRICE', const.NONE)
TASK_INSTANCE_TYPE = config.emr.get('TASK_INSTANCE_TYPE', const.M1_LARGE)
MASTER_INSTANCE_TYPE = config.emr.get('MASTER_INSTANCE_TYPE', const.M1_LARGE)
CLUSTER_TIMEOUT = config.emr.get('CLUSTER_TIMEOUT', const.DEFAULT_TIMEOUT)
HADOOP_VERSION = config.emr.get('HADOOP_VERSION', const.NONE)
HIVE_VERSION = config.emr.get('HIVE_VERSION', const.NONE)
PIG_VERSION = config.emr.get('PIG_VERSION', const.NONE)
CLUSTER_AMI = config.emr.get('CLUSTER_AMI', '2.4.7')
DEFAULT_BOOTSTRAP = config.emr.get('DEFAULT_BOOTSTRAP', [])
KEY_PAIR = config.etl.get('KEY_PAIR', const.NONE)

import logging
logger = logging.getLogger(__name__)


class EmrResource(PipelineObject):
    """EMR Resource class
    """

    def __init__(self,
                 id,
                 s3_log_dir,
                 schedule,
                 num_instances=NUM_CORE_INSTANCES,
                 instance_size=CORE_INSTANCE_TYPE,
                 bootstrap=None,
                 num_task_instances=None,
                 task_bid_price=TASK_INSTANCE_BID_PRICE,
                 task_instance_type=TASK_INSTANCE_TYPE,
                 master_instance_size=MASTER_INSTANCE_TYPE,
                 terminate_after=CLUSTER_TIMEOUT,
                 hadoop_version=HADOOP_VERSION,
                 install_hive=HIVE_VERSION,
                 install_pig=PIG_VERSION,
                 ami_version=CLUSTER_AMI):
        """Constructor for the Ec2Resource class

        Args:
            id(str): id of the object
            s3_log_dir(S3Directory): s3 directory for pipeline logs
            schedule(Schedule): pipeline schedule used for the machine
            num_instances(int): number of core instances used in the cluster
            instance_size(str): type of core instances
            bootstrap(list): list for bootstrap action of the cluster
            num_task_instances(int): number of task instances
            task_bid_price(str): bid price for spot task instances
            task_instance_type(str):  type of task instances
            master_instance_size(str):  type of master instance
            terminate_after(str): time to terminate the Emrresource after
            hadoop_version(str): hadoop version to be installed
            install_hive(str): version of hive to be installed
            install_pig(str): version of pig to be installed
            ami_version(str): ami version for the Emr resource
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')
        if not isinstance(s3_log_dir, S3LogPath):
            raise ETLInputError(
                's3 log directory must be of type S3LogPath')

        self.ami_version = ami_version
        if bootstrap:
            self.bootstrap = DEFAULT_BOOTSTRAP + bootstrap
        else:
            self.bootstrap = DEFAULT_BOOTSTRAP

        super(EmrResource, self).__init__(
            id=id,
            masterInstanceType=master_instance_size,
            coreInstanceCount=num_instances,
            coreInstanceType=instance_size,
            terminateAfter=terminate_after,
            bootstrapAction=self.bootstrap,
            type='EmrCluster',
            schedule=schedule,
            keyPair=KEY_PAIR,
            logUri=s3_log_dir,
            emrLogUri=s3_log_dir,
            enableDebugging="true",
            installHive=install_hive,
            pigVersion=install_pig,
            amiVersion=ami_version,
            hadoopVersion=hadoop_version,
        )

        if num_task_instances:
            self['taskInstanceCount'] = num_task_instances
            if task_bid_price is not None:
                self['taskInstanceBidPrice'] = task_bid_price
            self['taskInstanceType'] = task_instance_type

            if self['taskInstanceType'].find('xlarge') >= 0:
                if num_task_instances > 10:
                    logger.info('Using taskInstanceType: (%s)',
                                self['taskInstanceType'])
                    logger.warning(
                        'Are you sure you need %s task instances?',
                        num_task_instances)
