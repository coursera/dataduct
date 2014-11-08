"""
Pipeline object class for ec2 resource
"""
from ..constants import DEFAULT_NUM_CORE_INSTANCES
from ..constants import DEFAULT_CORE_INSTANCE_TYPE
from ..constants import DEFAULT_TASK_INSTANCE_BID_PRICE
from ..constants import DEFAULT_TASK_INSTANCE_TYPE
from ..constants import DEFAULT_MASTER_INSTANCE_TYPE
from ..constants import DEFAULT_CLUSTER_TIMEOUT
from ..constants import DEFAULT_HADOOP_VERSION
from ..constants import DEFAULT_HIVE_VERSION
from ..constants import DEFAULT_PIG_VERSION
from ..constants import DEFAULT_CLUSTER_AMI
from ..constants import KEY_PAIR

from .pipeline_object import PipelineObject
from ..s3.s3_directory import S3Directory
from .schedule import Schedule
from ..utils.exceptions import ETLInputError


class EmrResource(PipelineObject):
    """EMR Resource class
    """

    def __init__(self,
                 id,
                 s3_log_dir,
                 schedule,
                 num_instances=DEFAULT_NUM_CORE_INSTANCES,
                 instance_size=DEFAULT_CORE_INSTANCE_TYPE,
                 bootstrap=None,
                 num_task_instances=None,
                 task_bid_price=DEFAULT_TASK_INSTANCE_BID_PRICE,
                 task_instance_type=DEFAULT_TASK_INSTANCE_TYPE,
                 master_instance_size=DEFAULT_MASTER_INSTANCE_TYPE,
                 terminate_after=DEFAULT_CLUSTER_TIMEOUT,
                 hadoop_version=DEFAULT_HADOOP_VERSION,
                 install_hive=DEFAULT_HIVE_VERSION,
                 install_pig=DEFAULT_PIG_VERSION,
                 ami_version=DEFAULT_CLUSTER_AMI):
        """Constructor for the Ec2Resource class

        Args:
            id(str): id of the object
            s3_log_dir(S3Directory): s3 directory for pipeline logs
            schedule(Schedule): pipeline schedule used for the machine
            num_instances(int): number of core instances used in the cluster
            instance_size(str): type of core instances
            bootstrap(S3File): S3File for bootstrap action of the cluster
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
        if not isinstance(s3_log_dir, S3Directory):
            raise ETLInputError(
                's3 log directory must be of type S3Directory')

        super(EmrResource, self).__init__(
            id=id,
            masterInstanceType=master_instance_size,
            coreInstanceCount=num_instances,
            coreInstanceType=instance_size,
            terminateAfter=terminate_after,
            bootstrapAction=bootstrap,
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
                    print 'Using taskInstanceType: (%s)' % \
                        self['taskInstanceType']
                    print 'WARNING!!! Are you sure you need', \
                        '%s task instances?' % num_task_instances
