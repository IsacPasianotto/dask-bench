####
## Imports
####

# Installed modules:
import os
from dask_jobqueue import SLURMCluster
from dask_kubernetes.operator import KubeCluster
from dotenv import load_dotenv

# defined modules:
from modules.kube_helpers import custom_make_cluster_spec as cmcs

####
## Global constants
####

load_dotenv()

OUT_DIR:           str = str(os.getenv('OUT_DIR'))
USE_SSL:           bool = True if str(os.getenv('USE_SSL')).lower() == 'true' else None
CERT_DIR:          str = str(os.getenv('CERT_DIR')) if USE_SSL else None
MEM_PER_NODE:      str = str(os.getenv('MEM_PER_NODE'))
NET_INTERFACE:     str = str(os.getenv('NET_INTERFACE'))
ACCOUNT:           str = str(os.getenv('ACCOUNT'))
PARTITION:         str = str(os.getenv('PARTITION'))
TIME_LIMIT:        str = str(os.getenv('TIME_LIMIT'))
ENV_TO_SOURCE:     str = str(os.getenv('ENV_TO_SOURCE')) + '/bin/activate'

KUBE_NAMESPACE:    str = str(os.getenv('KUBE_NAMESPACE'))
KUBECLUSTER_NAME:  str = str(os.getenv('KUBECLUSTER_NAME'))
CONTAINER_IMAGE:   str = str(os.getenv('CONTAINER_IMAGE'))

####
## Function definition
####

def get_slurm_cluster(
        ncores:          int,
        mem_per_node:    str = MEM_PER_NODE,
        interface:       str = NET_INTERFACE,
        env_to_source:   str = ENV_TO_SOURCE,
        out_dir:         str = OUT_DIR,
        queue:           str = PARTITION,
        timelimit:       str = TIME_LIMIT,
        account:         str = ACCOUNT,
        ssl:             bool = USE_SSL,
        cert_dir:        str = CERT_DIR,
    ) -> SLURMCluster:
    """
    Crate a dask_jobqueue.SLURMCluster object able to interact with the SLURM scheduler
    installed in the used HPC system, which will ask for one or more workers with the
    specified resources. Default values are taken from the environment variables.

    :param ncores:          Number of cores each worker will have
    :param mem_per_node:    Amount of memory each worker will have
    :param interface:       Network interface to use
    :param env_to_source:   Path to the environment to source (each worker is a new instance of python interpreter, so the environment must be sourced)
    :param out_dir:         Directory where to save the SLURM-output of the workers
    :param queue:           SLURM partition to use
    :param timelimit:       Maximum time the workers will be alive
    :param account:         Account to use in the SLURM scheduler
    :return:                A dask_jobqueue.SLURMCluster object
    """

    cluster: SLURMCluster = SLURMCluster(
        cores                  = ncores,                 # Total number of cores per job
        job_cpu                = ncores,                 # Number of cpu to book in SLURM
        memory                 = mem_per_node + 'GB',    # Total amount of memory per job
        job_mem                = mem_per_node + 'GB',    # Amount of memory to request
        interface              = interface,              # use 'ip link show' to check
        processes              = ncores,                 # Cut the job up into this many processes. default ~= sqrt(cores)
        account                = account,
        queue                  = queue,
        walltime               = timelimit,
        n_workers              = 0,
        asynchronous           = False,
        death_timeout          = 60*5,
        security               = ssl,
        shared_temp_directory  = cert_dir,
        job_script_prologue = [
            '#SBATCH --output=' + out_dir + '/' + 'slurm-%j.out',
            '#SBATCH --job-name="d_slave"',
            '#SBATCH --get-user-env',
            '#SBATCH --exclusive',                       # No performance degradation due to other jobs
            'echo "-----------------------------------------------"',
            'echo "HOSTNAME:             $(hostname)"',
            'echo "DATE:                 $(date)"',
            'echo "SLURM_JOBID:          $SLURM_JOBID"',
            'echo "SLURM_JOB_NODELIST:   $SLURM_JOB_NODELIST"',
            'echo "SLURM_CPUS_PER_TASK:  $SLURM_CPUS_PER_TASK"',
            'echo "-----------------------------------------------"',
            'source ' + env_to_source,
            'export NUMACTL_CMD="numactl --physcpubind=+0-$((SLURM_CPUS_PER_TASK-1)) --localalloc"',
            'echo "NUMACTL_CMD: $NUMACTL_CMD"'
            'echo "Using python version: "',
            'python --version',
            'echo "......."',
            ],
        # worker_extra_args = ['--nthreads=1'],  # Ensure each worker has only one thread
    )
    return cluster

def get_kube_cluster(
        ncores:         int,
        mem_per_node:   str  = MEM_PER_NODE,
        ns:             str  = KUBE_NAMESPACE,
        clustername:    str  = KUBECLUSTER_NAME,
        containerimage: str  = CONTAINER_IMAGE,
        service_type:   str  = 'ClusterIP',
        worker_comm:    str  = 'dask-worker',
        quiet:          bool = True,
        ssl:            bool = USE_SSL,
        cert_dir:       str  = CERT_DIR
    ) -> KubeCluster:
    """
    Create a dask_kubernetes.operator.KubeCluster object able to interact with  a
    Kubernetes cluster installation. The cluster will spawn a pod which acts as a
    scheduler and one or more pods which act as workers. Default values are taken
    from the environment variables.
    The scaling is done with the dask_kubernetes.operator.KubeCluster.scale method
    in the benchmark loop.

    :param ncores:       Number of cores each worker will have
    :param mem_per_node: Amount of memory each worker will have
    :param ns:           Namespace where to deploy the cluster
    :param clustername:  Name of the created DaskCluster kubernetes resources
    :param containerimage: Name of the container image to use
    :param service_type: Type of service to use
    :param worker_comm:  Command to use to start the worker
    :param ssl:          If True, the cluster will use SSL to communicate
    :param cert_dir:     Directory where to look for the certificates (they will be mounted as a volume in the workers pods)
    :param quiet:        If True, the cluster will not print dynamically changing logs of the kubernetes status. It can be enabled for debugging purposes.
    :return: A dask_kubernetes.operator.KubeCluster object
    """

    # construct the dict configuration to pass to the custom defined function
    config: dict = {
        'name': clustername,
        'n_workers': 0,
        'resources': {
            'limits': {
                'cpu': str(ncores),
                'memory': mem_per_node + 'Gi'
                },
            'requests': {
                'cpu': str(ncores),
                'memory': mem_per_node + 'Gi'
                },
            },
        'image': containerimage,
        'scheduler_service_type': service_type,
        'worker_command': worker_comm,
        'use_ssl': ssl,
        'cert_dir': cert_dir
    }

    cluster = KubeCluster(namespace = ns, custom_cluster_spec=cmcs(**config), quiet=quiet)
    return cluster
