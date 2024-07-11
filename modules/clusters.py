####
## Imports
####

# Installed modules:
import os
from distributed import security
from distributed.deploy import cluster
from dask_jobqueue import SLURMCluster
from dask_kubernetes.operator import KubeCluster
from dotenv import load_dotenv

####
## Global constants
####

load_dotenv()

OUT_DIR:           str = str(os.getenv('OUT_DIR'))
CERT_DIR:          str = str(os.getenv('CERT_DIR'))
MEM_PER_NODE:      int = str(os.getenv('MEM_PER_NODE')) + 'GB'
NET_INTERFACE:     str = str(os.getenv('NET_INTERFACE'))
ACCOUNT:           str = str(os.getenv('ACCOUNT'))
PARTITION:         str = str(os.getenv('PARTITION'))
TIME_LIMIT:        str = str(os.getenv('TIME_LIMIT'))
ENV_TO_SOURCE:     str = str(os.getenv('ENV_TO_SOURCE')) + '/bin/activate'

KUBE_NAMESPACE:    str = str(os.getenv('KUBE_NAMESPACE'))
KUBE_CLUSTER_SPEC:  str = str(os.getenv('PWD')) + "/" + str(os.getenv('KUBE_CLUSTER_SPEC'))

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
        ssl:             bool = True,
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
        cores                  = ncores,          # Total number of cores per job
        job_cpu                = ncores,          # Number of cpu to book in SLURM
        memory                 = mem_per_node,    # Total amount of memory per job
        job_mem                = mem_per_node,    # Amount of memory to request
        interface              = interface,       # use 'ip link show' to check
        processes              = 1,               # Cut the job up into this many processes. default ~= sqrt(cores)
        account                = account,
        queue                  = queue,
        walltime               = timelimit,
        n_workers              = 0,
        asynchronous           = False,
        death_timeout          = 60*5,
        security               = ssl,
        shared_temp_directory  = CERT_DIR,
        job_script_prologue = [
            '#SBATCH --output=' + out_dir + '/' + 'slurm-%j.out',
            '#SBATCH --job-name="d_slave"',
            '#SBATCH --get-user-env',
            '#SBATCH --cpus-per-task=' + str(ncores),
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


def get_kube_cluster() -> KubeCluster:
    """
    Create a dask_kubernetes.operator.KubeCluster object able to interact with the Kubernetes

    :return: A dask_kubernetes.operator.KubeCluster object
    """
    cluster: KubeCluster = KubeCluster(
        namespace=KUBE_NAMESPACE,
        custom_cluster_spec=KUBE_CLUSTER_SPEC,
        n_workers=0,
        quiet=True
    )
    return cluster
