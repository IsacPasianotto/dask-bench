####
## Imports
####

# installed packages
import os
from distributed import security
from dotenv import load_dotenv
from distributed.deploy import cluster
from dask_jobqueue import SLURMCluster
from dask_kubernetes.operator import KubeCluster

####
## global variables
####

load_dotenv()

OUTDIR:         str = str(os.getenv('OUTDIR'))
CERT_DIR:       str = str(os.getenv('CERT_DIR'))
MEM_PER_NODE:   str = str(os.getenv('MEM_PER_NODE')) + 'GB'
NET_INTERFACE:  str = str(os.getenv('NET_INTERFACE'))
ACCOUNT:        str = str(os.getenv('ACCOUNT'))
PARTITION:      str = str(os.getenv('PARTITION'))
TIME_LIMIT:     str = str(os.getenv('TIME_LIMIT'))
ENV_TO_SOURCE:  str = str(os.getenv('ENV_TO_SOURCE'))


def slurm_cluster_getter(
        ncores: int,
        interface: str = NET_INTERFACE,
        env_to_source: str = ENV_TO_SOURCE,
        account: str = ACCOUNT,
        partition: str = PARTITION,
        time_limit: str = TIME_LIMIT,
        mem_per_node: str = MEM_PER_NODE,
        outdir: str = OUTDIR,
        cert_dir: str = CERT_DIR

    ) -> dask_jobqueue.SLURMCluster:
    """
    Create a worker cluster for a SLURM scheduler

    :param ncores: int
        Number of cores to request in the cluster
    :param interface: str
        Network interface to use
    :param env_to_source: str
        Path to the environment to source
    :param account: str
        Account to use in the cluster
    :param partition: str
        Partition to use in the cluster
    :param time_limit: str
        Time limit for the job
    :param mem_per_node: str
        Memory per node to request
    :param outdir: str
        Output directory for the job
    :param cert_dir: str
        Directory in which to put the self-signed certificates generated on the fly by Dask
    :return: dask_jobqueue.SLURMCluster
        A SLURMCluster object with the requested configuration
    """

    cluster: dask_jobqueue.SLURMCluster = dask_jobqueue.SLURMCluster(
        cores = ncores,                             # Total number of cores per job
        job_cpu = ncores,                           # Number of cpu to book in SLURM, if None, defaults to worker threads * processes
        memory = mem_per_node,                      # Total amount of memory per job
        interface = interface,                      # Network interface to use, use "ip link show" to list available interfaces
        asynchronous = False,
        processes = 1,                              # Cut the job up into this many processes. Good for GIL workloads or for nodes with many cores. By default, process ~= sqrt(cores)
        death_timeout = 60*5,
        n_workers = 0,                              # Number of workers to start
        account = account,
        queue = partition,
        walltime = time_limit,
        security = True,                            # If True, generate a self-signed certificate to use ssl
        shared_temp_directory = cert_dir,           # Directory to store the self-signed certificates
        job_script_prologue = [
            '#SBATCH --output=' + outdir + '/slurm-%j.out',
            '#SBATCH --job-name=d-worker',
            '#SBATCH --get-user-env',
            '#SBATCH --cpus-per-task=' + str(ncores),
            'echo "-------------------------------"',
            'echo "HOSTNAME:         $(hostname)"',
            'echo "JOBID:            $SLURM_JOB_ID"',
            'echo "NODE_LIST:        $SLURM_JOB_NODELIST"',
            'echo "NCPUS_PER_TASK:   $SLURM_CPUS_PER_TASK"',
            'echo "--------------------------------"',
            'source ' + env_to_source + '/activate',
            'export NUMACTL_CMD="numactl --physcpubind=+0-$((SLURM_CPUS_PER_TASK-1)) --localalloc"',
            'echo "NUMACTL_CMD: $NUMACTL_CMD"',
            'echo "Using python version: "',
            'python --version',
            'echo "......."',
        ]
    )
    return cluster
