####
## Imports
####

# installed packages
import os
import time
import dask_jobqueue
from dotenv import load_dotenv
from dask.distributed import Client, wait

# defined packages

# import weakscaling
from modules.clusters.slurm import slurm_cluster_getter as scg
from modules.weakscaling.common import benchmark_a_set_of_task

####
## global variables
####

load_dotenv()

MAX_N_CORE:     int = int(str(os.getenv('MAX_N_CORE')))
OBS_PER_ITER:   int = int(str(os.getenv('OBS_PER_ITER')))
TIMEOUT :       int = int(str(os.getenv('TIMEOUT')))
POLL_INTERVAL:  int = int(str(os.getenv('POLL_INTERVAL')))
CORES_PER_NODE: int = int(str(os.getenv('CORES_PER_NODE')))
VERBOSE:        bool = True if str(os.getenv('VERBOSE')).lower() == 'true' else False

####
## Function definitions
####


def cluster_is_ready(
        client:        Client,
        ncpus:         int,
        timeout:       int = TIMEOUT,
        poll_interval: int = POLL_INTERVAL
    ) -> bool:
    """
    Ensure that the cluster is up and running before proceeding

    :param client: dask.distributed.Client
         Dask client used in the cluster
    :param ncpus: int
            Number of CPUs requested in the current job
    :param timeout: int
            Maximum time to wait before giving up
    :param poll_interval: int
            Time interval between consecutive checks of the cluster status
    """

    start_time: float = time.time()
    while time.time() - start_time < timeout:
        workers: dict = client.scheduler_info()["workers"]
        if (sum([workers[w]["nthreads"] for w in workers.values()]) >= ncpus):
            return True
        time.sleep(poll_interval)
    return False


def benchmark_slurm(
        setoftasks:    callable,
        stepby:        int = 2
        obs_per_iter:  int = OBS_PER_ITER
    ) -> list:
    """
    Perform the weak scaling benchmark on a SLURM cluster. It generates one or more workers
    asking to the SLURM resources manager, scaling the number of cores asked from 1 to MAX_N_CORE
    and processing nobs observations for each configuration.

    :param setoftasks: callable
            Function that performs all the tasks the benchmark is supposed to do
            it must be defined like the 'execute_tasks' functions in the 'weakscaling' module
    :param stepby: int
            Increase the number of cores by this value at each iteration
    :param obs_per_iter: int
            Number of observations to process for each configuration
    :return: list
            List containing a dictionary for each configuration tested, with the following
            keys:
            - 'name':       str, name of the configuration
            - 'duration':   float, duration of the benchmark
            - 'unit':       str, unit of the duration
            - 'rate':       str, rate of observations processed per second
            - 'n':          int, number of cores used
            - 'collection': str, string that describe the operation performed
    """

    results: list = []

    for ncores in range(1, MAX_N_CORE+1, stepby):
        # 1. Compute preliminary needed values
        node_needed:      int ((ncores - 1) // CORES_PER_NODE) + 1
        cores_per_worker: int = ncores // node_needed

        try:
            # 2. Get the cluster and client
            cluster: dask_jobqueue.SLURMCluster = scg(ncores)
            client:  dask.distributed.Client    = Client(cluster)
            if cluster_is_ready(client=client, ncpus=ncores):
                if VERBOSE:
                    print(f"Cluster ready with {ncores} cores", flush=True)
                # 3. Perform the tasks
                for i in range(obs_per_iter):
                    if VERBOSE:
                        print(f"Assessing {setoftasks.__name__} with {ncores} cores", flush=True)
                    try:
                        results.append(benchmark_a_set_of_task(setoftasks, client))
                    except Exception as e:
                        print(f"Error in task execution: {e}")
                        client.close()
                        cluster.close()
                    time.sleep(1)
            else:
                 print("Cluster did not scaled yet")
        except Exception as e:
            print(f"Error in cluster creation: {e}")
        # ensure that the cluster is closed in any case
        finally:
            if client:
                client.close()
            if cluster:
                cluster.close()

    return results