####
## Imports
####

# Installed modules:
import os
import time
import pandas as pd
from dotenv import load_dotenv
from dask_jobqueue import SLURMCluster
from dask_kubernetes.operator import KubeCluster
from dask.distributed import Client

# Defined modules:
import modules.weakscaling as ws

####
## Global constants
####

load_dotenv()

OBS_PER_ITER:   int = int(str(os.getenv('OBS_PER_ITER')))
TIMEOUT:        int = int(str(os.getenv('TIMEOUT')))
POLL_INTERVAL:  int = int(str(os.getenv('POLL_INTERVAL')))
CORES_PER_NODE: int = int(str(os.getenv('CORES_PER_NODE')))
MAX_N_CORES:    int = int(str(os.getenv('MAX_N_CORES')))
STEP_BY:        int = int(str(os.getenv('STEP_BY')))
VERBOSE:        bool = True if str(os.getenv('VERBOSE')).lower() == 'true' else False

USE_SSL:        bool = True if str(os.getenv('USE_SSL')).lower() == 'true' else False
CERT_DIR:       str = str(os.getenv('CERT_DIR'))

####
## Function definition
####

def cluster_is_ready(
        client:         Client,
        ncpus:          int,
        timeout:        int = TIMEOUT,
        poll_interval:  int = POLL_INTERVAL
    ) -> bool:
    """
    Check if all the worker the client is expected to have are ready.

    :param client:          a dask client object
    :param ncpus:           number of cpus the client is expected to have
    :param timeout:         maximum time to wait for the cluster to scale
    :param poll_interval:   interval between checks
    :return:                True if the cluster is ready, False otherwise
    """

    start_time: float = time.time()

    while time.time() - start_time < timeout:
        workers: dict = client.scheduler_info()['workers']
        workers = client.scheduler_info()['workers']
        if sum([worker['nthreads'] for worker in workers.values()]) >= ncpus:
            return True
        time.sleep(poll_interval)

    return False


def slurm_benchmark(
        setoftasks,
        cluster_getter,
        max_n_cores:     int = MAX_N_CORES,
        step_by:         int = STEP_BY,
        obs_per_iter:    int = OBS_PER_ITER,
        verbose:         bool = VERBOSE
    ) -> list:
    """
    Perform a benchmark on a SLURM cluster. The code initially will start filling an entire node.
    When the number of cores exceeds the number of cores per node, it will start filling two nodes
    simultaneously, splitting the total number of cores equally among nodes. The process will continue.
    Default values of the parameters are taken from the environment variables.

    :param setoftasks:      a set of tasks to perform
    :param cluster_getter:  a function that returns a SLURM cluster
    :param max_n_cores:     maximum number of cores to use
    :param step_by:         increment of the number of cores at each iteration
    :param obs_per_iter:    number of observations to perform at each iteration
    :param verbose:         print information about the process
    :return:                a list of results
    """

    results: list = []

    for ncpus in range(0, max_n_cores+1, step_by):

        # In any case we want the serial case with 1 core
        ncpus = ncpus if ncpus > 0 else 1

        # check how many nodes are needed
        node_needed:        int = ((ncpus - 1) // CORES_PER_NODE) +  1
        core_per_job_node:  int = ncpus // node_needed

        try:

            cluster: SLURMCluster = cluster_getter(core_per_job_node)
            client:  Client = Client(cluster)
            cluster.scale(jobs = node_needed)

            if verbose:
                print(f'Scaling to {ncpus} cores...', flush=True)

            if not cluster_is_ready(client, ncpus):
                print(f'Cluster did not scale to {ncpus} cores in time. Exiting...', flush=True)
                client.close()
                cluster.close()
                raise TimeoutError(f'Cluster did not scale to {ncpus} cores in {TIMEOUT} seconds.')

            for j in range(obs_per_iter):
                if verbose:
                    print(f'Running {setoftasks.__name__} with {ncpus} cores, iteration {j + 1}/{obs_per_iter}', flush=True)
                try:
                    results.append(ws.perform_benchmark(setoftasks, client))
                except Exception as e:
                    print(f'Error: {e}')
                    client.close()
                    client=Client(cluster)
                time.sleep(5)

        except Exception as e:
            print(f'Error: {e}')
        finally:
            client.close()
            cluster.close()

    return results

from distributed.security import Security

def kube_benchmark(
        setoftasks,
        cluster_getter,
        max_n_cores:     int = MAX_N_CORES,
        step_by:         int = STEP_BY,
        obs_per_iter:    int = OBS_PER_ITER,
        verbose:         bool = VERBOSE,
        use_ssl:         bool = USE_SSL,
        cert_dir:        str = CERT_DIR
    ) -> list:
    """
    Perform a benchmark on a Kubernetes cluster. The code initially will start filling an entire node.
    When the number of cores exceeds the number of cores per node, it will start filling two nodes
    simultaneously, splitting the total number of cores equally among nodes. The process will continue.
    Default values of the parameters are taken from the environment variables.

    :param setoftasks:      a set of tasks to perform
    :param cluster_getter:  a function that returns a Kubernetes cluster
    :param max_n_cores:     maximum number of cores to use
    :param step_by:         increment of the number of cores at each iteration
    :param obs_per_iter:    number of observations to perform at each iteration
    :param verbose:         print information about the process
    :param use_ssl:         use SSL for the connection
    :param cert_dir:        directory where the certificates are stored
    :return:                a list of results
    """

    results: list = []

    for ncpus in range(0, max_n_cores+1, step_by):
        # In any case we want the serial case with 1 core
        ncpus = ncpus if ncpus > 0 else 1

        # check how many nodes are needed
        node_needed:        int = ((ncpus - 1) // CORES_PER_NODE) +  1
        core_per_pod:  int = ncpus // node_needed

        client = None
        cluster = None

        try:
            cluster = cluster_getter(core_per_pod)
            cluster.scale(n=node_needed, worker_group = 'default')  # will use the one defined in the yaml file

            if use_ssl:
                sec = Security(
                    tls_ca_file = f'{cert_dir}/tls.crt',
                    tls_client_cert = f'{cert_dir}/tls.crt',
                    tls_client_key = f'{cert_dir}/tls.key',
                    require_encryption = True)
                client = Client(cluster, sec)
            else:
                client = Client(cluster)

            if verbose:
                print(f'Cluster created!')

            if cluster_is_ready(client, ncpus):

                if verbose:
                    print(f'Cluster scaled to {ncpus} cores')

                for j in range(obs_per_iter):
                    if verbose:
                        print(f'Running {setoftasks.__name__} with {ncpus} cores, iteration {j + 1}/{obs_per_iter}', flush=True)
                    try:
                        results.append(ws.perform_benchmark(setoftasks, client=client))
                    except Exception as e:
                        print(f'Error: {e}')
                        client.close()
                        client=Client(cluster)
                    time.sleep(5)

            else:
                print("Cluster did not scaled yet")

        except Exception as e:
            print(f"ERROR: {e}")
        finally:
            if client:
                client.close()
            if cluster:
                cluster.close()
    return results
