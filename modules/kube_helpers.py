####
# This file contains helpers function needed to create a DaskCluster resources
# on a Kubernetes cluster which can mimic the same behavior of the Slurm cluster
# used in the benchmark loop.
#
# To do that we just adjusted the make_worker_spec, make_scheduler_spec and
# make_cluster specs functions from the dask_kubernetes.operator module.
#
# The original functions are available at:
#         https://kubernetes.dask.org/en/stable/_modules/dask_kubernetes/operator/kubecluster/kubecluster.html#make_worker_spec
#
# The main changes are due to the fact, from hardware constrains (the benchmark was performed on a small cluster),
# we need to assign different resources to the workers and the scheduler. 
####


####
## Imports
####

# Installed modules:
import os
from dask_kubernetes.operator import KubeCluster
from dotenv import load_dotenv

####
## Global constants
####

load_dotenv()

USE_SSL:           bool = True if str(os.getenv('USE_SSL')).lower() == 'true' else None
CERT_DIR:          str = str(os.getenv('CERT_DIR')) if USE_SSL else None
MEM_PER_NODE:      str = str(os.getenv('MEM_PER_NODE'))
NET_INTERFACE:     str = str(os.getenv('NET_INTERFACE'))

KUBE_NAMESPACE:    str = str(os.getenv('KUBE_NAMESPACE'))
CONTAINER_IMAGE:   str = str(os.getenv('CONTAINER_IMAGE'))
SCHEDULER_HOST:    str = str(os.getenv('SCHEDULER_HOST'))
SCHEDULER_CORES:   int = int(str(os.getenv('SCHEDULER_CORES')))
SCHEDULER_MEM:     str = str(os.getenv('SCHEDULER_MEM'))

####
## Function definitions
####

def custom_make_worker_spec(
        resources:      dict,
        image:          str  = CONTAINER_IMAGE,
        n_workers:      int  = 0,
        worker_command: str ="dask-worker",
        container_port: int = 8788,
    ) -> dict:
    """
    Create a dictionary with the specifications of the workers to be spawned in the
    Kubernetes cluster. The dictionary is compatible with the DaskWorker class.

    :param resources:      Dict with the resources to assign to each worker. It must have the following structure:
                            {
                                "requests": {"memory": "xxGi", "cpu": "xx"},
                                "limits": {"memory": "xxGi", "cpu": "xx"}
                            }
    :param image:          Container image to use for the workers
    :param n_workers:      Number of workers to spawn, default is 0 because the scaling is done with the KubeCluster.scale method
    :param container_port: Port to use for the dashboard
    :return:               A dictionary with the worker specifications
    """

    worker_command_lst: list[str] = worker_command.split(" ")
    args: list[str] = worker_command_lst + [
        "--name",
        "$(DASK_WORKER_NAME)",
        "--dashboard",
        "--dashboard-address",
        str(container_port),
    ]

    return {
        "replicas": n_workers,
        "spec": {
            "containers": [
                {
                    "name": "worker",
                    "image": image,
                    "args": args,
                    "resources": resources,
                    "ports": [
                        {
                            "name": "http-dashboard",
                            "containerPort": container_port,
                            "protocol": "TCP",
                        },
                    ],
                }
            ]
        },
    }


def custom_make_scheduler_spec(
        cluster_name:            str,
        resources:               dict,
        image:                   str  = CONTAINER_IMAGE,
        scheduler_host:          str  = SCHEDULER_HOST,
        scheduler_service_type:  str  ="ClusterIP",
        jupyter:                 bool = False,
        tcp_port:                int  = 8786,
        container_port:          int  = 8787,
        readiness_initial_delay: int  = 0,
        readiness_period:        int  = 1,
        readiness_timeout:       int  = 300,
        liveness_delay:          int  = 15,
        liveness_period:         int  = 20,
    ) -> dict:
    """
    Create a dictionary with the specifications of the scheduler to be spawned in the
    Kubernetes cluster. The dictionary is compatible with the DaskScheduler class.

    :param cluster_name:            Name of the cluster
    :param resources:               Dict with the resources to assign to the scheduler. It must have the following structure:
                                    {
                                        "requests": {"memory": "xxGi", "cpu": "xx"},
                                        "limits": {"memory": "xxGi", "cpu": "xx"}
                                    }
    :param image:                   Container image to use for the scheduler
    :param scheduler_host:          Hostname of the node where the scheduler will run
    :param scheduler_service_type:  Type of service to use for the scheduler
    :param jupyter:                 Flag to enable the Jupyter dashboard
    :param tcp_port:                Port to use for the communication
    :param container_port:          Port to use for the dashboard
    :param readiness_initial_delay: Initial delay for the readiness probe
    :param readiness_period:        Period for the readiness probe
    :param readiness_timeout:       Timeout for the readiness probe
    :param liveness_delay:          Initial delay for the liveness probe
    :param liveness_period:         Period for the liveness probe
    :return:                        A dictionary with the scheduler specifications
    """

    args = ["dask-scheduler", "--host", "0.0.0.0"]
    if jupyter:
        args.append("--jupyter")

    return {
        "spec": {
            "containers": [
                {
                    "name": "scheduler",
                    "image": image,
                    "args": args,
                    "resources": resources,
                    "ports": [
                        {
                            "name": "tcp-comm",
                            "containerPort": tcp_port,
                            "protocol": "TCP",
                        },
                        {
                            "name": "http-dashboard",
                            "containerPort": container_port,
                            "protocol": "TCP",
                        },
                    ],
                    "readinessProbe": {
                        "httpGet": {"port": "http-dashboard", "path": "/health"},
                        "initialDelaySeconds": readiness_initial_delay,
                        "periodSeconds": readiness_period,
                        "timeoutSeconds": readiness_timeout,
                    },
                    "livenessProbe": {
                        "httpGet": {"port": "http-dashboard", "path": "/health"},
                        "initialDelaySeconds": liveness_delay,
                        "periodSeconds": liveness_period,
                    },
                }
            ],
            "nodeName": scheduler_host,
        },
        "service": {
            "type": scheduler_service_type,
            "selector": {
                "dask.org/cluster-name": cluster_name,
                "dask.org/component": "scheduler",
            },
            "ports": [
                {
                    "name": "tcp-comm",
                    "protocol": "TCP",
                    "port": tcp_port,
                    "targetPort": "tcp-comm",
                },
                {
                    "name": "http-dashboard",
                    "protocol": "TCP",
                    "port": container_port,
                    "targetPort": "http-dashboard",
                },
            ],
        },
    }



def custom_make_cluster_spec(
        name:                      str,
        resources:                 dict,
        image:                     str  = CONTAINER_IMAGE,
        n_workers:                 int  = 0,
        worker_command:            str  = "dask-worker",
        scheduler_service_type:    str  = "ClusterIP",
        idle_timeout:              int  = 0,
        jupyter:                   bool = False,
        scheduler_cores:           int  = SCHEDULER_CORES,
        scheduler_mem:             str  = SCHEDULER_MEM,
):
    """
    Create a dictionary with the specifications of the cluster to be spawned in the
    Kubernetes cluster. The dictionary is compatible with the DaskCluster class.

    :param name:                    Name of the cluster
    :param resources:               Dict with the resources to assign to the worker. It must have the following structure:
                                    {
                                        "requests": {"memory": "xxGi", "cpu": "xx"},
                                        "limits": {"memory": "xxGi", "cpu": "xx"}
                                    }
    :param image:                   Container image to use for the workers and the scheduler
    :param n_workers:                Number of workers to spawn, default is 0 because the scaling is done with the KubeCluster.scale method
    :param worker_command:           Command to run on the worker pod
    :param scheduler_service_type:   Type of service to use for the scheduler
    :param idle_timeout:             Timeout for the idle workers
    :param jupyter:                  Flag to enable the Jupyter dashboard
    :param scheduler_cores:          Number of cores to assign to the scheduler, default is taken from the environment variables
    :param scheduler_mem:            Amount of memory to assign to the scheduler, default is taken from the environment variables
    :return:                        A dictionary with the cluster specifications compliant to the DaskCluster class constructor
    """

    # create the resources dict for the scheduler pods
    resources_scheduler: dict = {
        "requests": {
            "memory": scheduler_mem + "Gi",
            "cpu": str(scheduler_cores)
            },
        "limits": {
            "memory": scheduler_mem + "Gi",
            "cpu": str(scheduler_cores)
            },
        }

    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskCluster",
        "metadata": {
            "name": name,
            "namespace": KUBE_NAMESPACE,
        },
        "spec": {
            "idleTimeout": idle_timeout,
            "worker": custom_make_worker_spec(
                resources=resources,
                worker_command=worker_command,
                n_workers=n_workers,
                image=image,
            ),
            "scheduler": custom_make_scheduler_spec(
                cluster_name=name,
                resources=resources_scheduler,
                image=image,
                scheduler_service_type=scheduler_service_type,
                jupyter=jupyter,
            ),
        },
    }

