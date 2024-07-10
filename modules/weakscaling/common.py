####
## Imports
####

import time
from collections.abc import generator

import pandas as pd
from distributed.client import default_client

####
## Functions declaration
####

def benchmark_a_set_of_task(
    func:   callable,
    client: dask.distributed.Client = None,
    ) -> pd.DataFrame:
    """
    Take a function that returns a coroutine and run it on a set of tasks.

    :param func: callable
        A function that returns a coroutine
    :param client: dask.distributed.Client
        A Dask client to use for the computation
    :return: pd.DataFrame
        A DataFrame containing the results of the benchmark
    """

    client:     dask.distributed.Client = client or default_client()
    n:          int = sum(client.ncores().values())
    out:        list = []
    # a coroutine is a generator that can be paused and resumed
    coroutine:  generator = func(n)
    name: str, unit: str, numerator: int = next(coroutine)


    while True:
        start: float = time.time()
        try:
            next_name: str, next_unit: str, next_numerator: int = next(coroutine)
        except StopIteration:
            break
        finally:
            end: float = time.time()
            record: dict = {
                'name': name,
                'duration': end - start,
                'unit': unit + '/s',
                'rate': numerator / (end - start),
                'n': n,
                'collection': func.__name__
            }
            out.append(record)
        name: str, unit: str, numerator: int = next_name, next_unit, next_numerator
    return pd.DataFrame(out)