####
## Function definition
####

# Installed modules:
import os
import time
import math
import dask.array as da
import dask.dataframe as dd
import pandas as pd
from dask.distributed import wait
from distributed.client import Client
from dotenv import load_dotenv
from typing import Generator

####
## Global constants
####

load_dotenv()

PROBLEM_SIZE_ARRAY:      int = int(str(os.getenv('PROBLEM_SIZE_ARRAY')))
PROBLEM_SIZE_DATAFRAMES: int = int(str(os.getenv('PROBLEM_SIZE_DATAFRAMES')))

####
## Function definition
####

def perform_benchmark(
        func: Generator,
        client: Client,
    ) -> pd.DataFrame:
    """
    Perform a benchmark of a given function

    :param func:  an assessment function containing a set of tasks
    :param client: a dask client object
    :return: a pandas DataFrame with the results
    """
    n:       int = sum(client.ncores().values())
    out:     list = []

    coroutine = func(n)                     # coroutine: a generator that yields the next task to be performed
    name, unit, numerator = next(coroutine)

    while True:
        start: float = time.time()
        try:
            next_name, next_unit, next_numerator = next(coroutine)
        except StopIteration:
            break
        finally:
            end:     float = time.time()
            record:  dict = {
                'name': name,
                'duration': end - start,
                'unit': unit + '/s',
                'rate': numerator / (end - start),
                'n': n,
                'collection': func.__name__
            }
            out.append(record)
        name, unit, numerator = next_name, next_unit, next_numerator

    return pd.DataFrame(out)


def inc(
        x: int
    ) -> int:
    """
    Increment a value by one. Used to simulate element-wise operations

    :param x: a value
    :return: x + 1
    """
    return x + 1


def assess_arrays(
        n: int
    ) -> Generator:
    """
    Perform most common array operations to assess performance
    in terms of weak scaling

    :param n: number of cores to be used
    """

    N:           int = int(PROBLEM_SIZE_ARRAY * math.sqrt(n))
    chunk_size:  tuple = (int(N / math.sqrt(n)), int(N / math.sqrt(n)))

    x = da.random.randint(0, 1e5, size=(N, N), chunks=chunk_size)
    yield 'create random 2D-array', 'MB', x.nbytes / 1e6
    y = x.persist()
    wait(y)

    yield 'block-wise operation', 'MB', x.nbytes / 1e6
    y = x.map_blocks(inc, dtype=x.dtype).persist()
    wait(y)

    yield 'random access', 'bytes', 8
    x[42, 314].compute()

    yield 'reduction operation (std)', 'MB', x.nbytes / 1e6
    x.std().compute()

    yield 'reduction (std) along axis', 'MB', x.nbytes / 1e6
    x.std(axis=0).compute()

    yield 'elementwise computation', 'MB', x.nbytes / 1e6
    y = da.sin(x) ** 2 + da.cos(x) ** 2
    y = y.persist()
    wait(y)

    yield 'sum the transpose', 'MB', x.nbytes / 1e6
    y = x + x.T
    y = y.persist()
    wait(y)

def assess_dataframes(
        n: int
    ) -> Generator:
    """
    Perform most common dataframe operations to assess performance
    in terms of weak scaling

    :param n: number of cores to be used
    """

    N:      int = PROBLEM_SIZE_DATAFRAMES * n
    N_COL:   int = 6
    columns: list = [f'col_{i}' for i in range(N_COL)]  # needed for group-by operation

    x = da.random.randint(0, 1e5, size=(N, N_COL), chunks=(N / (2 * n), N_COL))

    yield 'create random dataframe', 'MB', x.nbytes / 1e6
    df = dd.from_dask_array(x, columns=columns).persist()
    wait(df)

    yield 'block-wise operation', 'MB', x.nbytes / 1e6
    df2 = df.map_partitions(lambda x: x + 1).persist()
    wait(df2)

    yield 'random access', 'bytes', 8
    df.loc[42].compute()

    yield 'reduction operation (std)', 'MB', x.nbytes / 1e6
    df.std().compute()

    yield 'series reduction operation (std)', 'MB', x.nbytes / 1e6
    df['col_0'].std().compute()

    yield 'group-by operation', 'MB', x.nbytes / 1e6
    df.groupby('col_1')['col_0'].mean().compute()

    yield 'group-by operation (2 columns)', 'MB', x.nbytes / 1e6
    df.groupby('col_1')['col_0', 'col_2'].mean().compute()

    yield 'group-by apply operation', 'MB', x.nbytes / 1e6
    df.groupby('col_1').apply(lambda x: x + 1).compute()

    yield 'order data', 'MB', x.nbytes / 1e6
    wait(df.sort_values('col_1').persist())

