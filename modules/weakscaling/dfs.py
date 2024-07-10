####
## Imports
####

import math
import os
import dask.dataframe as dd
from dask.distributed import wait
from dotenv import load_dotenv

####
## global variables
####

load_dotenv()

PROBLEM_SIZE: int = int(str(os.getenv('PROBLEM_SIZE_DATAFRAMES')))

####
## Function definitions
####

def dfs_tasks_provider(
        n: int
    ) -> None:
    """
    Perform some of the most common dataframe operations in Dask

    :param n: int
        Number of cores to use in the computation
    """

    # Weak scaling --> the workload per core remains constant
    N:              int = int(PROBLEM_SIZE * math.sqrt(n))
    N_COL:          int = 6

    x: da.Array = da.random.randint(0, 1e5, size=(N, N_COL), chunks=(N / (2 * n), N_COL))
    columns: list = [f'col_{i}' for i in range(N_COL)]                                  # needed for group-by operation

    yield 'create random dataframe', 'MB', x.nbytes / 1e6
    df: dd.DataFrame = dd.from_dask_array(x, columns=columns).persist()
    wait(df)

    yield 'block-wise operation', 'MB', x.nbytes / 1e6
    df2: dd.DataFrame = df.map_partitions(lambda x: x + 1).persist()
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

    # Error: to many files open!
    # yield 'join data', 'MB', x.nbytes / 1e6
    # m: dd.DataFrame = df.merge(df2, on='col_1').persist()
    # wait(m)

    # Free up all memory used by the client
    df = None
    df2 = None
    m = None