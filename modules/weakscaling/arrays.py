####
## Imports
####

import math
import os
import dask.array as da
from dask.distributed import wait
from dotenv import load_dotenv

####
## global variables
####

load_dotenv()

PROBLEM_SIZE: int = int(str(os.getenv('PROBLEM_SIZE_ARRAY')))

####
## Function definitions
####

def inc(x: int) -> int:
    """
    Increment a number by one, used to simulate a element-wise operation

    :param x: int
        Number to increment
    :return: int
        Number incremented by one
    """
    return x + 1

def array_tasks_provider(
        n: int
    ) -> da.Array:

    """
    Perform some of the most common array operations in Dask

    :param n: int
        Number of cores to use in the computation
    """

    # Weak scaling --> the workload per core remains constant
    N:              int = int(PROBLEM_SIZE * math.sqrt(n))
    chunk_size:     tuple = (int(N / math.sqrt(n)), int(N / math.sqrt(n)))
    x: da.Array = da.random.randint(0, 1e5, size=(N, N), chunks=chunk_size)

    yield 'create random 2D-array', 'MB', x.nbytes / 1e6
    y: da.Array = x.persist()
    wait(y)

    # map_blocks returns a new dask array with the same block structure but
    # with each block replaced by the result of applying the function func to that block.
    yield 'block-wise operation', 'MB', x.nbytes / 1e6
    y: da.Array = x.map_blocks(inc, dtype=x.dtype).persist()
    wait(y)

    yield 'random access', 'bytes', 8
    x[42, 314].compute()

    yield 'reduction operation (std)', 'MB', x.nbytes / 1e6
    x.std().compute()

    yield 'reduction (std) along axis', 'MB', x.nbytes / 1e6
    x.std(axis=0).compute()

    yield 'elementwise computation', 'MB', x.nbytes / 1e6
    y: da.Array = da.sin(x) ** 2 + da.cos(x) ** 2
    y: da.Array = y.persist()
    wait(y)

    yield 'sum the transpose', 'MB', x.nbytes / 1e6
    y: da.Array = x + x.T
    y = y.persist()
    wait(y)

    # Free up all memory used by the client
    x = None
    y = None