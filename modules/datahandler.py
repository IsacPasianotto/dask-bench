####
## Imports
####

from numpy._typing import _UnknownType
import pandas as pd

####
## Global constants
####

AGG_FUNCS = ['median', 'mean', 'min', 'max', 'std']

####
## Function definitions
####

def process_results(data: list) -> pd.DataFrame:
    out: pd.DataFrame = pd.concat(data)
    res: pd.DataFrame = out.groupby(['collection', 'name', 'n', 'unit']).agg(AGG_FUNCS).reset_index()
    # Flatten the multi-level columns
    res.columns = ['_'.join(col).strip() if col[1] else col[0] for col in res.columns.values]
    return res
