####
##  Imports
####

# installed packages
import os
import pandas as pd
from dotenv import load_dotenv

# defined packages
from modules.benchmarks.slurm import benchmark_slurm as bs
from modules.weakscaling.arrays import array_tasks_provider as atp
from modules.weakscaling.dfs import dfs_tasks_provider as dtp

####
##  global variablesextstep=2)

####

load_dotenv()

KUBE:       bool = True if str(os.getenv('KUBE')).lower() == 'true' else False
F_OUT_NAME: str = str(os.getenv('RES_DIR')) + '/' + str(os.getenv('RES_FILE_NAME'))

####
## Main function
####

def main() -> None:

    # 0. Perform the weak scaling benchmark

    results_array: list = []
    results_df:    list = []

    if KUBE:
        print("NOT IMPLEMENTED YET")
        pass
    else:
        results_array = bs(atp, stepby=2)
        results_df    = bs(dtp, stepby=2)

    # 1. Save the obtained data

    f: str = F_OUT_NAME + '_array_raw.csv'
    with open(f, 'w') as file:
        file.write(str(results_array))
        file.close()
    f: str = F_OUT_NAME + '_df_raw.csv'
    with open(f, 'w') as file:
        file.write(str(results_df))
        file.close()

    # 2. Process the data

    agg_fun: list = ['mean', 'std', 'min', 'max', 'median']

    array_pd: pd.DataFrame = pd.concat(results_array)
    df_pd:    pd.DataFrame = pd.concat(results_df)

    array_res: pd.DataFrame = array_pd.groupby(['name', 'n', 'unit']).agg(agg_fun).reset_index()
    df_res:    pd.DataFrame = df_pd.groupby(['name', 'n', 'unit']).agg(agg_fun).reset_index()

    #flatten multi-level columns name
    array_res.columns = ['_'.join(col).strip() for col in array_res.columns.values]
    df_res.columns = ['_'.join(col).strip() for col in df_res.columns.values]

    # 3. Save the processed data

    array_res.to_csv(F_OUT_NAME + '_array.csv', index=False)
    df_res.to_csv(F_OUT_NAME + '_df.csv', index=False)

    print('Benchmark completed successfully!')
    return None

####
##  Run the main function
####

if __name__ == '__main__':
    main()
