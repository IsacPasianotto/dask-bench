####
##  Imports
####

# Installed modules:
from dotenv import load_dotenv
import os
import pandas as pd

# Defined modules:
import modules.weakscaling as ws
import modules.clusters as cls
import modules.benchs as benchs
from modules.datahandler import process_results as dh

####
##  Global constants
####

load_dotenv()

KUBE:            bool= True if str(os.getenv('KUBE')).lower() == 'true' else False
RES_DIR:         str = str(os.getenv('RES_DIR'))
RES_FILE_NAME:   str = str(os.getenv('RES_DIR')) + '/' + str(os.getenv('RES_FILE_NAME'))

####
## Main function
####



def main() -> None:
    results_array: list = []
    results_df:    list = []

    if KUBE:
        results_array = benchs.kube_benchmark(setoftasks=ws.assess_arrays, cluster_getter=cls.get_kube_cluster)
        results_df = benchs.kube_benchmark(setoftasks=ws.assess_dataframes, cluster_getter=cls.get_kube_cluster)
    else:
        results_array = benchs.slurm_benchmark(setoftasks=ws.assess_arrays, cluster_getter=cls.get_slurm_cluster)
        results_df = benchs.slurm_benchmark(setoftasks=ws.assess_dataframes, cluster_getter=cls.get_slurm_cluster)

    # Save the raw results for possible future analysis
    with open(RES_FILE_NAME + '_arrays_raw.csv', 'w') as file:
        for result in results_array:
            file.write(result)
    with open(RES_FILE_NAME + '_dataframes_raw.csv', 'w') as file:
        for result in results_df:
            file.write(result)


    # Process the raw data and save the results
    results_array: pd.DataFrame = dh(results_array)
    results_df:    pd.DataFrame = dh(results_df)

    results_array.to_csv(RES_FILE_NAME + '_arrays.csv', index=False)
    results_df.to_csv(RES_FILE_NAME + '_dataframes.csv', index=False)

####
## Start the program
####

if __name__ == '__main__':
    main()
