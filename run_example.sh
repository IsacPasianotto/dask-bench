#!/bin/bash
#SBATCH --no-requeue
#SBATCH --job-name="d_master"
#SBATCH --get-user-env
#SBATCH --account=
#SBATCH --partition=
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=30GB
#SBATCH --time=10:00:00

# Standard preamble
echo "---------------------------------------------"
echo "SLURM job ID:        $SLURM_JOB_ID"
echo "SLURM job node list: $SLURM_JOB_NODELIST"
echo "DATE:                $(date)"
echo "HOSTNAME:            $(hostname)"
echo "---------------------------------------------"

envtosource='/path/to/your/env/bin/activate'

source $envtosource

echo "---------------------------------"
echo "Python used: "
python --version
which python3
echo "---------------------------------"

# To avoid the "Too many files open" on SLURMCluster
# See:
# 	https://stackoverflow.com/questions/18280612/ioerror-errno-24-too-many-open-files
# 	https://stackoverflow.com/questions/60062942/dask-too-many-open-files-sockets
# 	https://dask.discourse.group/t/too-many-open-files-on-slurmcluster/54
ulimit -n 50000

python3 main.py
