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

python3 main.py
