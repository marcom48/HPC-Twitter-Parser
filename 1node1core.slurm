#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --time=0-01:00:00
#SBATCH --partition=physical

# Set CPU type.
#SBATCH --constraint=physg1

echo "1 Node 1 Core"
module purge
module load Python/3.7.3-spartan_gcc-8.1.0
time srun -n 1 python3 app.py data/bigTwitter.json
