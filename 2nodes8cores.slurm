#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-01:00:00
#SBATCH --partition=physical

# Set CPU type.
#SBATCH --constraint=physg1

echo "2 Node 8 Cores"
module purge
module load Python/3.7.3-spartan_gcc-8.1.0
time srun -n 8 python3 app.py data/bigTwitter.json
