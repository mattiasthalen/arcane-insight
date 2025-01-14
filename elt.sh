#!/bin/bash

# Record the start time
start_time=$(date +%s)

# ELT
echo "Starting ELT process..."
python ./pipelines/battle_net.py
sqlmesh run

# Record the end time
end_time=$(date +%s)

# Calculate the elapsed time
elapsed_time=$((end_time - start_time))

# Print the elapsed time
echo "Elapsed time: ${elapsed_time} seconds"