#!/usr/bin/bash
ROOT_PATH=/Users/rois.haqq/Documents/sharing/Dibimbing_id_DE_Bootcamp/python/assignment #adjust this

export WORK_DIR="${ROOT_PATH}"
export TKPENV=local


#    \$1 is param of bash command
#    bash python/assignment/run_app.sh bulk
#    bash python/assignment/run_app.sh update
#    \$1 => normal or update
export BUILDERAPP=$1  

python3 ${WORK_DIR}/app.py