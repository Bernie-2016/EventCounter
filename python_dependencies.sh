#!/bin/bash

set -e

source $1/bin/activate && \
    pip install -r requirements.txt --allow-all-external

echo "source $1/bin/activate" >> ~vagrant/.bashrc
echo "cd /vagrant" >> ~vagrant/.bashrc
