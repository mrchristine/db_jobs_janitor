#!/bin/bash

rm -rf ./dep/
mkdir -p dep/
for i in `cat requirements.txt`;
do
  echo "Downloading dep: $i ..."
  pip install --target=dep $i 
done
