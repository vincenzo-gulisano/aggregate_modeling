# 25/04/03

- Trying to move to Michelangelo
- creating a conda environment (needs to be python 3.9 for Flink)
- `conda create -n flink_env python=3.9 -y`
- `conda activate flink_env`
- `python -m pip install apache-flink==2.0.0`
- Then attach the environment to VS Code (cmd + shift + P and then Python: select interpreter)
- Then we try running `testquery.py` and we take it from there
- Fixing stuff, check https://nightlies.apache.org/flink/flink-docs-release-2.0/api/python/examples/datastream/window.html for examples 