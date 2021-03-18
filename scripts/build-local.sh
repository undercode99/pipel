#!/bin/bash

python -m unittest discover -v
pip uninstall pipel -y
python setup.py sdist bdist_wheel
pip install dist/pipel-1.0.0-py3-none-any.whl