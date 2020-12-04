#!/bin/bash
echo "Preparing for Python installation" 
sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
echo "Installing Python3.7" 
sudo apt install python3.7
python3.7 --version 
echo "Python 3.7 is installed"
echo "Installing pip3"
sudo apt update
sudo apt install python3-pip
pip3 --version 
echo "pip3 is installed"