#! /bin/bash

set +a
source .env
set -a

sudo apt update && sudo apt upgrade -y
sudo apt install -y \
    git \
    curl \
    wget \
    azure-cli

sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update

PY_VER=3.10

sudo apt install -y \
    python3 \
    python3-pip \
    python3-dev \
    python3-venv \
    python3-distutils \
    python$PY_VER \
    python$PY_VER-dev \
    python$PY_VER-venv \
    python$PY_VER-distutils

python3 -m pip install --user pipx
python3 -m pipx ensurepath

$HOME/.local/bin/pipx install poetry

$HOME/.local/bin/poetry config virtualenvs.in-project true
$HOME/.local/bin/poetry install

az login
az account set --subscription $AZURE_SUBSCRIPTION_NAME
