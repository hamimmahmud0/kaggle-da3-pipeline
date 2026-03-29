# DA3 Remote Pipeline

This repository is a manager/worker version of the DA3 pipeline, modeled after the `kaggle-sam3-pipline` flow and extended with `Fare-Drive` support.

## What Is Here

- `automate_da3_remote.py`: local control plane for SSH setup, remote bootstrap, launch, `datop`, and `datalog`
- `da3_remote_pipeline.py`: remote session runtime with worker state, task claiming, and background launch
- `da3_inference_server.py`: per-GPU inference backend used by the remote workers
- `run_da3_pipeline.sh`: remote launcher wrapper
- `datop` and `datalog`: local convenience commands
- `environment.local.yml`: local manager environment
- `environment.remote.yml`: remote worker environment
- `da3_remote.sample.json`: example config file
- `Fare-Drive/`: embedded file transport subsystem used for remote-to-local synchronization

## Architecture

The local machine acts as the manager.

It connects to the remote notebook or server over SSH, uploads the runtime files, creates the remote environment, starts the remote `Fare-Drive` server, issues an access token, initializes session state, and launches one worker per GPU.

The remote machine runs:

- the DA3 worker runtime
- one DA3 inference backend per worker
- the `Fare-Drive` server for artifact transport

## Quick Start

Create the local environment:

```bash
conda env create -f environment.local.yml
conda activate da3-manager
```

Copy the sample config and fill in your values:

```bash
cp da3_remote.sample.json da3_remote.json
```

Verify the remote host:

```bash
python automate_da3_remote.py verify --config-file da3_remote.json
```

Bootstrap the remote side:

```bash
python automate_da3_remote.py setup --config-file da3_remote.json
```

Launch the workers:

```bash
python automate_da3_remote.py launch --config-file da3_remote.json
```

Watch status or logs:

```bash
./datop --config-file da3_remote.json
./datalog --config-file da3_remote.json
```

## Tests

Run the repo-level unit tests:

```bash
PYTHONPATH=. python3 -m unittest discover -s tests
```

Run the embedded `Fare-Drive` integration tests:

```bash
cd Fare-Drive
PYTHONPATH=src python3 -m unittest tests.test_integration
```

## Current Scope

The manager/runtime scaffolding is in place and tested locally. The remaining work is deeper pipeline integration, especially automatic manifest generation from Google Drive inputs and end-to-end artifact sync behavior driven by completed DA3 tasks.
