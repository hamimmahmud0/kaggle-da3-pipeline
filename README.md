# DA3 Remote Pipeline

This repository is a manager/worker version of the DA3 pipeline, modeled after `kaggle-sam3-pipline` and adapted for a `Fare-Drive` client upload workflow.

## What Is Here

- `automate_da3_remote.py`: local control plane for SSH setup, remote bootstrap, launch, `datop`, and `datalog`
- `da3_remote_pipeline.py`: remote session runtime with worker state, task claiming, and output uploads
- `da3_inference_server.py`: per-GPU inference backend used by the remote workers
- `run_da3_pipeline.sh`: remote launcher wrapper
- `datop` and `datalog`: local convenience commands
- `environment.local.yml`: local manager environment
- `environment.remote.yml`: remote worker environment
- `da3_remote.sample.json`: example config file
- `Fare-Drive/`: embedded file transport client/server project

## Architecture

The local PC acts as the manager and hosts the `Fare-Drive` server.

The remote notebook or server acts as a `Fare-Drive` client only. It logs in with an access token from the local PC server, runs DA3 workers remotely, and uploads completed output directories back to the local PC with `fare-drive client put`.

The remote machine runs:

- the DA3 worker runtime
- one DA3 inference backend per worker
- the `Fare-Drive` client for artifact uploads

## Quick Start

Create the local environment:

```bash
conda env create -f environment.local.yml
conda activate da3-manager
```

Start the `Fare-Drive` server on the local PC and issue an access token there. Then copy the sample config and fill in your values, including `inference_batch_size` for model frame batching:

```bash
cp da3_remote.sample.json da3_remote.json
```

Example `da3_remote.json`:

```json
{
  "host": "127.0.0.1",
  "port": 10022,
  "username": "notebook",
  "password": "YOUR_REMOTE_SSH_PASSWORD",
  "remote_workspace": "/kaggle/working/DA3",
  "remote_miniforge": "/kaggle/working/miniforge3",
  "remote_env_name": "da3-remote",
  "remote_fare_drive_client_home": "/kaggle/working/DA3/.fare-drive-client",
  "local_fare_drive_endpoint": "http://YOUR_PC_HOST:8876",
  "local_fare_drive_access_token": "PASTE_PC_SERVER_ACCESS_TOKEN_HERE",
  "local_fare_drive_upload_root": "da3-output",
  "transport": "fare-drive",
  "drive_folder_url": "https://drive.google.com/drive/folders/1SWlrL2pjpM11mYTZAQCyLZJKjwdAGY76?usp=sharing",
  "manifest_path": "",
  "worker_count": 2,
  "inference_batch_size": 16
}
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

The manager/runtime scaffolding is in place and tested locally. The remaining work is deeper DA3 integration, especially automatic manifest generation from Google Drive inputs and installation of the actual `depth_anything_3` runtime package in the remote environment.
