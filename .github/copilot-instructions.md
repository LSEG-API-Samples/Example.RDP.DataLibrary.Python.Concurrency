# Data Library - Project Setup Guide

## Overview

A practical Copilot guide for the current [Data Library for Python](https://developers.lseg.com/en/api-catalog/lseg-data-platform/lseg-data-library-for-python) workspace, focused on validating environment health and running the existing asyncio Gather and TaskGroup notebooks on Windows and macOS.

See [README.md](../README.md) for full project context, architecture diagrams, and rate-limit details.

---

## Agent Notes (Key Conventions)

- **Main notebook**: `notebook/ld_notebook_async_gather.ipynb` — the primary working notebook for concurrent async historical pricing requests using `asyncio.gather()`.
- **Main notebook**: `notebook/ld_notebook_async_taskgroup.ipynb` — the primary working notebook for concurrent async historical pricing requests using `asyncio.TaskGroup`.
- **Companion articles**: `Article_Gather.md` documents the Gather notebook; `Article_Task.md` is its sequel documenting the TaskGroup notebook. Keep both in sync with their respective notebooks.
- **Skill**: `.github/skills/ld-historical-pricing-async/SKILL.md` — reference this skill when converting synchronous single-RIC loops to concurrent `asyncio.gather()`/`asyncio.TaskGroup()` requests, or when adding semaphore-based rate limiting.
- **Credentials**: Copy `notebook/.env.example` → `notebook/.env` and fill in `LSEG_API_KEY`, `LSEG_MACHINE_ID`, and `LSEG_PASSWORD`. Never commit `.env`.
- **Library version**: `lseg-data==2.1.1` is pinned in `requirements.txt`. Do not upgrade without testing — API behavior may change.
- **Rate limits**: The Data Platform enforces per-account request-per-second limits. HTTP **429** errors mean too many concurrent requests — reduce concurrency and retry. See [README.md](../README.md) for details.
- **Logging**: `notebook/lseg-data.config.json` controls Data Library logging. File and console logging are disabled by default; enable them there for debugging.
- **Virtual environment**: Always use `.venv` at the workspace root. Never install packages globally.

## Prerequisites

- Python 3.11 or higher installed and available on your `PATH`
- Git installed and configured
- Network access to PyPI (or a trusted mirror)

## Working Directory (Important)

Run all commands in this guide from the **workspace root** folder.

## Expected Project Layout

Before running setup commands, confirm the project structure includes at least the following layout:

```text
/
├── .github/
│   ├── copilot-instructions.md
│   └── skills/
│       └── ld-historical-pricing-async/
│           └── SKILL.md
├── .gitignore
├── images/
├── LICENSE.md
├── README.md
├── Article_Gather.md
├── Article_Task.md
├── requirements.txt
├── .venv/
└── notebook/
    ├── ld_notebook_async_gather.ipynb
    ├── ld_notebook_async_taskgroup.ipynb
    ├── ld_notebook_gather_performance.ipynb
    ├── ld_notebook_taskgroup_performance.ipynb
    ├── ld_notebook_gethistory_performance.ipynb
    ├── .env
    ├── .env.example
    └── lseg-data.config.json
```

Notes:
- `.venv/` must be inside the workspace root.
- `notebook/` must contain `lseg-data.config.json` and `.env.example`.
- Additional temporary files (for example, `.ipynb_checkpoints/`) may be present.
- Additional notebooks may be present under `notebook/` beyond those listed (for example, exploratory or scratch notebooks); the ones listed above are the primary working notebooks.

---

## Part 1: Check the minimum project file requirements

1. Check if the following files and folders are present in the workspace root and have content:

   - `README.md`
   - `LICENSE.md`
   - `Article_Gather.md`
   - `Article_Task.md`
   - `.gitignore` (must include `.venv/` and `notebook/.env` entries)
   - `images/` (folder must exist)
   - `notebook/` (folder must exist)
   - `requirements.txt` (must exist)
   - `notebook/.env.example` (must exist)
   - `notebook/lseg-data.config.json` (must exist)
  - `.github/skills/ld-historical-pricing-async/SKILL.md` (must exist)

---

## Part 2: Set Up the Python Virtual Environment

> **Prerequisite:** The Part 1 must be completed

1. Create a virtual environment named `.venv` inside the workspace root (same command on all platforms):

   ```bash
   python -m venv .venv
   ```

   This must create `.venv` at the workspace root.

2. Activate the environment:

   - **Windows (PowerShell):**
     ```powershell
     .\.venv\Scripts\Activate.ps1
     ```
   - **Windows (CMD):**
     ```cmd
     .venv\Scripts\activate.bat
     ```
   - **macOS / Linux:**
     ```bash
     source .venv/bin/activate
     ```

3. Update pip to the latest version:

   - **Windows (PowerShell/CMD):**
     ```powershell
     python -m pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org --no-cache-dir --upgrade pip
     ```
   - **macOS:**
     ```bash
     python3 -m pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org --no-cache-dir --upgrade pip
     ```

4. Install the required packages:

   - **Windows (PowerShell/CMD):**
     ```powershell
     python -m pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org --no-cache-dir -r requirements.txt
     ```
   - **macOS:**
     ```bash
     python3 -m pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org --no-cache-dir -r requirements.txt
     ```

5. Verify the installation succeeded:

   - **Windows (PowerShell/CMD):**
     ```powershell
     python -c "import lseg.data; print('lseg-data installed successfully')"
     ```
   - **macOS:**
     ```bash
     python3 -c "import lseg.data; print('lseg-data installed successfully')"
     ```
---
