# QBO Project — WSL & Python Environment Setup

This document describes a **clean, repeatable setup** for developing the QBO system inside **WSL (Ubuntu)** using Python best practices.  
It separates **system-level tooling** from **project-level environments** to minimize breakage and make recovery fast.


## 0. Scope & Philosophy

* **System Python is for bootstrapping only** (creating virtual environments, pipx tools).
* **All project dependencies live in isolated envs** (venv / Poetry / Conda).
* **Global Python CLIs** (Poetry, Jupyter, etc.) are installed via **pipx**, not pip.
* This setup is designed to survive repo nukes, machine rebuilds, and high-stress recovery.

## 1. One-time WSL System Setup (Global)

### 1.1 Update Ubuntu packages

```bash
sudo apt update && sudo apt upgrade -y
```

**What this does**

* Refreshes the local package index (`apt update`)
* Upgrades all installed packages with safe upgrades (`apt upgrade -y`)

**Why this matters**

* Security patches and bug fixes
* Prevents stale package metadata (404s, version mismatches)
* Reduces dependency conflicts

**Optional cleanup**

```bash
sudo apt autoremove
sudo apt autoclean
```

If dependency removals are required during upgrade:

```bash
sudo apt full-upgrade
```

---

### 1.2 Install essential build & tooling packages

```bash
sudo apt install -y build-essential curl git pkg-config unzip
```

**Packages explained**

* `build-essential` → GCC/G++, `make`, headers (needed for compiling Python extensions)
* `curl` → HTTP client (install scripts, downloads, APIs)
* `git` → version control
* `pkg-config` → helps compilers locate system libraries

---

### 1.3 Why this matters for Python wheels

* Python prefers installing **wheels (`.whl`)** — prebuilt binary packages
* If a wheel is unavailable, pip falls back to **source distributions (`sdist`)**
* Building from source requires:

  * compilers (`build-essential`)
  * headers (`python3-dev`)
  * system libs (`libssl-dev`, etc.)

**Common add-ons when builds fail**

```bash
sudo apt install -y python3-dev
sudo apt install -y libssl-dev libffi-dev
sudo apt install -y zlib1g-dev libbz2-dev liblzma-dev
sudo apt install -y libxml2-dev libxslt1-dev
```

## 2. Install System Python (Bootstrap Only)

```bash
sudo apt install -y python3 python3-venv python3-pip
```

**What each package does**

* `python3` → system Python interpreter (`/usr/bin/python3`)
* `python3-venv` → enables `python -m venv`
* `python3-pip` → Python package manager

**Critical rule**

> Never install project libraries into system Python.

System Python is managed by `apt`. Polluting it with `pip install` can:

* break OS tools
* mix system and user dependencies
* cause silent failures after Ubuntu upgrades

All real work happens inside **virtual environments**.

## 3. pipx — Global Python CLI Tools (Isolated)

### 3.1 Install pipx

```bash
sudo apt install -y pipx
pipx ensurepath
```

> Restart your shell (or reboot) after this step.

**What pipx does**

* Installs Python CLI apps into **dedicated virtual environments**
* Exposes their executables on your PATH
* Prevents global dependency pollution

Internally:

* venvs live in `~/.local/pipx/venvs/<app>/`
* executables are symlinked into `~/.local/bin/`

---

### 3.2 pip vs pipx (important distinction)

| Tool           | Purpose                                        |
| -------------- | ---------------------------------------------- |
| `pip install`  | Installs into the *active* Python environment  |
| `pipx install` | Installs a *standalone CLI app* in its own env |

Use:

* `pip` → project dependencies (inside venv / Poetry)
* `pipx` → tools like Poetry, Jupyter, Black, Ruff

---

### 3.3 Install global Python tools

```bash
pipx install poetry
pipx install --include-deps jupyter
```

**Why `--include-deps` for Jupyter**

* `jupyter` is a **meta package**
* It depends on apps like `jupyter-core`, but doesn’t expose a CLI itself
* `--include-deps` ensures dependent CLIs are available on PATH

## 4. Java Setup (Required for PySpark)

### 4.1 Install OpenJDK 17

```bash
sudo apt install -y openjdk-17-jdk
```

* Installs Java compiler + runtime
* Java 11 or 17 is standard for PySpark

---

### 4.2 Configure JAVA_HOME

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

**Why this matters**

* Many tools (PySpark, JVM-based libs) rely on `JAVA_HOME`
* Putting Java first on PATH ensures correct `java` / `javac`

---

## 5. Project-Level Setup (Per Repo)

> This is **not** global. Repeat per project.

Typical flow:

```bash
git clone <repo>
cd <repo>
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Or with Poetry:

```bash
poetry install
poetry shell
```

---

## 6. Recovery Principle (Why this exists)

If the repo or environment is destroyed:

1. Re-run **Sections 1–4** only once per machine
2. Clone repo
3. Recreate venv
4. Restore `.env`

Total recovery time: **minutes, not hours**.

---

## 7. Final Rule of Thumb

> System = boring, stable, untouched
> Project = isolated, disposable, reproducible

If you follow this, nuking a repo is an inconvenience — not a disaster.
