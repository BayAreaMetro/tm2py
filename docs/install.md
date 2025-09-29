## Install tm2py 🎂

First, you need to [set up your server](server-setup.md).

### 1. **Clone the Repository** 

Clone the [tm2py GitHub repo](https://github.com/BayAreaMetro/tm2py) using Git Bash or GitHub Desktop, and switch to the branch you want to run.

### 2. **Open the OpenPaths EMME Shell and Create a Virtual Environment** 🐍

Open the **OpenPaths EMME Shell** and:

- Change directory (`cd`) to the parent folder of the `tm2py` GitHub folder.  
- Create a new virtual environment alongside the `tm2py` folder (not inside it):

`python -m venv <your_tm2py_env_name>`


### 3. **Activate the Virtual Environment** ⚡

Activate the virtual environment in the OpenPaths EMME shell:

`<your_tm2py_env_name>\Scripts\activate`

### 4. **Copy `emme.pth` to the Virtual Environment** 

Copy the `emme.pth` file from your OpenPaths EMME installation folder to the virtual environment’s `site-packages` folder. You can do this via shell commands or manually in Windows.

Copy from:  
`C:\Program Files\Bentley\OpenPaths\EMME 24.01.00\emme.pth`  
Copy to:  
`<your_tm2py_env_name>\Lib\site-packages\`

(*Yes, this is one of those quirky things travel modelers do.*)

### 5. **Install `tm2py` in Editable Mode**

In the same activated shell:

- Change into the `tm2py` folder:  
`cd tm2py`

- Install in editable mode:  
`pip install -e .`

### 6. **Verify the Installation**

- In the shell, launch Python:  
`python`

- Try importing tm2py:  
`import tm2py`

You should only see packages listed in `requirements.txt` installed. If the import works without errors, the installation was successful.



