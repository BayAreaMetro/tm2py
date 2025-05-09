
# Installation

First you need to [setup your server](server-setup.md).

1. Clone the [tm2py repo](https://github.com/BayAreaMetro/tm2py) and switch to the branch you want to run (with Git Bash or GitHub desktop).

2. Open the ``OpenPaths EMME Shell``.
    a. In the OpenPaths Shell, cd to the parent folder of the tm2py GitHub folder. Create a new virtual environment in that folder (alongside the tm2py folder, not within it): 
    
    python -m venv <your_tm2py_env_name>

3. Activate your virtual environment in the EMME shell:
   <your_tm2py_env_name>\Scripts\activate

4. Copy the emme.pth file from the OpenPaths EMME installation folder to the virutal environment. You can copy in the shell with shell commands or just do things in Windows. (This part feels like the crazy things travel modelers do because we are such a small field.)

Copy "C:\Program Files\Bentley\OpenPaths\EMME 24.01.00\emme.pth" to <your_tm2py_env_name>\Lib\site-packages\

5. In the OpenPaths EMME shell activated from step 2, install tm2py from local clone in editable mode
a.	cd to the tm2py GitHub folder cd tm2py
b.	pip install -e .
For developers: if you would like to install additional dev or doc dependencies,
c.	pip install -e .[dev,doc]

6.	In step 4 loggings, you should expect to see only the packages listed in the requirements.txt are installed. After step 4 completes, you can try importing tm2py to verify if there's any quick dependency error.
a.	In the shell, type python
b.	type import tm2py




