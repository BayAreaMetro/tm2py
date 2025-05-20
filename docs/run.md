
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

6.	In step 4 loggings, you should expect to see only the packages listed in the requirements.txt are installed. After step 4 completes, you can try importing tm2py to verify if there's any quick dependency error.
a.	In the shell, type python
b.	type import tm2py


## User Configuration

### model_config.toml
the model config file allows for customization on the the model run performance settings.

1) network acceleration

Emme Openpaths provides the network accelerate option, which allows for faster assignment on smaller machines.  
WARNING: This has lead to some instability with model runs completing so, especially on large machines, this should remain off 

to enable this under [highway] in toml
```
    network_acceleration=true
```

2) Parallel Highway Assignment

tm2py offers the option to run assignment in parallel to reduce runtime. This can be achieved by including the following configuration under [emme] in the model_config.
```
    [[emme.highway_distribution]]
        time_periods = ["AM"]
        num_processors = "MAX/3"
    [[emme.highway_distribution]]
        time_periods = ["PM"]
        num_processors = "MAX/3"
    [[emme.highway_distribution]]
        time_periods = ["EA", "MD", "EV"]
        num_processors = "MAX/3"
```

otherwise, to turn this feature off, explicitly configure tm2py to use 1 thread:
```
    if serial assignment is required comment about the above block and use the below
    [[emme.highway_distribution]]
        time_periods = ["EA", "AM", "MD", "PM", "EV"]
        num_processors = "MAX-1"
```


