
# Run the Model Setup 🚀

## 1. **Set Up the Model Run Directory** 🎂

### a. Open the **OpenPaths EMME Shell** and a terminal and activate your *tm2py* virtual environment: 🐍
```batch
OpenPaths EMME Environment is set to:
OpenPaths EMME 25.00.01.06 64-bit,   Copyright 2025 Bentley Systems, Incorporated

Python Path is set to:
C:\Program Files\Bentley\OpenPaths\EMME 25.00.01\Python311\

C:\Users\lzorn\Documents>E:\GitHub\tm2\tm2py_env\Scripts\activate

(tm2py_env) C:\Users\lzorn\Documents>
```

### b. **Configure Input and Output Paths** ⚙️

If needed, edit the configuration file located at:
[`tm2py-utils/tm2py-utils/config/develop/setup_config_mtc_2015.toml`](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/config/develop/setup_config_mtc_2015.toml)

### c. **Run `setup_model.py`** 🍰

This script is a light wrapper for [SetupModel](/tm2py/api/#tm2py.SetupModel) and it takes two arguments:

1. the location of the setup configuration file from the previous step
2.  the model run directory. 

Either argument can be relative or absolute paths

```batch
(tm2py_env) E:\GitHub\tm2\tm2py>python scripts\setup_model.py E:\GitHub\tm2\tm2py-utils\tm2py-utils\config\develop\setup_config_mtc_2015.toml E:\TM2\2023_TM2_test_20250606
```

TODO: Fix this
*Note: You may need to update the `emmebanks` to the latest version before the model will run.*  
*Also: We used a different `WalkTransitDriveSkims.xls` file in the CTRAMP folder.*

---

## 2. **Run the Model** 🎂

While still in the activated virtual environment:

1. Navigate to the model run directory you setup in the previous step.
2. Run the model:  
```batch
(tm2py_env) E:\GitHub\tm2>cd E:\TM2\2023_TM2_test_20250606

(tm2py_env) E:\TM2\2023_TM2_test_20250606>python RunModel.py
```
   
# User Configuration 🛠️

## Model Configuration ⚙️
The model config file allows for customization on the the model run performance settings.

### Network Acceleration 🚀

Emme Openpaths provides the network accelerate option, which allows for faster assignment on smaller machines.  
WARNING: This has lead to some instability with model runs completing so, especially on large machines, this should remain off 🚧

To enable this under [highway] in toml
```
    network_acceleration=true
```

### Parallel Highway Assignment 🛣️

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

Otherwise, to turn this feature off, explicitly configure tm2py to use 1 thread:
```
    if serial assignment is required comment about the above block and use the below
    [[emme.highway_distribution]]
        time_periods = ["EA", "AM", "MD", "PM", "EV"]
        num_pro
```