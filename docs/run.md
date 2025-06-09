
## Run the Model

### 1. **Set Up the Model Run Directory**

#### a. **Activate Your Virtual Environment**

Open a terminal and activate your *tm2py* virtual environment:  
`<your_tm2py_env_name>\Scripts\activate`

#### b. **Configure Input and Output Paths**

Edit the configuration file located at:  
`tm2py/examples/setup_config.toml`

Set your input file paths and expected output (run) paths here.

#### c. **Run `setup-model.ipynb`**

1. In the same virtual environment, launch Jupyter Notebook:  
   `jupyter notebook`

2. Navigate to the notebook:  
   `tm2py/notebooks/setup-model.ipynb`


cessors = "MAX-1"
```

   *Alternatively*, open the `.ipynb` file in **Visual Studio Code** using the correct virtual environment.


3. This notebook:
   - References the example config file: `examples/setup_config.toml`
   - Copies model input files and folder templates from various sources
   - Includes Box links for data sources in the comments
   - Prints setup logs in the model run folder

**Run this notebook to create a clean model directory structure.**

#### d. **Manually Update IP Addresses in CT-RAMP Properties Files**

Update the following files with the correct IP address values:

- `...\CTRAMP\runtime\mtctm2.properties`  
- `...\CTRAMP\runtime\mtcpcrm.properties`  
- `...\CTRAMP\runtime\logsum.properties`

Look for the following keys in each file:

- `RunModel.MatrixServerAddress`
- `RunModel.HouseholdServerAddress`

#### e. **Enable Warm Start Demand**

Edit the file:  
`<your_run_directory>/scenario_config.toml`

Set the model to use *warm start demand* ,like this:


[warmstart]

    warmstart = true

    use_warmstart_skim = false

    use_warmstart_demand = true

*Note: You may need to update the `emmebanks` to the latest version before the model will run.*  
*Also: We used a different `WalkTransitDriveSkims.xls` file in the CTRAMP folder.*

---

### 2. **Run the Model**

While still in the activated virtual environment:

1. Navigate to the model run directory you set in step 1b.  
2. Run the model:  
   `python RunModel.py`
   
   
   
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
        num_pro