## What happens when you run the model

Setup of model run reads the settings and queues the components for each iteration.

```python
my_controller = tm2py.RunController(config_files,run_dir)
```

```mermaid
flowchart TD
    RunController[["Initiate the controller object\nRunController( config_file, run_dir )\nwhich does the following."]]
    LoadConfig("Load Configuration\ncontroller.config=Configuration.load_toml()")
    Logger("Initiate Logger\ncontroller.logger=Logger()")
    queue_inputs("Queue Components\ncontroller.queue_inputs()")
    LoadConfig-->Logger-->queue_inputs
```

`RunController.queue_inputs()`

```mermaid
flowchart LR
    queue_inputs[["queue_inputs()"]]

    InitialComponents("INITIAL COMPONENTS\nconfig.run.initial_components()")
    Iterations("GLOBAL ITERATIONS\nFor each iteration from config.run.start_iteration to config.run.end_iteration")
    PerIterComponents("COMPONENTS PER ITERATION\nFor each iteration config.run.global_iteration_components")
    IterComponents("COMPONENTS AND ITERATIONS")
    FinalComponents("FINAL COMPONENTS\nconfig.run.final_components")
    Queue("RunController._queued_components")

    queue_inputs-->Iterations
    queue_inputs-->PerIterComponents
    PerIterComponents-->IterComponents
    Iterations-->IterComponents
    queue_inputs-->InitialComponents
    InitialComponents-->Queue
    IterComponents-->Queue
    queue_inputs-->FinalComponents
    FinalComponents-->Queue
```

Example model run configuraiton file with components in the order they are to be run:

```toml
[run]
    start_component = ""
    initial_components = [
        "create_tod_scenarios", 
        "active_modes", 
        "air_passenger", 
        "prepare_network_highway", 
        "highway", 
        "highway_maz_skim", 
        "prepare_network_transit",
        "transit_assign",
        "transit_skim"
    ]
    global_iteration_components = [
        "household", 
        "internal_external", 
        "truck", 
        "highway_maz_assign", 
        "highway", 
        "prepare_network_transit",
        "transit_assign",
        "transit_skim"
    ]
    final_components = []
    start_iteration = 0
    end_iteration = 1

```

Running the model simply iterates through the queued components.

```python
my_run = my_controller.run()
```

```mermaid
flowchart TD
    controller_run[["controller.run()"]]
    validate_inputs("controller.validate_inputs()")
    component_run[["For each item in controller._queued_components\ncomponent.run()"]]
    
    validate_inputs-->component_run
