USAGE = """

Setup a TM2 model given a setup_config.toml file

"""
import argparse, pathlib, traceback
import tm2py

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("setup_config_toml", type=pathlib.Path, help="The setup_config.toml to use. Can be absolute or relative.")
    parser.add_argument("model_dir", type=pathlib.Path, help="The model directory. Can be absolute or relative.")
    args = parser.parse_args()
    
    print("Running tm2py.setup_model.setup.SetupModel with")
    print(f"setup_config_toml: {args.setup_config_toml.resolve()}")
    print(f"        model_dir: {args.model_dir.resolve()}")
    print("")
    print(f"See log file: {args.model_dir.resolve() / 'setup.log'}")

    try:
        setup_model = tm2py.setup_model.setup.SetupModel(config_file=args.setup_config_toml, model_dir=args.model_dir)
        setup_model.run_setup()
    except Exception as inst:
        print(inst)
        trace = traceback.format_exc()
        print(trace)
        print("Setup failed")
