"Test commercial vehicle model."

import tm2py.examples


def test_commercial_vehicle(inro_context, examples_dir, root_dir):
    "Tests that commercial vehicle component can be run."
    from tools import test_component

    tm2py.examples.get_example(example_name="UnionCity", root_dir=root_dir)

    my_run = test_component(examples_dir, "truck")
    my_run.run_next()

    # TODO write assert
