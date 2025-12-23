import sys
sys.path.insert(0, r'C:\Program Files\INRO\Emme\Emme 4\Emme-4.6.0\Python39\Lib\site-packages')
import inro.emme.database.emmebank as _eb

emmebank = _eb.Emmebank(r'E:\2015_TM2_20250619\emme_project\Database_highway\emmebank')
scenario = emmebank.scenario(1)
if scenario:
    print('Base scenario 1 exists')
    print(f'Title: {scenario.title}')
    modes = list(scenario.modes())
    print(f'Number of modes: {len(modes)}')
    print('Modes:')
    for mode in modes:
        print(f'  {mode.id}: {mode.type} - {mode.description}')
else:
    print('Scenario 1 not found')
