import sys
sys.path.insert(0, r'C:\Program Files\Bentley\OpenPaths\EMME 24.01.00\Python311\Lib\site-packages')
from inro.emme.database import emmebank
from collections import Counter

eb = emmebank.Emmebank(r'M:\Development\Travel Model Two\Supply\Network Creation 2025\from_OSM\SanMateo\7_scenario\emme\emme_project\Database_highway\emmebank')
scenario = eb.scenario(1)
network = scenario.get_network()

print('#drive_access values:')
vals = Counter(link['#drive_access'] for link in network.links())
for v, cnt in vals.most_common():
    print(f'  {repr(v)}: {cnt}')

print('\n#bus_only values:')
vals = Counter(link['#bus_only'] for link in network.links())
for v, cnt in vals.most_common():
    print(f'  {repr(v)}: {cnt}')

print('\n#walk_access values:')
vals = Counter(link['#walk_access'] for link in network.links())
for v, cnt in vals.most_common():
    print(f'  {repr(v)}: {cnt}')

print('\n#rail_only values:')
vals = Counter(link['#rail_only'] for link in network.links())
for v, cnt in vals.most_common():
    print(f'  {repr(v)}: {cnt}')
