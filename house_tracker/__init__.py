
import sys
import importlib

version = '1.1.0'


def run():
    sys.argv = sys.argv[1:]
    module = importlib.import_module('.'+sys.argv[0],
                                     'house_tracker.commands')
    module.run()