
import sys
import importlib

if __name__ == '__main__':
    runner = importlib.import_module('.'+sys.argv[1],
                                     'house_tracker.commands')
    runner.run()