
import sys
import importlib

if __name__ == '__main__':
    pkg = 'house_tracker.commands'
    try:
        runner = importlib.import_module('.'+sys.argv[1], pkg)
    except ImportError:
        runner = importlib.import_module('.alembic', pkg)
    
    runner.run()