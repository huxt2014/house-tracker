
import sys
import importlib

if __name__ == '__main__':
    try:
        runner = importlib.import_module('.'+sys.argv[1],
                                         'house_tracker.commands')
        runner.run()
    except ImportError:
        print 'command not exist: %s' % sys.argv[1]