
import os
import importlib


def load_tests(loader, standard_tests, pattern):
    """Customize the order of the top level TestSuit.
    """
    # basic suit come first
    m = importlib.import_module(".basic", __name__)
    standard_tests.addTests(loader.loadTestsFromModule(m))

    # only test*.py file, not support package.
    this_dir = os.path.dirname(__file__)
    for fn in os.listdir(this_dir):
        if fn.startswith("test") and fn.endswith(".py"):
            m = importlib.import_module("."+fn[:-3], __name__)
            standard_tests.addTests(loader.loadTestsFromModule(m))
    return standard_tests
