from __future__ import absolute_import

import alembic.config

from house_tracker.utils.db import get_database_url

class CommandLine(alembic.config.CommandLine):
    def main(self, argv=None):
        options = self.parser.parse_args(argv)
        if not hasattr(options, "cmd"):
            # see http://bugs.python.org/issue9253, argparse
            # behavior changed incompatibly in py3.3
            self.parser.error("too few arguments")
        else:
            alembic_cfg = alembic.config.Config()
            alembic_cfg.set_main_option("script_location", 
                                        "house_tracker:migrations")
            alembic_cfg.set_main_option("sqlalchemy.url", get_database_url())
            self.run_cmd(alembic_cfg, options)


def run(argv=None, prog=None, **kwargs):
    CommandLine(prog=prog).main(argv=argv)
    

    