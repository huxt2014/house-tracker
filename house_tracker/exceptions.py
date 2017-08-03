
class HouseTrackerError(Exception):
    pass


class ModelError(Exception):
    pass


class JobError(ModelError):
    pass


class BatchJobError(ModelError):
    pass


class DownloadError(ModelError):
    pass


class ParseError(ModelError):
    pass


class ConfigError(HouseTrackerError):
    pass
