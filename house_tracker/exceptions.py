
class HouseTrackerError(Exception): pass

class JobError(HouseTrackerError): pass

class BatchJobError(JobError): pass

class DownloadError(HouseTrackerError): pass

class ParseError(HouseTrackerError): pass

class ConfigError(HouseTrackerError): pass