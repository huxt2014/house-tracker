
class HouseTrackerError(Exception): pass

class ConfigError(HouseTrackerError): pass

class DownloadError(HouseTrackerError): pass

class ParseError(HouseTrackerError): pass