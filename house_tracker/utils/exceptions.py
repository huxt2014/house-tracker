
class HouseTrackerError(Exception): pass

class DownloadError(HouseTrackerError): pass

class ParseError(HouseTrackerError): pass

class ConfigError(HouseTrackerError): pass