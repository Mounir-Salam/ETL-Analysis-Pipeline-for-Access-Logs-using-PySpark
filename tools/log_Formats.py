class logFormats:

    @staticmethod
    def get_format(log_format):
        """
        Get the log format based on the specified type.
        """
        if log_format == "combined":
            return logFormats.__Combined_log_format()
        elif log_format == "common":
            return logFormats.__Common_log_format()
        else:
            raise ValueError("Unsupported log format: {}".format(log_format))
    
    @staticmethod
    def __Common_log_format():
        
        return r'^([\d\.]+) - - \[([^\]]+)\] "(\w+) ([^"]+) ([^"]+)" (\d+) (\d+)$'

    @staticmethod
    def __Combined_log_format():

        return r'^([\d\.]+) - - \[([^\]]+)\] "(\w+) ([^"]+) ([^"]+)" (\d+) (\d+) "([^"]*)" "([^"]*)"$'