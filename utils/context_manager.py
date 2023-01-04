# Custom context manager allows you to allocate and release resources precisely when you want to. 


class RandomLibraryWithClient():
    """ This is a sample of any library that require generating a client to perform actions based on, samples: SparkSession, BigQuery Client, etc.
    """
    def __init__(self):
        self.client_name = "client"
        self.conection_port = "8080"


class ContextManager():
    """ Tracking the on-going clients and sessions in the application 
    """
    def __init__(self) -> None:
        self._random_lib_context = []
    
    def get_random_lib_context(self):
        if len(self._random_lib_context) == 0:
            # create a new context
            client = RandomLibraryWithClient()
            self._random_lib_context.append(client)
        return self._random_lib_context[-1]