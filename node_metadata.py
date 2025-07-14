from net import network_utils


class NodeMetadata:
    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
    
    def get_host(self) -> str:
        return self._host
    
    def get_port(self) -> int:
        return self._port
    
    def __eq__(self, value):
        if value is None:
            return False
        return network_utils.are_ipaddrs_equal(self._host, value.get_host()) and self._port == value.get_port()
    
    def __str__(self):
        ret = f"{self._host}, {self._port}"
        return ret