"""
接口ifTable表解析器

"""
import logging
from function_snmp.oids.base_parsers import CommonIndexParser

logger = logging.getLogger(__name__)

class ifDescrParser(CommonIndexParser):
    """
    描述接口的字符串,一般为接口名称
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.2", default_ttl, bulk_size)  # ifDescr


class ifMtuParser(CommonIndexParser):
    """
    接口mtu最大传输单元。接口上可以传送的最大报文的大小，单位是octet。
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.4", default_ttl, bulk_size)  # ifMtu


class ifSpeedParser(CommonIndexParser):
    """
    接口该项为额定带宽值
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.5", default_ttl, bulk_size)  # ifSpeed


class ifPhysAddressParser(CommonIndexParser):
    """
    接口的协议子层对应的接口地址
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.6", default_ttl, bulk_size)  # ifPhysAddress


class ifAdminStatusParser(CommonIndexParser):
    """
    接口的管理状态
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.7", default_ttl, bulk_size)  # ifAdminStatus

class ifOperStatusParser(CommonIndexParser):
    """
    接口的管理状态
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.8", default_ttl, bulk_size)  # ifOperStatus


class ifInDiscardsParser(CommonIndexParser):
    """
    接口入向丢包
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.13", default_ttl, bulk_size)  #ifInDiscards

class ifInErrorsParser(CommonIndexParser):
    """
    接口入向错包
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.14", default_ttl, bulk_size)  #ifInErrors


class ifOutDiscardsParser(CommonIndexParser):
    """
    接口出向丢包
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.19", default_ttl, bulk_size)  #ifOutDiscards

class ifOutErrorsParser(CommonIndexParser):
    """
    接口出向错包
    """
    def __init__(self, default_ttl: int = 300, bulk_size: int = 10):
        super().__init__("1.3.6.1.2.1.2.2.1.20", default_ttl, bulk_size)  #ifOutErrors



