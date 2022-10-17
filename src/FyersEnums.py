# -*- coding: utf-8 -*-
from enum import Enum
class OrderStatus(Enum):
    Canceled = 1
    Filled = 2
    Transit = 4
    Rejected = 5
    Pending = 6
    Expired = 7
    
 
class OrderType(Enum):
    Market = 2
    Limit = 1
    Stop = 3
    StopLimit = 4
    