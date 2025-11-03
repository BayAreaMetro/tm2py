
from enum import Enum

class ModeChoice(Enum):
    DRIVEALONEFREE = 1
    DRIVEALONEPAY = 2
    SHARED2GP = 3
    SHARED2HOV = 4
    SHARED2PAY = 5
    SHARED3GP = 6
    SHARED3HOV = 7
    SHARED3PAY = 8
    WALK = 9
    BIKE = 10
    WALK_SET = 11
    PNR_SET =12 
    KNR_PERS =13
    KNR_TNC = 14
    TAXI = 15
    TNC = 16