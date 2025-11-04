
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
    SCHLBUS = 17

    id_to_matrix_name = {
        1: 'sov_gp',
        2: 'sov_pay',
        3: 'sr2_gp',
        4: 'sr2_hov',
        5: 'sr2_pay',
        6: 'sr3_gp',
        7: 'sr3_hov',
        8: 'sr3_pay',
        9: 'walk',
        10: 'bike',
        11: 'wlk',
        12: 'pnr',
        13: 'knr',
        14: 'knr',
        15: 'taxi',
        16: 'tnc',
        17: 'schlbus'
    }