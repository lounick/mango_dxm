#!/usr/bin/env python

import rethinkdb as r
import struct
import sys
from time import time

connection = r.connect()

class TargetInfo:
    type_id_ = 2
    vehicle_shift_ = 4
    vehicle_mask_ = 240
    classification_mask_ = 15
    pack_fmt_ = '!BQdddBBL'

    def __init__(self, uid, lat, lon, depth, vehicle_id, classification, timestamp):
        self.id = uid
        self.lat = lat
        self.lon = lon
        self.depth = depth
        self.vehicle_id = vehicle_id
        self.classification = classification
        self.timestamp = timestamp

    def pack(self):
        return struct.pack(self.pack_fmt_, self.type_id_, self.id, self.lat, self.lon, self.depth, self.vehicle_id, self.classification, self.timestamp)

class VehicleInfo:
    type_id_ = 1
    status_shift_ = 4
    status_mask_ = 240
    intention_mask_ = 15
    pack_fmt_ = 'BQdddBBL'

    def __init__(self, uid, lat, lon, depth, status, intention, timestamp):
        self.id = uid
        self.lat = lat
        self.lon = lon
        self.depth = depth
        self.intention = intention
        self.status = status
        self.timestamp = timestamp

    def pack(self):
        # vinfo = 0
        # vinfo |= self.status_ << self.status_shift_
        # vinfo |= self.intention_
        return struct.pack(self.pack_fmt_, self.type_id_, self.id, self.lat, self.lon, self.depth, self.status, self.intention, self.timestamp)

def insert_db(table, item, db_num):
    connection.use('sunset_'+str(db_num))
    if (r.table(table).get(item.id).run(connection)) is None:
        # This vehicle doesn't exist. Insert into DB.
        res = r.table(table).insert(item.__dict__).run(connection)
    else:
        # Vehicle exists in DB. Just update its info.
        res = r.table(table).update(item.__dict__).run(connection)


def main(argv):
    connection.use('sunset')

    if "-t" in argv:
        argv.remove("-t")
        print("USAGE: python test_db_insert.py -t [sunset_id] [uid] [lat] [lon] [classified]")
        db_num = int(argv[0])
        uid = int(argv[1])
        lat = int(argv[2])
        lon = int(argv[3])
        classified = int(argv[4])
        print("Target:\n\tsunset_id: {0}\n\tuid: {1}\n\tlat: {2}\n\tlon: {3}\n\tclassified: {4}".format(db_num, uid,
                                                                                               lat, lon, classified))
        t = TargetInfo(uid, lat, lon, 0, 0, classified, time())
        insert_db('targets', t, db_num)
        print("Inserted Target")
    elif "-v" in argv:
        # Insert a vehicle into the database
        argv.remove("-v")
        print("USAGE: python test_db_insert.py -v [sunset_id] [uid] [lat] [lon]")
        db_num = int(argv[0])
        uid = int(argv[1])
        lat = int(argv[2])
        lon = int(argv[3])
        print("Vehicle\n\tsunset_id: {0}\n\tuid: {1}\n\tlat: {2}\n\tlon: {3}".format(db_num, uid, lat, lon))
        v = VehicleInfo(uid, lat, lon, 0, 0, 0, time())
        insert_db('vehicles', v, db_num)
        print("Inserted Vehicle")
    else:
        print("USAGE:")
        print("\tpython test_db_insert.py -v [sunset_id] [uid] [lat] [lon]")
        print("\tpython test_db_insert.py -t [sunset_id] [uid] [lat] [lon] [classified]")
    print("")


if __name__ == '__main__':
    main(sys.argv[1:])
