#!/usr/bin/env python

import rethinkdb as r
import struct
import sys

connection = r.connect()


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

def insert_db(table, item):
    connection.use('sunset')
    if (r.table(table).get(item.id).run(connection)) is None:
        # This vehicle doesn't exist. Insert into DB.
        res = r.table(table).insert(item.__dict__).run(connection)
    else:
        # Vehicle exists in DB. Just update its info.
        res = r.table(table).update(item.__dict__).run(connection)


def main(argv):
    connection.use('sunset')
    uid = int(argv[0])
    v = VehicleInfo(uid, 0, 0, 0, 0, 0, 0)
    insert_db('vehicles', v)


if __name__ == '__main__':
    main(sys.argv[1:])