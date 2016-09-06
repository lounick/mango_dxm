#!/usr/env/python

import rethinkdb as r
from tornado import ioloop, gen
from tornado.locks import Lock
from tornado.concurrent import Future, chain_future
import functools
import time
import random


r.set_loop_type("tornado")
connection = r.connect(host='localhost', port=28015)

import sys
import struct
import roslib
roslib.load_manifest('mango_dxm')
import rospy
from sunset_ros_networking_msgs.msg import SunsetTransmission, SunsetReception, SunsetNotification
from mango_dxm.srv import *


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


class TargetInfo:
    type_id_ = 2
    vehicle_shift_ = 4
    vehicle_mask_ = 240
    classification_mask_ = 15
    pack_fmt_ = 'BQdddBBL'

    def __init__(self, uid, lat, lon, depth, vehicle_id, classification, timestamp):
        self.id = uid
        self.lat = lat
        self.lon = lon
        self.depth = depth
        self.vehicle_id = vehicle_id
        self.classification = classification
        self.timestamp = timestamp

    def pack(self):
        # tinfo = 0
        # tinfo |= self.vehicle_id_ << self.vehicle_shift_
        # tinfo |= self.classification_
        return struct.pack(self.pack_fmt_, self.type_id_, self.id, self.lat, self.lon, self.depth, self.vehicle_id, self.classification, self.timestamp)

class sauv_exec:
    def __init__(self):
        pass

    @gen.coroutine
    def insert_db(self, table, item):
        with (yield self.mutex_.acquire()):
            tmp_id = self.last_inserted_id_
            tmp_ts = self.last_timestamp_
            self.last_inserted_id_ = item.id
            self.last_timestamp_ = item.timestamp
            if (yield r.table(table).get(item.id).run(self.connection_)) is None:
                # This vehicle doesn't exist. Insert into DB.
                res = r.table(table).insert(item.__dict__).run(self.connection_)
                if res['errors'] != 0:
                    self.last_inserted_id_ = tmp_id
                    self.last_timestamp_ = tmp_ts
                    rospy.logerr("Error inserting in table: " + table + ". With error: " + res['first_error'])
            else:
                # Vehicle exists in DB. Just update its info.
                res = r.table(table).update(item.__dict__).run(self.connection_)
                if res['errors'] != 0:
                    self.last_inserted_id_ = tmp_id
                    self.last_timestamp_ = tmp_ts
                    rospy.logerr("Error updating in table: " + table + ". With error: " + res['first_error'])
                    self.mutex_.release()

    @gen.coroutine
    def get_db(self, table, uid, retval):
        with (yield self.mutex_.acquire()):
            item = None
            record = yield r.table(table).get(uid).run(self.connection_)
            if table == 'vehicles':
                item = VehicleInfo(record['id'], record['lat'], record['lon'], record['depth'], record['intention'], record['status'], record['timestamp'])
            elif table == 'targets':
                item = TargetInfo(record['id'], record['lat'], record['lon'], record['depth'], record['vehicle_id'], record['classification'], record['timestamp'])
            self.mutex_.release()
            retval = item

    @gen.coroutine
    def rethink_vehicle_cb(self, connection_future, table, feeds_ready):
        # We have a vehicle update. Queue it for transmission
        connection = yield connection_future
        with (yield self.mutex_.acquire()):
            connection.use("sunset")
            feed = yield r.table(table).changes().run(connection)
            feeds_ready[table].set_result(True)
            self.mutex_.release()
        while (yield feed.fetch_next()):
            item = yield feed.next()
            if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                self.candidates_[item['new_val']['id']] = 1

    @gen.coroutine
    def rethink_target_cb(self,connection_future, table, feeds_ready):
        # We have a target update. Queue it for transmission
        connection = yield connection_future
        with (yield self.mutex_.acquire()):
            connection.use("sunset")
            feed = yield r.table(table).changes().run(connection)
            feeds_ready[table].set_result(True)
            self.mutex_.release()
        while (yield feed.fetch_next()):
            item = yield feed.next()
            if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                self.candidates_[item['new_val']['id']] = 2

    @gen.coroutine
    def run(self):
        pass
