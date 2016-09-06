#!/usr/bin/env python

import rethinkdb as r
from tornado import ioloop, gen
from tornado.locks import Lock
from tornado.concurrent import Future, chain_future
import functools
import time
import random
# from threading import Lock

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


class Dxm:
    def __init__(self, module_id, port):
        self.msg_flag_ = False
        self.avail_nodes_ = []
        self.msg_count_ = 0
        self.module_id_ = module_id
        self.msg_pub_ = rospy.Publisher('sunset_networking/sunset_transmit_'+self.module_id_, SunsetTransmission, queue_size=10)
        self.msg_rec_sub_ = rospy.Subscriber('sunset_networking/sunset_reception_'+self.module_id_, SunsetReception, self.received_cb)
        self.notification_sub_ = rospy.Subscriber('sunset_networking/sunset_notification_'+self.module_id_, SunsetNotification, self.notification_cb)
        self.db_ready_srv_ = rospy.Service('db_ready', DBReady, self.handle_dbready)

        self.rate_ = rospy.Rate(10)  # 10hz
        # Initialise the database by connecting and dropping any existing tables

        # r.set_loop_type("tornado")
        # self.connection = r.connect(host='localhost', port=28015)
        self.connection_ = None
        self.last_inserted_id_ = None
        self.last_timestamp_ = None
        self.db_ready_ = False

        self.candidates_ = {}
        self.transmitted_ = {}
        self.finished_ = {}

        self.mutex_ = Lock()

    def init_db(self):
        databases = r.db_list().run(self.connection_)
        if "sunset" not in databases:
            r.db_create("sunset").run(self.connection_)
            self.connection_.use("sunset")
            r.table_create("vehicles").run(self.connection_)
            r.table_create("targets").run(self.connection_)
        else:
            self.connection_.use("sunset")
            tables = r.table_list().run(self.connection_)
            for t in tables:
                r.table_drop(t).run(self.connection_)
            for t in tables:
                r.table_create(t).run(self.connection_)
        self.db_ready_ = True

    def handle_dbready(self, req):
        return DBReadyResponse(self.db_ready_)

    def received_cb(self, msg):
        # You received a message, get the payload, decode and decide what to do
        # Send an ack
        msg_pack_fmt_ = 'QdddBBL'
        msg_id = struct.unpack('L', msg.payload[0:4])
        type_id = struct.unpack('B', msg.payload[4])
        if type_id == 1:
            (uid, lat, lon, depth, info1, info2, timestamp) = struct.unpack(msg_pack_fmt_, msg.payload[5:])
            vehicle = VehicleInfo(uid, lat, lon, depth, info1, info2, timestamp)
            self.insert_db("vehicles", vehicle)
            if msg.node_address not in self.avail_nodes_:
                self.avail_nodes_.append(msg.node_address)
            self.gen_and_send_ack(msg_id, msg.node_address)
        elif type_id == 2:
            (uid, lat, lon, depth, info1, info2, timestamp) = struct.unpack(msg_pack_fmt_, msg.payload[5:])
            target = TargetInfo(uid, lat, lon, depth, info1, info2, timestamp)
            if msg.node_address not in self.avail_nodes_:
                self.avail_nodes_.append(msg.node_address)
            self.insert_db("targets", target)
            self.gen_and_send_ack(msg_id, msg.node_address)
        elif type_id == 3:
            # It is an ack for a message we sent. Add this to acked messages.
            acked_msg_id = struct.unpack('L', msg.payload[5:])
            if acked_msg_id in self.transmitted_:
                self.transmitted_[acked_msg_id]['acked'].append(msg.node_address)
                if msg.node_address not in self.avail_nodes_:
                    self.avail_nodes_.append(msg.node_address)
                if set(self.transmitted_[acked_msg_id]['sent']).issubset(set(self.transmitted_[acked_msg_id]['acked'])):
                    # Message was succsessfully received. Process that.
                    self.finished_[acked_msg_id] = self.transmitted_[acked_msg_id]
                    del self.transmitted_[acked_msg_id]
                # else:
                #     # Check for message TTL
                #     self.check_for_msg_ttl(acked_msg_id)
            else:
                # Got an ack for a message that is no longer valid. Too bad.
                rospy.loginfo("Got an ack for a non valid message.")

    def notification_cb(self, msg):
        # Got a notification from sunset. Deal with that
        if msg.notification_type == 1:
            if msg.notification_subtype == 1:
                rospy.loginfo("Transmission started")
            elif msg.notification_subtype == 2:
                rospy.loginfo("Transmission finished")
                self.msg_flag_ = False
            elif msg.notification_subtype == 3:
                rospy.loginfo("Transmission error")
                self.msg_flag_ = False
                # TODO: If you get an error here you shouldn't even wait for acks
            else:
                rospy.logerr("GOT WRONG NOTIFICATION SUBTYPE")
        elif msg.notification_type == 2:
            if msg.notification_subtype == 1:
                rospy.logwarn("Wrong destination address (id)")
            elif msg.notification_subtype == 2:
                rospy.logwarn("Wrong payload length")
            else:
                rospy.logerr("GOT WRONG NOTIFICATION SUBTYPE")
            self.msg_flag_ = False
        else:
            rospy.logerr("GOT WRONG NOTIFICATION TYPE SETTING FLAG TO ALLOW MESSAGES")
            self.msg_flag_ = False

    def gen_uid(self):
        pass

    def check_for_msg_ttl(self):
        for k, v in self.transmitted_.iteritems():
            if v['TTL'] >= rospy.Time.now():
                # Message expired.
                # Check if we should insert it again into the queue
                if v['id'] not in self.candidates_:
                    rospy.loginfo("Message failed to transmit. Rescheduling transmission.")
                    self.candidates_[v['id']] = v['priority']
                else:
                    rospy.loginfo("Message failed to transmit. Has obsolate info. Discarding.")
                del self.transmitted_[k]

    def gen_and_send_ack(self, msg_id, address):
        self.mutex_.acquire()
        payload = struct.pack('L', self.msg_count_)
        payload += struct.pack('B', 3)
        payload += struct.pack('L', msg_id)
        msg = SunsetTransmission()
        msg.payload = payload
        msg.node_address = address
        msg.header.stamp = rospy.Time.now()
        self.msg_pub_.publish(msg)
        self.msg_flag_ = True
        self.msg_count_ += 1
        self.mutex_.release()

    def insert_db(self, table, item):
        tmp_id = self.last_inserted_id_
        tmp_ts = self.last_timestamp_
        self.last_inserted_id_ = item.id
        self.last_timestamp_ = item.timestamp
        if r.table(table).get(item.id).run(self.connection_) is None:
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

    def get_db(self, table, uid):
        item = None
        record = r.table(table).get(uid).run(self.connection_)
        if table == 'vehicles':
            item = VehicleInfo(record['id'], record['lat'], record['lon'], record['depth'], record['intention'], record['status'], record['timestamp'])
        elif table == 'targets':
            item = TargetInfo(record['id'], record['lat'], record['lon'], record['depth'], record['vehicle_id'], record['classification'], record['timestamp'])
        return item


    @gen.coroutine
    def rethink_vehicle_cb(self, connection_future, table, feeds_ready):
        # We have a vehicle update. Queue it for transmission
        connection = yield connection_future
        connection.use("sunset")
        feed = yield r.table(table).changes().run(connection)
        feeds_ready[table].set_result(True)
        while (yield feed.fetch_next()):
            item = yield feed.next()
            if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                self.candidates_[item['new_val']['id']] = 1

    @gen.coroutine
    def rethink_target_cb(self,connection_future, table, feeds_ready):
        # We have a target update. Queue it for transmission
        connection = yield connection_future
        connection.use("sunset")
        feed = yield r.table(table).changes().run(connection)
        feeds_ready[table].set_result(True)
        while (yield feed.fetch_next()):
            item = yield feed.next()
            if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                self.candidates_[item['new_val']['id']] = 2

    @gen.coroutine
    def run(self):
        self.connection_ = yield connection
        self.init_db()
        feeds_ready = {'vehicle':Future(), 'target':Future()}
        ioloop.IOLoop.current().add_callback(self.rethink_vehicle_cb, connection, 'vehicle', feeds_ready)
        ioloop.IOLoop.current().add_callback(self.rethink_target_cb, connection, 'target', feeds_ready)
        yield feeds_ready
        next_transmission = rospy.Time.now().secs + 10 + random.randint(-3,3)
        while not rospy.is_shutdown():
            self.check_for_msg_ttl()
            if rospy.Time.now().secs >= next_transmission:
                # It is time to transmit.
                if not self.msg_flag_:
                    # If you haven't already transmited and waiting for notification from sunset
                    # Choose the candidate with the highest priority.
                    # Make a message
                    # Transmit it
                    # Claculate new transmission time
                    candidate_v = 0
                    candidate_key = 0
                    for k in self.candidates_.iterkeys():
                        v = self.candidates_[k]
                        if v >= candidate_v:
                            candidate_v = v
                            candidate_key = k
                    self.mutex_.acquire()
                    payload = struct.pack('L', self.msg_count_)
                    if candidate_v == 1:
                        candidate = self.get_db('vehicles', candidate_key)
                    elif candidate_v == 2:
                        candidate = self.get_db('targets', candidate_key)
                    payload += candidate.pack()

                    msg = SunsetTransmission()
                    msg.header = rospy.Time.now()
                    msg.node_address = 0
                    msg.payload = payload
                    del self.candidates_[candidate_key]
                    self.transmitted_[self.msg_count_] = {'TTL':rospy.Time.now().secs+180, 'id':candidate_key}
                    self.msg_flag_ = True
                    self.msg_pub_.publish(msg)
                    self.msg_count_ += 1
                    next_transmission = rospy.Time.now().secs + 10 + random.randint(-3,3)
                    self.mutex_.release()
            self.rate_.sleep()


def main(argv):
    module_id = argv[0]
    if len(argv) > 1:
        port = argv[1]
    else:
        port = 9000 + int(module_id)
    rospy.init_node('dxm', anonymous=True)
    d = Dxm(module_id, port)
    try:
        ioloop.IOLoop.current().run_sync(d.run)
        ioloop.IOLoop.current().start()
        # d.run()
    except rospy.ROSInterruptException:
        pass

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: dxm moduleId (port)"
    else:
        main(sys.argv[1:])
