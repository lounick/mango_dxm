#!/usr/bin/env python

import rethinkdb as r
from tornado import ioloop, gen
from tornado.concurrent import Future, chain_future
import functools
import time

import sys
import struct
import roslib
roslib.load_manifest('mango_dxm')
import rospy
from std_msgs.msg import String
from sunset_ros_networking_msgs.msg import SunsetTransmission, SunsetReception, SunsetNotification


class vehicle_info:
    type_id_ = 1
    status_shift_ = 4
    status_mask_ = 240
    intention_mask_ = 15
    pack_fmt_ = 'BQdddBBL'
    def __init__(self, uid, lat, lon, depth, status, intention, timestamp):
        self.uid_ = uid
        self.lat_ = lat
        self.lon_ = lon
        self.depth_ = depth
        self.intention_ = intention
        self.status_ = status
        self.timestamp_ = timestamp

    def pack(self):
        # vinfo = 0
        # vinfo |= self.status_ << self.status_shift_
        # vinfo |= self.intention_
        return struct.pack(self.pack_fmt_, self.type_id_, self.uid_, self.lat_, self.lon_, self.depth_, self.status_, self.intention_, self.timestamp_)



class target_info:
    type_id_ = 2
    vehicle_shift_ = 4
    vehicle_mask_ = 240
    classification_mask_ = 15
    pack_fmt_ = 'BQdddBBL'
    def __init__(self, uid, lat, lon, depth, vehicle_id, classification, timestamp):
        self.uid_ = uid
        self.lat_ = lat
        self.lon_ = lon
        self.depth_ = depth
        self.vehicle_id_ = vehicle_id
        self.classification_ = classification
        self.timestamp_ = timestamp

    def pack(self):
        # tinfo = 0
        # tinfo |= self.vehicle_id_ << self.vehicle_shift_
        # tinfo |= self.classification_
        return struct.pack(self.pack_fmt_, self.type_id_, self.uid_, self.lat_, self.lon_, self.depth_, self.vehicle_id_, self.classification_, self.timestamp_)




class Dxm:
    def __init__(self, module_id, port):
        self.msg_flag_ = False
        self.module_id_ = module_id
        self.msg_pub_ = rospy.Publisher('sunset_networking/sunset_transmit_'+self.module_id_, SunsetTransmission, queue_size=10)
        self.msg_rec_sub_ = rospy.Subscriber('sunset_networking/sunset_reception_'+self.module_id_, SunsetReception, self.received_cb)
        self.notification_sub_ = rospy.Subscriber('sunset_networking/sunset_notification_'+self.module_id_, SunsetNotification, self.notification_cb)
        self.rate_ = rospy.Rate(10)  # 10hz
        # Initialise the database by connecting and dropping any existing tables

        r.set_loop_type("tornado")
        self.connection = r.connect(host='localhost', port=28015)

    def received_cb(self, msg):
        # You received a message, get the payload, decode and decide what to do
        # Send an ack
        pack_fmt_ = 'BQdddBBL'
        (type_id, uid, lat_, lon, depth, info1, info2, timestamp) = struct.unpack(pack_fmt_, msg.payload)
        if type_id == 1:
            vehicle = vehicle_info(type_id, uid, lat_, lon, depth, info1, info2, timestamp)
        elif type_id == 2:
            target = target_info(type_id, uid, lat_, lon, depth, info1, info2, timestamp)


    def notification_cb(self, msg):
        # Got a notiffication from sunset. Deal with that
        pass

    def gen_uid(self):
        pass

    def run(self):
        while not rospy.is_shutdown():
            # Do stuff as described
            self.rate_.sleep()


def main(argv):
    module_id = argv[0]
    if (len(argv) > 1):
        port = argv[1]
    else:
        port = 9000 + int(module_id)
    rospy.init_node('dxm', anonymous=True)
    d = Dxm(module_id, port)
    try:
        d.run()
    except rospy.ROSInterruptException:
        pass

if __name__ == "__main__":
    if(len(sys.argv) < 2):
        print "Usage: dxm moduleId (port)"
    else:
        main(sys.argv[1:])
