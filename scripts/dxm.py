#!/usr/bin/env python

import rethinkdb as r
import sys
import struct
import roslib
roslib.load_manifest('mango_dxm')
import rospy
from std_msgs.msg import String
from sunset_ros_networking_msgs.msg import SunsetTransmission, SunsetReception, SunsetNotification


class vehicle_info:
    type_id_ = 2
    id_shift_ = 5
    status_shift_ = 3
    id_mask_ = 224
    status_mask_ = 24
    intention_mask_ = 7
    pack_fmt_ = 'BdddBL'
    def __init__(self, id, lat, lon, depth, intention, status, timestamp):
        self.id_ = id
        self.lat_ = lat
        self.lon_ = lon
        self.depth_ = depth
        self.intention_ = intention
        self.status_ = status
        self.timestamp_ = timestamp

    def pack(self):
        vinfo = 0
        vinfo = vinfo | (self.id_ << self.id_shift_)
        vinfo = vinfo | (self.status_ << self.status_shift_)
        vinfo = vinfo | self.intention_
        return struct.pack(self.pack_fmt_, self.type_id_, self.lat_, self.lon_, self.depth_, vinfo, self.timestamp_)



class target_info:
    type_id_ = 1
    def __init__(self, id, lat, lon, depth, vehicle_id, classification, timestamp):
        self.id_ = id
        self.lat_ = lat
        self.lon_ = lon
        self.depth_ = depth
        self.vehicle_id_ = vehicle_id
        self.classification_ = classification
        self.timestamp_ = timestamp


class Dxm:
    def __init__(self, module_id, port):
        self.msg_flag_ = False
        self.module_id_ = module_id
        self.msg_pub_ = rospy.Publisher('sunset_networking/sunset_transmit_'+self.module_id_, SunsetTransmission, queue_size=10)
        self.msg_rec_sub_ = rospy.Subscriber('sunset_networking/sunset_reception_'+self.module_id_, SunsetReception, self.received_cb)
        self.notification_sub_ = rospy.Subscriber('sunset_networking/sunset_notification_'+self.module_id_, SunsetNotification, self.notification_cb)
        self.rate_ = rospy.Rate(10)  # 10hz

    def received_cb(self, msg):
        # You received a message, get the payload, decode and decide what to do
        # Send an ack
        pass

    def notification_cb(self, msg):
        # Got a notiffication from sunset. Deal with that
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
