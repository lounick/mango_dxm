#!/usr/bin/env python

import rethinkdb as r
import roslib
roslib.load_manifest('mango_dxm')
import rospy
from std_msgs.msg import String


class vehicle_info:
    def __init__(self, id, lat, lon, depth, intention, status, timestamp):
        self.id_ = id
        self.lat_ = lat
        self.lon_ = lon
        self.depth_ = depth
        self.intention_ = intention
        self.status_ = status
        self.timestamp_ = timestamp


class vehicle_info:
    def __init__(self, id, lat, lon, depth, vehicle_id, classification, timestamp):
        self.id_ = id
        self.lat_ = lat
        self.lon_ = lon
        self.depth_ = depth
        self.vehicle_id_ = vehicle_id
        self.classification_ = classification
        self.timestamp_ = timestamp


class Dxm:
    def __init__(self):
        self.msg_pub_ = rospy.Publisher('chatter', String, queue_size=10)
        self.rate_ = rospy.Rate(1)  # 1hz

    def received_cb(self):
        pass

    def notification_cb(self):
        pass

    def run(self):
        while not rospy.is_shutdown():
            hello_str = "hello world %s" % rospy.get_time()
            rospy.loginfo(hello_str)
            self.msg_pub_.publish(hello_str)
            self.rate_.sleep()


def main():
    rospy.init_node('dxm', anonymous=True)
    d = Dxm()
    try:
        d.run()
    except rospy.ROSInterruptException:
        pass

if __name__ == "__main__":
    main()
