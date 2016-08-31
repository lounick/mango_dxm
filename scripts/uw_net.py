#!/usr/bin/env python

import sys
import struct
import roslib
roslib.load_manifest('mango_dxm')
import rospy
from std_msgs.msg import String
from sunset_ros_networking_msgs.msg import SunsetTransmission, SunsetReception, SunsetNotification


class Uw_net:
    def __init__(self, num_nodes):
        self.num_nodes_ = int(num_nodes)
        self.rate_ = rospy.Rate(10)  # 10hz
        self.trans_sub_ = []
        self.rec_pub_ = []
        self.notif_pub_ = []
        for i in range(self.num_nodes_):
            self.trans_sub_.append(rospy.Subscriber('sunset_networking/sunset_transmit_'+str(i+1), SunsetTransmission, self.trans_cb, (i+1)))
            self.rec_pub_.append(rospy.Publisher('sunset_networking/sunset_reception_'+str(i+1), SunsetReception, queue_size=10))
            self.notif_pub_.append(rospy.Publisher('sunset_networking/sunset_notification_'+str(i+1), SunsetNotification, queue_size=10))
        rospy.sleep(1)

    def trans_cb(self, msg, args):
        trans_id = args
        payload = msg.payload
        delivery = msg.node_address
        rec = SunsetReception()
        rec.header.stamp = rospy.Time.now()
        rec.node_address = trans_id
        rec.payload = payload
        if delivery == 0:
            # Broadcast message publish to all
            for i in range(self.num_nodes_):
                if(i+1 != trans_id):
                    self.rec_pub_[i].publish(rec)
        else:
            self.rec_pub_[delivery-1].publish(rec)
        # Anyway you need a notification
        notif = SunsetNotification()
        notif.header.stamp = rospy.Time.now()
        notif.notification_type = 1
        notif.notification_subtype = 2
        self.notif_pub_[trans_id-1].publish(notif)

    def run(self):
        while not rospy.is_shutdown():
            # Do stuff as described
            self.rate_.sleep()

def main(argv):
    rospy.init_node('uw_net', anonymous=True)
    net = Uw_net(argv[0])
    try:
        net.run()
    except rospy.ROSInterruptException:
        pass

if __name__ == '__main__':
    if(len(sys.argv) < 2):
        print "Usage: uw_net num_nodes"
    else:
        main(sys.argv[1:])
