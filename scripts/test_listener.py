#!/usr/bin/env python
import sys
import struct
import rospy
import std_msgs.msg
from sunset_ros_networking_msgs.msg import SunsetReception


def reception_cb(msg):
    # payload = struct.unpack('BBB', msg.payload)
    payload = struct.unpack('ccc', msg.payload)
    print "I heard " + ','.join(str(i) for i in payload) + " from " + str(msg.node_address)


def main(argv):
    id = argv[0]
    if(len(argv) > 1):
        port = argv[1]
    else:
        port = str(9000 + int(id))

    rospy.init_node("listener_" + id)
    rospy.Subscriber('sunset_networking/sunset_reception_' + id, SunsetReception, reception_cb)

    rospy.spin()

if __name__ == '__main__':
    if (len(sys.argv) < 3):
        print "Usage: test_listener id [port]"
    else:
        main(sys.argv[1:])
