#!/usr/bin/env python
import sys
import struct
import time
import binascii
import rospy
import std_msgs.msg
from sunset_ros_networking_msgs.msg import SunsetTransmission, SunsetNotification, SunsetReception

notification_received = True

def notification_cb(msg):
    global notification_received
    if msg.notification_type == 1:
        if msg.notification_subtype == 1:
            rospy.logwarn("Starting transmission")
            notification_received = False
        elif msg.notification_subtype == 2:
            rospy.logwarn("Transmission successful")
            notification_received = True
        elif msg.notification_subtype == 3:
            rospy.logerr("Acoustic transmission error")
            notification_received = True
    elif msg.notification_type == 2:
        if msg.notification_subtype == 1:
            rospy.logerr("Message address is wrong")
        elif msg.notification_subtype == 2:
            rospy.logerr("Message is too long")

def rec_cb(msg):
    pass

def main(argv):
    id = argv[0]
    if (len(argv) > 1):
        port = argv[1]
    else:
        port = str(9000+int(id))

    rospy.init_node("sunset_talker_" + id)
    msg_pub = rospy.Publisher('sunset_networking/sunset_transmit_' + id, SunsetTransmission, queue_size=1000)
    rospy.Subscriber('sunset_networking/sunset_notification_' + id, SunsetNotification, notification_cb)
    rospy.Subscriber('sunset_networking/sunset_reception_' + id, SunsetReception, rec_cb)
    rate = rospy.Rate(10) #0.1Hz
    time.sleep(5) # IF THIS IS NOT PRESENT THINGS GET OUT OF SYNCH
    global notification_received
    while not rospy.is_shutdown():
        if notification_received:
            rospy.logwarn("Sending message")
            msg = SunsetTransmission()
            msg.header = std_msgs.msg.Header()
            # msg.header.stamp = rospy.Time.now()
            msg.node_address = 2
            # payload = struct.pack('ccc', '0', '0', '3') WORKS
            # payload = struct.pack('ddd', 3,0,0) FAILS
            # payload = struct.pack('B', 3) WORKS
            # payload = struct.pack('d', 189.345) WORKS
            # payload = struct.pack('BBBBBBBB',215, 163, 112, 61, 10, 171, 103, 64) WORKS
            # payload = struct.pack('BBBBBBBB', 254, 9, 9, 9, 0, 6, 6, 6) FAILS
            # payload = struct.pack('BQdddBBL',1,100,53,5,5,1,1,50) FAILS
            print type(payload)
            print type(msg.payload)
            msg.payload = payload
            notification_received = False
            print binascii.hexlify(msg.payload)
            msg_pub.publish(msg)
        rate.sleep()


if __name__ == '__main__':
    if(len(sys.argv) < 3):
        print "Usage: test_talker id [port]"
    else:
        main(sys.argv[1:])
