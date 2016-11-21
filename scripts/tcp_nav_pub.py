
# ROS imports
import roslib; roslib.load_manifest("vehicle_core")
import rospy
from nav_msgs.msg import Odometry
from auv_msgs.msg import NavSts

# other imports
import zmq
import random
import sys
import time

class TCPBroadcaster(object):
    def __init__(self, port, vehicle_id):
        context = zmq.Context()
        self.id = vehicle_id
        self.socket = context.socket(zmq.PUB)
        self.socket.bind("tcp://*:%s" % port)
        odom_sub = rospy.Subscriber("/nav/odometry", Odometry, self.odometryCallback)
        self.odomData = Odometry()
    def odometryCallback(self, msg):
        self.odomData = msg
    def loop(self):
        zmq_filter = 10001#random.randrange(9999,10005)
        # messagedata = random.randrange(1,215) - 80
        # print "%d %d" % (topic, messagedata)
        # socket.send("%d %d" % (topic, messagedata))
        messageData = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}".format(zmq_filter, self.id,
                                                                           self.odomData.pose.pose.position.x,
                                                                           self.odomData.pose.pose.position.y,
                                                                           self.odomData.pose.pose.position.z,
                                                                           self.odomData.pose.pose.orientation.x,
                                                                           self.odomData.pose.pose.orientation.y,
                                                                           self.odomData.pose.pose.orientation.z,
                                                                           self.odomData.pose.pose.orientation.w,
                                                                            self.odomData.header.frame_id,
                                                                            self.odomData.child_frame_id)
        self.socket.send(messageData, zmq.NOBLOCK)
        print("forwarding odom of vehicle: {0}".format(self.id))

if __name__ == '__main__':
    if len(sys.argv) == 1 or ("-h" in sys.argv or  "--help" in sys.argv):
        print("USAGE: python tcp_nav_pub.py [port number] [vehicle id]")
        exit(0)

    rospy.init_node("nav_tcp_broadcaster")
    rate = rospy.Rate(2)

    port =  int(sys.argv[1])
    vehicle_id = sys.argv[2]

    tcp_broadcast = TCPBroadcaster(port, vehicle_id)
    while not rospy.is_shutdown():
        tcp_broadcast.loop()
        rate.sleep()
