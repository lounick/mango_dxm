# ROS imports
import roslib; roslib.load_manifest("vehicle_core")
import rospy
from nav_msgs.msg import Odometry
from auv_msgs.msg import NavSts

# other imports
import sys
import zmq

IPs = {"sauv1": {"ip": "tcp://137.195.182.62", "port": 5556, "filter": "10001"},
       "iauv1": {"ip": "tcp://localhost", "port": 5546, "filter": "10001"}}

class TCPSubscriber(object):
    def __init__(self):
        # Socket to talk to server
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)
        self.ros_publisher_objects = {}
        for key, value in IPs.iteritems():
            self.socket.connect("{0}:{1}".format(value["ip"], value["port"]))
            self.socket.setsockopt(zmq.SUBSCRIBE, value["filter"])

            self.ros_publisher_objects[key] = rospy.Publisher("/{0}/odometry".format(key), Odometry, queue_size=1)

    def loop(self):
        try:
            string = self.socket.recv(zmq.NOBLOCK)

            string = string.split("\t")
            print(string)
            odomMsg = Odometry()
            vehicle_id = string[1]
            odomMsg.pose.pose.position.x = float(string[2])
            odomMsg.pose.pose.position.y = float(string[3])
            odomMsg.pose.pose.position.z = float(string[4])
            odomMsg.pose.pose.orientation.x = float(string[5])
            odomMsg.pose.pose.orientation.y = float(string[6])
            odomMsg.pose.pose.orientation.z = float(string[7])
            odomMsg.pose.pose.orientation.w = float(string[8])
            odomMsg.header.frame_id = string[9]
            odomMsg.child_frame_id = string[10]

            # ToDo: publish vehicle_name/base_link transform frames for namespaced odoms?

            # Publish the multiple Odometry topics on the same ros master
            self.ros_publisher_objects[vehicle_id].publish(odomMsg)
        except zmq.error.Again:
            pass


if __name__ == '__main__':
    if ("-h" in sys.argv or  "--help" in sys.argv):
        print("USAGE: python tcp_nav_sub.py [port number] [vehicle id]")
        exit(0)

    rospy.init_node("nav_tcp_broadcaster")
    rate = rospy.Rate(5)

    tcp_sub = TCPSubscriber()

    while not rospy.is_shutdown():
        tcp_sub.loop()
        rate.sleep()