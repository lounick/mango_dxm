#!/usr/bin/env python
import sys
# sys.path.append("/home/gordon/ros_workspace/vehicle_interface/src/vehicle_interface")

import roslib; roslib.load_manifest('vehicle_core')
import rospy
import threading
import zmq
import rethinkdb as r
import functools
import time, sched
import tf
import random
import struct

# __init__.py must be in the same directory as this script, also any module that is calling it unless
# this is put on PYTHONPATH environment variable
from lawnmower_generator import *
from vehicle_interface.msg import PilotRequest, Vector6
from nav_msgs.msg import Odometry
from auv_msgs.msg import NavSts
from visualization_msgs.msg import Marker
from visualization_msgs.msg import MarkerArray
from mango_dxm.srv import *

class VehicleInfo:
    type_id_ = 1
    status_shift_ = 4
    status_mask_ = 240
    intention_mask_ = 15
    pack_fmt_ = '!BQdddBBL'

    def __init__(self, uid, lat, lon, depth, status, intention, timestamp):
        self.id = uid
        self.lat = lat
        self.lon = lon
        self.depth = depth
        self.intention = intention
        self.status = status
        self.timestamp = timestamp

    def pack(self):
        return struct.pack(self.pack_fmt_, self.type_id_, self.id, self.lat, self.lon, self.depth, self.status, self.intention, self.timestamp)

class TargetInfo:
    type_id_ = 2
    vehicle_shift_ = 4
    vehicle_mask_ = 240
    classification_mask_ = 15
    pack_fmt_ = '!BQdddBBL'

    def __init__(self, uid, lat, lon, depth, vehicle_id, classification, timestamp):
        self.id = uid
        self.lat = lat
        self.lon = lon
        self.depth = depth
        self.vehicle_id = vehicle_id
        self.classification = classification
        self.timestamp = timestamp

    def pack(self):
        return struct.pack(self.pack_fmt_, self.type_id_, self.id, self.lat, self.lon, self.depth, self.vehicle_id, self.classification, self.timestamp)

TEXT_SIZE = 1.5
IPs = {0: "sauv",
       1: "iauv",
       2: "base_link"}

class RViz_Rethink_MultiVehicle_Visualisation(object):
    def __init__(self, module_id=0, test_executor=False):
        self.module_id_ = int(module_id)
        self.db_name_ = "sunset_"+module_id
        self.db_ready_srv_ = rospy.Service('db_ready', DBReady, self.handle_dbready)
        self.db_server_ready = rospy.ServiceProxy('db_ready', DBReady)
        self.target_markers_pub = rospy.Publisher("/target_markers", MarkerArray, queue_size=1)
        self.text_markers_pub = rospy.Publisher("/label_markers", MarkerArray, queue_size=1)
        self.db_ready_ = False
        self.connection_ = r.connect()
        self.init_db()
        self._nav = None
        self.start_time = time.time()

        # self.connection_ = None
        self.run_threads_ = True
        self.last_inserted_id_ = None
        self.last_timestamp_ = None
        self.db_ready_ = False
        self.targets = {}
        self.target_ids = []
        self.scheduler_ = sched.scheduler(time.time, time.sleep)

        self.vehicle_positions = {}
        self.my_targets = []
        self.my_targets_ids = []
        
        self.nav_count_ = 0

        # Socket to talk to server
        context = zmq.Context()
        # self.socket = context.socket(zmq.SUB)
        self.ros_publisher_objects = {}
        for key, value in IPs.iteritems():
            # self.socket.connect("{0}:{1}".format(value["ip"], value["port"]))
            # self.socket.setsockopt(zmq.SUBSCRIBE, value["filter"])

            self.ros_publisher_objects[key] = rospy.Publisher("/{0}/odometry".format(key), Odometry, queue_size=1)

    def init_db(self):
        print("INIT.......")
        databases = r.db_list().run(self.connection_)
        if self.db_name_ not in databases:
            rospy.loginfo("Sunset db was not found. Creating.")
            r.db_create(self.db_name_).run(self.connection_)
            rospy.loginfo("Database created")
            self.connection_.use(self.db_name_)
            rospy.loginfo("Creating tables")
            r.table_create("vehicles").run(self.connection_)
            rospy.loginfo(r.table_list().run(self.connection_))
            rospy.loginfo("Table Vehicles created")
            r.table_create("targets").run(self.connection_)
            rospy.loginfo(r.table_list().run(self.connection_))
            rospy.loginfo("Table targets created")
        else:
            rospy.loginfo("Sunset db was found. Droping tables")
            self.connection_.use(self.db_name_)
            tables = r.table_list().run(self.connection_)
            rospy.loginfo(tables)
            for t in tables:
                r.table_drop(t).run(self.connection_)
            for t in tables:
                r.table_create(t).run(self.connection_)
            if 'vehicles' not in tables:
                r.table_create("vehicles").run(self.connection_)
            if 'targets' not in tables:
                r.table_create("targets").run(self.connection_)
        rospy.loginfo("Database initialised")
        self.db_ready_ = True

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

            # Publish the multiple Odometry topics on the same ros master
            self.ros_publisher_objects[vehicle_id].publish(odomMsg)
        except zmq.error.Again:
            pass

    def handle_dbready(self, req):
        return DBReadyResponse(self.db_ready_)

    # def init_db(self):
    #     while not self.db_server_ready():
    #         rospy.sleep(0.1)

    def update_nav(self):
        print(type(self.module_id_))
        print(type(int(self.module_id_)))
        v = VehicleInfo(int(self.module_id_), self._nav.position.north, self._nav.position.east, self._nav.position.depth, 0, 0, rospy.Time.now().secs)
        r.table("vehicles").get(self.module_id_).update(v.__dict__).run(self.connection_)
        #self.scheduler_.enter(30, 1, self.update_nav, ())
        #threading.Timer(30, self.update_nav, ()).start()

    def rethink_vehicle_cb(self):
        # We have a vehicle update. Queue it for transmission
        c = r.connect()
        print("Vehicle cb: Using sunset database")
        c.use(self.db_name_)
        feed = r.table('vehicles').changes().run(c)
        while self.run_threads_:
            try:
                item = feed.next(wait=False)
                print(item)
                # Process item
                if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                    self.vehicle_positions[item['new_val']['id']] = [item['new_val']['lat'], item['new_val']['lon'], item['new_val']['depth']]
            except r.ReqlTimeoutError:
                time.sleep(0.01)  # Sleep thread

    def rethink_target_cb(self):
        # We have a target update. Queue it for transmission
        c = r.connect()
        print("Target cb: Using sunset database")
        c.use(self.db_name_)
        feed = r.table('targets').changes().run(c)
        while self.run_threads_:
            try:
                item = feed.next(wait=False)
                print(item)
                # Process item
                if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                    self.targets[item['new_val']['id']] = [item['new_val']['lat'], item['new_val']['lon'], item['new_val']['depth'],
                                         item['new_val']['vehicle_id'], item['new_val']['classification'], item['new_val']['timestamp']]
                    self.target_ids.append(item['new_val']['id'])


                    self.my_targets.append([item['new_val']['lat'], item['new_val']['lon'], item['new_val']['depth'],
                                     item['new_val']['vehicle_id'], item['new_val']['classification'], item['new_val']['timestamp']])
                    self.my_targets_ids.append(item['new_val']['id'])
            except r.ReqlTimeoutError:
                time.sleep(0.01)  # Sleep thread for 10ms

    def run(self):
        self.connection_ = r.connect()
        th1 = threading.Thread(target=self.rethink_vehicle_cb)
        th2 = threading.Thread(target=self.rethink_target_cb)
        th1.start()
        th2.start()
        self.connection_.use(self.db_name_)
        self.UID_counter = 0

        while not rospy.is_shutdown():
            # self.my_targets.pop(0)
            # self.my_targets_ids.pop(0)

            # if self.targets.count() > 0:
            #     pass
            print("---------")

            text_markerArray = MarkerArray()
            # Update the positions of the Vehicles in RViz
            for vehicle_id, vehicle_pos in self.vehicle_positions.iteritems():
                print("vehicle ID: {0}".format(type(vehicle_id)))
                odomMsg = Odometry()
                odomMsg.pose.pose.position.x = float(vehicle_pos[0])
                odomMsg.pose.pose.position.y = float(vehicle_pos[1])
                odomMsg.pose.pose.position.z = float(vehicle_pos[2])
                odomMsg.pose.pose.orientation.x = 0.0
                odomMsg.pose.pose.orientation.y = 0.0
                odomMsg.pose.pose.orientation.z = 0.0
                odomMsg.pose.pose.orientation.w = 0.0
                odomMsg.header.frame_id = "/map"
                odomMsg.child_frame_id = "/base_link"

                # Publish the multiple Odometry topics on the same ros master
                self.ros_publisher_objects[vehicle_id].publish(odomMsg)

                br = tf.TransformBroadcaster()
                br.sendTransform((vehicle_pos[0], vehicle_pos[1], vehicle_pos[2]),
                             tf.transformations.quaternion_from_euler(0, 0, 0),
                             rospy.Time.now(),
                             IPs[vehicle_id],
                             "/map")
                text_marker = Marker()
                text_marker.id = vehicle_id
                text_marker.header.frame_id = "/map"
                text_marker.type = text_marker.TEXT_VIEW_FACING
                text_marker.action = text_marker.ADD
                text_marker.text = IPs[vehicle_id]
                text_marker.scale.z = TEXT_SIZE
                text_marker.color.a = 1.0
                text_marker.color.r = 1.0
                text_marker.color.g = 1.0
                text_marker.color.b = 1.0
                text_marker.pose.position.x = vehicle_pos[0]
                text_marker.pose.position.y = vehicle_pos[1]
                text_marker.pose.position.z = vehicle_pos[2] + 0.5  # offset in depth to avoid text going through model
                text_markerArray.markers.append(text_marker)
            if self.vehicle_positions:
                self.text_markers_pub.publish(text_markerArray)

            # Update the target markers in RViz
            markerarray = MarkerArray()
            print("Numer of targets: {0}".format(len(self.targets.values())))
            for target_id, target_stats in self.targets.iteritems():
                marker = Marker()
                marker.header.frame_id = "/map"
                marker.type = marker.SPHERE
                marker.action = marker.ADD
                marker.scale.x = 0.5
                marker.scale.y = 0.5
                marker.scale.z = 0.5
                marker.color.a = 1.0
                # Change the colour depending if the target is classified or not
                if target_stats[-2] == 0:
                    marker.color.r = 1.0
                    marker.color.g = 0.0
                    marker.color.b = 0.0
                elif target_stats[-2] == 1:
                    marker.color.r = 0.0
                    marker.color.g = 1.0
                    marker.color.b = 0.0
                marker.pose.orientation.w = 1.0
                marker.id = target_id
                marker.pose.position.x = target_stats[0]
                marker.pose.position.y = target_stats[1]       # This one doesn't need reversed as value obtain from Odom topic!!
                marker.pose.position.z = target_stats[2]
                markerarray.markers.append(marker)
            if self.targets:
                self.target_markers_pub.publish(markerarray)
            rospy.sleep(0.1)

        self.run_threads_ = False
        th1.join()
        th2.join()

class RvizPlotting(object):
    WORLD_FRAME = "/map"
    NAV_ODOM_TOPIC = "nav/odometry"

    VEHICLE_ODOM_MARKER_RVIZ = "/vehicle_odom_pos_rviz"
    OBJECT_MARKER_RVIZ = "/object_marker"
    PDDL_WPS_RVIZ = "/wp_marker"
    VISITED_WAYPOINTS_TOPIC = "/visited_wps"
    def __init__(self):
        # RViz Marker Topics
        self.vehicle_odom_rviz_pub = rospy.Publisher(self.VEHICLE_ODOM_MARKER_RVIZ, MarkerArray)
        self.object_rviz_pub = rospy.Publisher(self.OBJECT_MARKER_RVIZ, Marker)
        self.pddl_wps_pub = rospy.Publisher(self.PDDL_WPS_RVIZ, MarkerArray)

        # Initialise an empty dictionary which will hold the names and positions of the PDDL WP variables once the
        # service is called
        self.marker_objects = {}
        self.pddl_wps_data = {}
        self.wp_orientations = {}
        self.visited_wps = {}
        self.vehicle_marker_id = 0

    def object_markerCallback(self, req):
        # object_marker = copy.deepcopy(req.marker)
        object_marker = Marker()
        object_marker.header.frame_id = self.WORLD_FRAME
        object_marker.type = object_marker.SPHERE
        object_marker.action = object_marker.ADD
        #object_marker.scale.x = 0.1
        #object_marker.scale.y = 0.1
        #object_marker.scale.z = 0.1
        #object_marker.color.a = 0.5
        #object_marker.color.r = 0.0
        #object_marker.color.g = 1.0
        #object_marker.color.b = 1.0
        #object_marker.pose.orientation.w = 1.0
        #object_marker.id = self.vehicle_marker_id
        #object_marker.pose.position.x = point.x
        #object_marker.pose.position.y = point.y       # This one doesn't need reversed as value obtain from Odom topic!!
        #object_marker.pose.position.z = point.z

        # Publish the Marker Array
        self.object_rviz_pub.publish(object_marker)
        print "DRAWING OBJECT IN RVIZ"
        success = True
        return DrawObjectResponse(success)

    def handle_wp_markersCallback(self, req):
        """ Callback for '/draw_pddl_wps' service. Uses request type of MarkerArray and calls drawWaypoint()"""
        rospy.loginfo("Request received")
        markerArray_in = req.markers.markers
        self.pddl_wps_data = {}
        self.wp_orientations = {}
        been_visited = False
        markerArray_out = MarkerArray()
        wp_id = 0

        print "Waypoint Positions:"
        # Step through each marker in request and for each one draw a sphere and arrow (orientation)
        # At this stage, all waypoints are not visited, hence "been_visited" is False
        for marker in markerArray_in:
            self.marker_objects[marker.ns] = marker
            markerPosition = marker.pose.position
            # Populate the dictionary with "coarse_wp" names and a field stating whether the WP has been visited
            tmp_id = re.findall(r'(\w+?)(\d+)', marker.ns)
            namespace, wp_id = tmp_id[0][0], int(tmp_id[0][1])

            # Print out each Waypoints position and yaw for debugging purposes
            print marker.ns + ":"
            marker_quaternion = [marker.pose.orientation.x, marker.pose.orientation.y,
                                 -marker.pose.orientation.z, marker.pose.orientation.w]
            euler_yaw = tf.transformations.euler_from_quaternion(marker_quaternion)
            print "\tN: %s, E: %s, Y: %s" % (markerPosition.x, -markerPosition.y, euler_yaw)        # -east due to numbers are in RVIZ coord frame

            if namespace == "coarse_wp":
                # contents of this dictionary doesn't matter except for been_visited field.
                self.pddl_wps_data[marker.ns] = been_visited

                sphere_marker, arrow_marker = self.drawWaypoint(marker, been_visited)
                markerArray_out.markers.append(sphere_marker)
                markerArray_out.markers.append(arrow_marker)


        rospy.loginfo("Sending PDDL Waypoints to Rviz")
        rospy.loginfo("Initially ALL are RED for Unvisited!")
        self.pddl_wps_pub.publish(markerArray_out)

        print "Coarse_WPS: \n", self.pddl_wps_data

        success = True
        return DrawPDDLWpsResponse(success)

    def waypoints_visitedCallback(self, msg):
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@"
        rospy.loginfo("New PDDL Waypoint has been visited - Saving")
        v_wps = msg.waypoints
        print "List of Visited Waypoints: \n", v_wps
        markerArray = MarkerArray()

        for wp_name in v_wps:
            if self.pddl_wps_data[wp_name] == False:
                self.pddl_wps_data[wp_name] = True

                sphere_marker, arrow_marker = self.drawWaypoint(self.marker_objects[wp_name], self.pddl_wps_data[wp_name])
                markerArray.markers.append(sphere_marker)
                markerArray.markers.append(arrow_marker)

        rospy.loginfo("Updating MarkerArray to Rviz with visited waypoints")
        self.pddl_wps_pub.publish(markerArray)
        print "------------------------"

    def navupdateOdomRviz(self, vehicle_position):
        self.create_vehicleMarker(vehicle_position.pose.pose.position)

    def drawWaypoint(self, marker, visited):
        """ Method which takes a marker as input which contains ned pos and orientation for an arrow_marker """
        arrow_marker = copy.deepcopy(marker)
        print "Drawing %s" % marker.ns
        marker.type = marker.SPHERE
        marker.id = 0
        marker.scale.x = 0.4
        marker.scale.y = 0.4
        marker.scale.z = 0.4
        arrow_marker.type = marker.ARROW
        arrow_marker.id = 1
        arrow_marker.scale.x = 2.0 #0.15
        arrow_marker.scale.y = 2.0 #0.3
        arrow_marker.scale.z = 1.6 #0.8
        if visited:
            #print "Visited is True, hence green"
            marker.color.r = 0.0
            marker.color.g = 1.0
            marker.color.b = 0.0
            arrow_marker.color.r = 0.0
            arrow_marker.color.g = 1.0
            arrow_marker.color.b = 0.0
        else:
            #print "Visited is False, hence red"
            marker.color.r = 1.0
            marker.color.g = 0.0
            marker.color.b = 0.0
            arrow_marker.color.r = 1.0
            arrow_marker.color.g = 0.0
            arrow_marker.color.b = 0.0

        return marker, arrow_marker

    # def create_vehicleMarker(self, point):
    #     """ Plots the vehicles trajectory in light blue/turqoise spheres """
    #     odom_markerArray = MarkerArray()
    #
    #     odom_marker = Marker()
    #     odom_marker.header.frame_id = self.WORLD_FRAME
    #     odom_marker.type = odom_marker.SPHERE
    #     odom_marker.action = odom_marker.ADD
    #     odom_marker.scale.x = 0.1
    #     odom_marker.scale.y = 0.1
    #     odom_marker.scale.z = 0.1
    #     odom_marker.color.a = 0.5
    #     odom_marker.color.r = 0.0
    #     odom_marker.color.g = 1.0
    #     odom_marker.color.b = 1.0
    #     odom_marker.pose.orientation.w = 1.0
    #     odom_marker.id = self.vehicle_marker_id
    #     odom_marker.pose.position.x = point.x
    #     odom_marker.pose.position.y = point.y       # This one doesn't need reversed as value obtain from Odom topic!!
    #     odom_marker.pose.position.z = point.z
    #     odom_markerArray.markers.append(odom_marker)
    #     self.vehicle_marker_id = self.vehicle_marker_id + 1
    #     if self.vehicle_marker_id > 300:
    #         self.vehicle_marker_id = 0
    #
    #     # Publish the Marker Array
    #     self.vehicle_odom_rviz_pub.publish(odom_markerArray)

if __name__=='__main__':
    rospy.init_node("sauv_executor")
    module_id = sys.argv[1]

    executor = RViz_Rethink_MultiVehicle_Visualisation(module_id=module_id, test_executor=True)
    print("Finished Init")
    executor.run()

    print("Finished insertion into DB")
    print("Closing Down")
