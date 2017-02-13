#!/usr/bin/env python
import sys
# sys.path.append("/home/gordon/ros_workspace/vehicle_interface/src/vehicle_interface")

import roslib; roslib.load_manifest('vehicle_core')
import rospy
import threading
import rethinkdb as r
import functools
import time
import random

# __init__.py must be in the same directory as this script, also any module that is calling it unless
# this is put on PYTHONPATH environment variable
from lawnmower_generator import *
from vehicle_interface.msg import PilotRequest, Vector6
from vehicle_interface.srv import BooleanService
from auv_msgs.msg import NavSts
from mango_dxm.srv import *

from visualization_msgs.msg import MarkerArray, Marker

import struct

CONFIG = {
    "VehicleUID": 0,
    "intention": "sauv",
    "lawnmower_area": np.array([[15, -15, 2],
                               [15, 15, 2],
                               [-15, 15, 2],
                               [-15, -15, 2]]),
    "start_corner": 0,
    "spacing": 5,
    "overlap": 0,
    "synthetic_target_insertion_times": [1.5, 3.0, 4.5, 6.0]#[0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0]  #in minutes
}

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

def waypointReached(a_list, b_list, e):
    def epsilonEquals(a, b, e):
        return a < b + e and a > b - e
    return epsilonEquals(a_list[0], b_list[0], e) and \
           epsilonEquals(a_list[1], b_list[1], e) and \
           epsilonEquals(a_list[2], b_list[2], e)

class sauv_exec(object):
    def __init__(self, module_id=0, test_executor=False):
        self.module_id_ = module_id
        self.db_name_ = "sunset_"+str(module_id)

        self.target_pub_ = rospy.Publisher("target_info", Vector6)
        self.pilot_pub = rospy.Publisher("pilot/position_req", PilotRequest)
        self.nav_sub = rospy.Subscriber("nav/nav_sts", NavSts, self.navCallback)
        rospy.wait_for_service('db_ready')
        self.db_server_ready = rospy.ServiceProxy('db_ready', DBReady)
        rospy.wait_for_service('pilot/switch')
        self.pilot_switch = rospy.ServiceProxy('pilot/switch', BooleanService)
        self._nav = None
        self.start_time = time.time()

        self.connection_ = None
        self.run_threads_ = True
        self.last_inserted_id_ = None
        self.last_timestamp_ = None

        self.vehicle_positions = {}

        self.marker_array_ = []
        self.marker_position_ = []
        self.marker_uids_ = []
        self.marker_colors_ = []
        self.color_red = [1, 0, 0, 1]
        self.color_green = [0.000, 0.502, 0.000, 1]
        self.color_orange = [1.000, 0.647, 0.000, 1]
        self.vis_pub_ = rospy.Publisher("visualization_marker_array", MarkerArray, queue_size=100)

    def print_markers(self):
        markerArray = MarkerArray()
        for i in range(len(self.marker_colors_)):
            marker = Marker()
            marker.header.frame_id = "/map"
            marker.header.stamp = rospy.Time().now()
            marker.lifetime = rospy.Time(0.1)
            marker.ns = "sauv"
            marker.id = i
            marker.type = marker.SPHERE
            marker.action = marker.ADD
            marker.scale.x = 1.0
            marker.scale.y = 1.0
            marker.scale.z = 1.0
            marker.color.r = self.marker_colors_[i][0]
            marker.color.g = self.marker_colors_[i][1]
            marker.color.b = self.marker_colors_[i][2]
            marker.color.a = self.marker_colors_[i][3]
            marker.pose.orientation.w = 1.0
            marker.pose.position.x = self.marker_position_[i][0]
            marker.pose.position.y = self.marker_position_[i][1]
            marker.pose.position.z = self.marker_position_[i][2]
            markerArray.markers.append(marker)
        self.vis_pub_.publish(markerArray)

    def init_db(self):
        rospy.logerr("@@@@@@@@@@@@@@@ Waiting for DB!")
        while not self.db_server_ready():
            rospy.logerr("@@@@@@@@@@@@@@@ Waiting for DB!")
            rospy.sleep(0.1)

    def navCallback(self, msg):
        self._nav = msg

    def generate_lawnmower(self):
        fixes, cols = pattern_from_ned(CONFIG["lawnmower_area"], CONFIG["start_corner"],
                                       CONFIG["spacing"], CONFIG["overlap"])
        rospy.loginfo("Lawnmower Waypoints: %s", fixes)
        return list(fixes)

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
            res = r.table(table).get(item.id).update(item.__dict__).run(self.connection_)
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

    def rethink_vehicle_cb(self):
        # We have a vehicle update. Queue it for transmission
        c = r.connect()
        rospy.loginfo("Vehicle cb: Using sunset database %s", self.db_name_)
        c.use(self.db_name_)
        feed = r.table("vehicles").changes().run(c)
        while self.run_threads_:
            try:
                item = feed.next(wait=False)
                #rospy.loginfo("%s", item)
                # Process item
                if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                    self.vehicle_positions[item['new_val']['id']] = [item['new_val']['lat'], item['new_val']['lon'], item['new_val']['depth']]
            except r.ReqlTimeoutError:
                time.sleep(0.01)  # Sleep thread

    def rethink_target_cb(self):
        # We have a target update. Queue it for transmission
        c = r.connect()
        rospy.loginfo("Target cb: Using sunset database")
        c.use(self.db_name_)
        feed = r.table("targets").changes().run(c)
        while self.run_threads_:
            try:
                item = feed.next(wait=False)
                #rospy.loginfo("%s", item)
                # Process item
                if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                    rospy.logerr("Vehicle %s classified target %s", item['new_val']['vehicle_id'], item['new_val']['id'])
                    idx = self.marker_uids_.index(int(item['new_val']['id']))
                    if int(item['new_val']['vehicle_id']) == 2:
                        self.marker_colors_[idx] = self.color_orange
                    elif int(item['new_val']['vehicle_id']) == 3:
                        self.marker_colors_[idx] = self.color_green
            except r.ReqlTimeoutError:
                time.sleep(0.01)  # Sleep thread for 10ms

    def run(self):
        self.connection_ = r.connect()
        rospy.logerr("BEFOREeeeeeeeeeeeeeeeeeeeeeee")
        self.init_db()
        rospy.logerr("AFTER!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        th1 = threading.Thread(target=self.rethink_vehicle_cb)
        th2 = threading.Thread(target=self.rethink_target_cb)
        th1.start()
        th2.start()
        self.connection_.use(self.db_name_)
        self.UID_counter = 0
        self.synth_target_counter = 0
        finished_targets = False
        #BEST
        # self.synth_targets =[(41.23214085671712, -27.201950413623123, 2.507601851990801),
        #                      (5.7165853992716364, 28.13035108084523, 3.2569861341176365),
        #                      (-22.921098870304235, -27.35763420616464, 5.93929430044889),
        #                      (28.791543007698465, 37.455757992696704, 5.3225382421725325),
        #                      (-26.175709934212932, -36.401480691211496, 5.550885565126501),
        #                      (27.076177241433314, -41.184145119401194, 1.4703737342842098),
        #                      (-16.70984410691139, -23.847370956216107, 7.788646903251107),
        #                      (23.81731236684422, 1.3440963907671133, 5.307472651327279),
        #                      (46.814325120138676, -34.4773656609357, 2.476461381772065),
        #                      (3.8778329798072946, -25.708114420837724, 4.587886340854672)]
        #WORST
        # self.synth_targets = [(30.329564123281358, 28.14863010466327, 1.2858447488292848),
        #                       (26.521784945074444, 45.77654364367736, 3.8838172917747245),
        #                       (36.932781852488006, -31.915917248663995, 7.937098691329582),
        #                       (-25.701251788032277, 40.307245254273994, 5.033835962400384),
        #                       (-32.44150586223431, 22.839350609725983, 2.4313900618466375),
        #                       (-10.14252818801802, 46.80607146932462, 5.349518735578121),
        #                       (13.35717296722597, 28.22779351109702, 7.062150750343796),
        #                       (4.549645213892937, 37.70218387616838, 6.977401746766278),
        #                       (5.875575667200863, 13.256456241209321, 7.267109179287102),
        #                       (-27.292285165833707, 34.13956385187832, 9.059700779397224)]
        #MEDIUM
        # self.synth_targets = [(-10.224606425392999, -30.138106430819345, 7.596271732924227),
        # (-23.362031552619676, -47.51655539856652, 6.686983265656394),
        # (-28.901440987732563, 36.204064699048146, 7.177438941597014),
        # (-23.774913980125344, -14.093801419700029, 8.968190900167556),
        # (-21.467264097947037, -14.348123052658458, 6.70399219156594),
        # (-32.934170653480095, 32.688030559395074, 6.27463489840251),
        # (-0.813063922337335, 8.18106433834275, 4.474609531919449),
        # (-13.289516561288458, -31.564176955655643, 8.754248030997406),
        # (19.139010260773745, -8.599469956397662, 9.110762108322994),
        # (22.89877239426154, 27.520424548061357, 5.28889078851552)]

        self.synth_targets = []
        filename = "/home/nick/src/random_target_generator/10targets10.txt"
        f = open(filename, 'r')
        for line in f:
            n,e,d = line.split(" ")
            print((float(n),float(e),float(d)))
            self.synth_targets.append((float(n),float(e),float(d)))

        # Insert the SAUV initial details into rethinkDB
        vehicle = VehicleInfo(self.module_id_,0,0,0,0,0,rospy.Time.now().secs)
        # self.insert_db("vehicles", vehicle)
        # generate lawnmower pattern/waypoints
        wps = self.generate_lawnmower()
        _action_executing = False
        while self._nav is None and not rospy.is_shutdown():
            print("@@@@@@@@@@@@@@@ Waiting for NAV!")
            rospy.sleep(0.1)
        self.pilot_switch(True)
        rospy.sleep(30)
        ma = MarkerArray()
        while not rospy.is_shutdown():
            self.print_markers()
            if len(wps) > 0 and not _action_executing:
                # Take an action (waypoint) from the beginning of the list and send it to the pilot
                wp = wps.pop(0)
                _action_executing = True
                pilotMsg = PilotRequest()
                pilotMsg.position = wp
                rospy.loginfo("Action/WP Executing: %s", wp)
                rospy.loginfo("please wait ...")
                self.pilot_pub.publish(pilotMsg)
            else:
                # Do some other stuff to pass the time whilst checking to see if the action has completed yet
                time_now_mins = float(time.time() - self.start_time) / 60.0
                if time_now_mins > CONFIG["synthetic_target_insertion_times"][self.synth_target_counter] and \
                    not finished_targets:
                    # self.synth_target_counter < len(CONFIG["synthetic_target_insertion_times"]):
                    # Time to insert a synthetic target into the database
                    rospy.loginfo("Inserting synth target into RethinkDB")
                    target = TargetInfo(self.synth_target_counter+10, self._nav.position.north, # FIXME: Not using proper uids
                                        self._nav.position.east, 10, 1, 0, rospy.Time.now().secs)

                    self.marker_position_.append([self._nav.position.north, -self._nav.position.east, -10])
                    self.marker_colors_.append(self.color_red)
                    self.marker_uids_.append(self.synth_target_counter+10)

                    # north = random.uniform(-50,50)
                    # east = random.uniform(-50,50)
                    # depth = random.uniform(1,10)
                    # north = self.synth_targets[self.synth_target_counter][0]
                    # east = self.synth_targets[self.synth_target_counter][1]
                    # depth = self.synth_targets[self.synth_target_counter][2]
                    # target = TargetInfo(self.synth_target_counter+10, north, east, depth, 1, 0, rospy.Time.now().secs)
                    self.insert_db("targets", target)

                    # change this to the next time point for synthetic target insertion
                    #if self.synth_target_counter + 1 != len(CONFIG["synthetic_target_insertion_times"]):
                    # msg = Vector6()
                    # msg.values = [north, east, depth, 0, 0, 0]
                    # self.target_pub_.publish(msg)
                    if CONFIG["synthetic_target_insertion_times"][self.synth_target_counter] != CONFIG["synthetic_target_insertion_times"][-1]:
                        self.synth_target_counter += 1
                    else:
                        finished_targets = True
                    # if self.synth_target_counter == len(CONFIG["synthetic_target_insertion_times"]):
                    #     rospy.loginfo("run out of targets ....")
                    #     self.synth_target_counter = len(CONFIG["synthetic_target_insertion_times"]) - 1

                # Check if the action/waypoint request has been completed
                current_position = [self._nav.position.north, self._nav.position.east, self._nav.position.depth]
                _action_completed = waypointReached(current_position, wp[0:3], 0.5)
                if _action_completed:
                    #rospy.loginfo("Action_completed: %s", _action_completed)
                    _action_executing = False
                    _action_completed = False

            #if len(wps) == 0:
            #    rospy.loginfo("All Actions Completed, shutting down ...")
            #    rospy.signal_shutdown("program finished")
            rospy.sleep(0.1)


            # Insert synthetic targets into RethinkDB
            # TODO: resolve UID between vehicle entries and target entries
        self.run_threads_ = False
        th1.join()
        th2.join()

if __name__=='__main__':
    rospy.init_node("sauv_executor")
    module_id = int(sys.argv[1])

    executor = sauv_exec(module_id=module_id, test_executor=True)
    rospy.loginfo("Finished Init")
    executor.run()

    rospy.loginfo("Finished insertion into DB")
    rospy.loginfo("Closing Down")
