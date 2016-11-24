#!/usr/bin/env python
import sys
# sys.path.append("/home/gordon/ros_workspace/vehicle_interface/src/vehicle_interface")

import roslib; roslib.load_manifest('vehicle_core')
import rospy
import threading
import rethinkdb as r
import functools
import time, sched
import random

# __init__.py must be in the same directory as this script, also any module that is calling it unless
# this is put on PYTHONPATH environment variable
from lawnmower_generator import *
from vehicle_interface.msg import PilotRequest, Vector6
from auv_msgs.msg import NavSts
from mango_dxm.srv import *

import struct

CONFIG = {
    "VehicleUID": 0,
    "intention": "sauv",
    "lawnmower_area": np.array([[0, 0, 2],
                                [0, 20, 2],
                                [-20, 20, 2],
                                [-20, 0, 2]]),
    "start_corner": 0,
    "spacing": 5,
    "overlap": 0,
    "synthetic_target_insertion_times": [1, 2, 3]  #in minutes
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

def eucledian_distance(a,b):
    return (a[0]-b[0])**2 + (a[1]-b[1])**2 + (a[2]-b[2])**2

class iauv_exec(object):
    def __init__(self, module_id=0, test_executor=False):
        self.module_id_ = int(module_id)
        self.db_name_ = "sunset_"+module_id
        self.db_ready_srv_ = rospy.Service('db_ready', DBReady, self.handle_dbready)

        self.pilot_pub = rospy.Publisher("pilot/position_req", PilotRequest)
        self.nav_sub = rospy.Subscriber("nav/nav_sts", NavSts, self.navCallback)
        self.db_server_ready = rospy.ServiceProxy('db_ready', DBReady)
        self._nav = None
        self.start_time = time.time()

        self.connection_ = None
        self.run_threads_ = True
        self.last_inserted_id_ = None
        self.last_timestamp_ = None
        self.db_ready_ = False
        self.targets = []
        self.target_ids = []
        self.scheduler_ = sched.scheduler(time.time, time.sleep)

        self.vehicle_positions = {}
        self.my_targets = []
        self.my_targets_ids = []
        
        self.nav_count_ = 0

    def handle_dbready(self, req):
        return DBReadyResponse(self.db_ready_)

    def init_db(self):
        while not self.db_server_ready():
            rospy.sleep(0.1)

    def navCallback(self, msg):
        self._nav = msg
        self.vehicle_positions[self.module_id_] = [self._nav.position.north, self._nav.position.east, self._nav.position.depth]
        self.nav_count_ += 1
        if self.nav_count_ > 600:
            self.update_nav()
            self.nav_count_ = 0

    def update_nav(self):
        print(type(self.module_id_))
        print(type(int(self.module_id_)))
        v = VehicleInfo(int(self.module_id_), self._nav.position.north, self._nav.position.east, self._nav.position.depth, 0, 0, rospy.Time.now().secs)
        r.table("vehicles").get(self.module_id_).update(v.__dict__).run(self.connection_)
        #self.scheduler_.enter(30, 1, self.update_nav, ())
        #threading.Timer(30, self.update_nav, ()).start()

    def generate_lawnmower(self):
        fixes, cols = pattern_from_ned(CONFIG["lawnmower_area"], CONFIG["start_corner"],
                                       CONFIG["spacing"], CONFIG["overlap"])
        print("Lawnmower Waypoints: {0}".format(fixes))
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
                    self.targets.append([item['new_val']['lat'], item['new_val']['lon'], item['new_val']['depth'],
                                         item['new_val']['vehicle_id'], item['new_val']['classification'], item['new_val']['timestamp']])
                    self.target_ids.append(item['new_val']['id'])
                    min_dist = 1000000
                    vid = -1
                    target_pos = [item['new_val']['lat'], item['new_val']['lon'], item['new_val']['depth']]
                    for k, v in self.vehicle_positions.iteritems():
                        dist = eucledian_distance(v,target_pos)
                        if dist < min_dist:
                            dist = min_dist
                            vid = k
                    if vid == self.module_id_:
                        self.my_targets.append([item['new_val']['lat'], item['new_val']['lon'], item['new_val']['depth'],
                                         item['new_val']['vehicle_id'], item['new_val']['classification'], item['new_val']['timestamp']])
                        self.my_targets_ids.append(item['new_val']['id'])
                        self._action_executing = False
            except r.ReqlTimeoutError:
                time.sleep(0.01)  # Sleep thread for 10ms

    def run(self):
        self.connection_ = r.connect()
        self.init_db()
        th1 = threading.Thread(target=self.rethink_vehicle_cb)
        th2 = threading.Thread(target=self.rethink_target_cb)
        th1.start()
        th2.start()
        self.connection_.use(self.db_name_)
        self.UID_counter = 0
        self.synth_target_counter = 0

        # Insert the SAUV initial details into rethinkDB
        vehicle = VehicleInfo(int(self.module_id_),0,0,0,0,0,rospy.Time.now().secs)
        self.insert_db("vehicles", vehicle)

        self._action_executing = False
        while self._nav is None and not rospy.is_shutdown():
            rospy.sleep(0.1)

        #self.scheduler_.enter(30, 1, self.update_nav, ())
        #threading.Timer(30, self.update_nav, ()).start()

        while not rospy.is_shutdown():
            if len(self.my_targets) > 0:
                if not self._action_executing:
                    # Take an action (waypoint) from the beginning of the list and send it to the pilot
                    wp = self.my_targets[0]
                    target_id = self.my_targets_ids[0]
                    self._action_executing = True
                    pilotMsg = PilotRequest()
                    pilotMsg.position = wp
                    print("Action/WP Executing: {0}".format(wp))
                    print("please wait ...")
                    self.pilot_pub.publish(pilotMsg)
                else:
                    # Do some other stuff to pass the time whilst checking to see if the action has completed yet
                    # Check if the action/waypoint request has been completed
                    current_position = [self._nav.position.north, self._nav.position.east, self._nav.position.depth]
                    _action_completed = waypointReached(current_position, wp[0:3], 0.5)
                    if _action_completed:
                        print("Action_completed: {0}".format(_action_completed))
                        target = TargetInfo(target_id, wp[0], wp[1], wp[2], self.module_id_, 1, rospy.get_time())
                        self.insert_db("targets", target)
                        self.my_targets.pop(0)
                        self.my_targets_ids.pop(0)
                        self._action_executing = False
                        _action_completed = False

            rospy.sleep(0.1)


            # Insert synthetic targets into RethinkDB
            # TODO: resolve UID between vehicle entries and target entries
        self.run_threads_ = False
        th1.join()
        th2.join()

if __name__=='__main__':
    rospy.init_node("sauv_executor")
    module_id = sys.argv[1]

    executor = iauv_exec(module_id=module_id, test_executor=True)
    print("Finished Init")
    executor.run()

    print("Finished insertion into DB")
    print("Closing Down")
