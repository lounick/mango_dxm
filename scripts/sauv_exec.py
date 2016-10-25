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

class sauv_exec(object):
    def __init__(self, module_id=0, test_executor=False):
        self.module_id_ = module_id
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

    def handle_dbready(self, req):
        return DBReadyResponse(self.db_ready_)

    def init_db(self):
        while not self.db_server_ready():
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
        rospy.loginfo("Vehicle cb: Using sunset database")
        c.use(self.db_name_)
        feed = r.table('vehicles').changes().run(c)
        while self.run_threads_:
            try:
                item = feed.next(wait=False)
                rospy.loginfo("%s", item)
                # Process item
                if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                    self.db_out_.put((item['new_val']['id'], 1))
            except r.ReqlTimeoutError:
                time.sleep(0.01)  # Sleep thread

    def rethink_target_cb(self):
        # We have a target update. Queue it for transmission
        c = r.connect()
        rospy.loginfo("Target cb: Using sunset database")
        c.use(self.db_name_)
        feed = r.table('targets').changes().run(c)
        while self.run_threads_:
            try:
                item = feed.next(wait=False)
                rospy.loginfo("%s", item)
                # Process item
                if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                    self.db_out_.put((item['new_val']['id'], 2))
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
        self.synth_target_counter = 10
        finished_targets = False

        # Insert the SAUV initial details into rethinkDB
        vehicle = VehicleInfo(self.module_id_,0,0,0,0,0,rospy.Time.now().secs)
        self.insert_db("vehicles", vehicle)
        # generate lawnmower pattern/waypoints
        wps = self.generate_lawnmower()
        _action_executing = False
        while self._nav is None and not rospy.is_shutdown():
            rospy.sleep(0.1)
        while not rospy.is_shutdown():
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
                    target = TargetInfo(self.synth_target_counter, self._nav.position.north, # FIXME: Not using proper uids
                                        self._nav.position.east, 10, 1, 0, rospy.Time.now().secs)
                    self.insert_db("targets", target)

                    # change this to the next time point for synthetic target insertion
                    if self.synth_target_counter + 1 != len(CONFIG["synthetic_target_insertion_times"]):
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
                    rospy.loginfo("Action_completed: %s", _action_completed)
                    _action_executing = False
                    _action_completed = False

            if len(wps) == 0:
                rospy.loginfo("All Actions Completed, shutting down ...")
                rospy.signal_shutdown("program finished")
            rospy.sleep(0.1)


            # Insert synthetic targets into RethinkDB
            # TODO: resolve UID between vehicle entries and target entries
        self.run_threads_ = False
        th1.join()
        th2.join()

if __name__=='__main__':
    rospy.init_node("sauv_executor")
    module_id = sys.argv[1]

    executor = sauv_exec(module_id=module_id, test_executor=True)
    rospy.loginfo("Finished Init")
    executor.run()

    rospy.loginfo("Finished insertion into DB")
    rospy.loginfo("Closing Down")