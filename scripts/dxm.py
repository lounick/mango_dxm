#!/usr/bin/env python

import rethinkdb as r
from Queue import Queue, Empty
import threading
import time
import random

import traceback

import rospy
from sunset_ros_networking_msgs.msg import SunsetTransmission, SunsetReception, SunsetNotification
from mango_dxm.srv import *
import roslib
roslib.load_manifest('mango_dxm')


class VehicleInfo:
    type_id_ = 1
    status_shift_ = 4
    status_mask_ = 240
    intention_mask_ = 15
    pack_fmt_ = 'BQdddBBL'

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
    pack_fmt_ = 'BQdddBBL'

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


class Dxm:
    def __init__(self, module_id, port):
        self.msg_flag_ = False
        self.avail_nodes_ = []
        self.msg_count_ = 0
        self.module_id_ = module_id

        self.msg_pub_ = rospy.Publisher('sunset_networking/sunset_transmit_'+self.module_id_, SunsetTransmission, queue_size=10)
        self.msg_rec_sub_ = rospy.Subscriber('sunset_networking/sunset_reception_'+self.module_id_, SunsetReception, self.received_cb)
        self.notification_sub_ = rospy.Subscriber('sunset_networking/sunset_notification_'+self.module_id_, SunsetNotification, self.notification_cb)
        self.db_ready_srv_ = rospy.Service('db_ready', DBReady, self.handle_dbready)

        self.rate_ = rospy.Rate(10)  # 10hz
        # Initialise the database by connecting and dropping any existing tables

        # r.set_loop_type("tornado")
        # self.connection = r.connect(host='localhost', port=28015)
        self.connection_ = None
        self.run_threads_ = True
        self.last_inserted_id_ = None
        self.last_timestamp_ = None
        self.db_ready_ = False

        self.next_transmission_ = rospy.Time.now().secs + 10 + random.randint(-3, 3)
        self.candidates_ = {}
        self.transmitted_ = {}
        self.finished_ = {}

        self.db_in_ = Queue(maxsize=0)
        self.ack_in_ = Queue(maxsize=0)
        self.ack_out_ = Queue(maxsize=0)
        self.db_out_ = Queue(maxsize=0)

    def handle_dbready(self, req):
        return DBReadyResponse(self.db_ready_)

    def received_cb(self, msg):
        msg_pack_fmt_ = 'QdddBBL'
        msg_id = struct.unpack('L', msg.payload[0:4])
        type_id = struct.unpack('B', msg.payload[4])
        if msg.node_address not in self.avail_nodes_:
            self.avail_nodes_.append(msg.node_address)
        if type_id == 1:
            (uid, lat, lon, depth, info1, info2, timestamp) = struct.unpack(msg_pack_fmt_, msg.payload[5:])
            vehicle = VehicleInfo(uid, lat, lon, depth, info1, info2, timestamp)
            self.db_in_.put((type_id, vehicle))
            self.ack_out_.put((msg_id, msg.node_address))
        elif type_id == 2:
            (uid, lat, lon, depth, info1, info2, timestamp) = struct.unpack(msg_pack_fmt_, msg.payload[5:])
            target = TargetInfo(uid, lat, lon, depth, info1, info2, timestamp)
            self.db_in_.put((type_id, target))
            self.ack_out_.put((msg_id, msg.node_address))
        elif type_id == 3:
            acked_msg_id = struct.unpack('L', msg.payload[5:])
            self.ack_in_.put((acked_msg_id, msg.node_address))

    def notification_cb(self, msg):
        # Got a notification from sunset. Deal with that
        if msg.notification_type == 1:
            if msg.notification_subtype == 1:
                rospy.loginfo("Transmission started")
            elif msg.notification_subtype == 2:
                rospy.loginfo("Transmission finished")
                self.msg_flag_ = False
            elif msg.notification_subtype == 3:
                rospy.loginfo("Transmission error")
                self.msg_flag_ = False
                # TODO: If you get an error here you shouldn't even wait for acks
            else:
                rospy.logerr("GOT WRONG NOTIFICATION SUBTYPE")
        elif msg.notification_type == 2:
            if msg.notification_subtype == 1:
                rospy.logwarn("Wrong destination address (id)")
            elif msg.notification_subtype == 2:
                rospy.logwarn("Wrong payload length")
            else:
                rospy.logerr("GOT WRONG NOTIFICATION SUBTYPE")
            self.msg_flag_ = False
        else:
            rospy.logerr("GOT WRONG NOTIFICATION TYPE SETTING FLAG TO ALLOW MESSAGES")
            self.msg_flag_ = False

    def process_db_in(self):
        try:
            item = self.db_in_.get(block=False)
            if item[0] == 1:
                self.insert_db("vehicles", item[1])
            elif item[0] == 2:
                self.insert_db("targets", item[1])
            self.db_in_.task_done()
        except Empty:
            pass

    def process_ack_out(self):
        try:
            item = self.ack_out_.get(block=False)
            while self.msg_flag_: # TODO: Here we wait to transmit successfully an ack. Maybe we should just push all and let sunset handle transmission queue
                pass
            self.gen_and_send_ack(item[0], item[1])
            self.ack_out_.task_done()
        except Empty:
            pass

    def process_ack_in(self):
        try:
            item = self.ack_in_.get(block=False)
            self.process_ack(item[0], item[1])
            self.ack_in_.task_done()
        except Empty:
            pass

    def process_db_out(self):
        try:
            item = self.db_out_.get(block=False)
            self.candidates_[item[0]] = item[1]
            self.db_out_.task_done()
        except Empty:
            pass

    def init_db(self):
        databases = r.db_list().run(self.connection_)
        if "sunset" not in databases:
            print("Sunset db was not found. Creating.")
            r.db_create("sunset").run(self.connection_)
            print("Database created")
            self.connection_.use("sunset")
            print("Creating tables")
            r.table_create("vehicles").run(self.connection_)
            print(r.table_list().run(self.connection_))
            print("Table Vehicles created")
            r.table_create("targets").run(self.connection_)
            print(r.table_list().run(self.connection_))
            print("Table targets created")
        else:
            print("Sunset db was found. Droping tables")
            self.connection_.use("sunset")
            tables = r.table_list().run(self.connection_)
            print(tables)
            for t in tables:
                r.table_drop(t).run(self.connection_)
            for t in tables:
                r.table_create(t).run(self.connection_)
            if 'vehicles' not in tables:
                r.table_create("vehicles").run(self.connection_)
            if 'targets' not in tables:
                r.table_create("targets").run(self.connection_)
        print("Database initialised")
        self.db_ready_ = True

    def check_for_msg_ttl(self):
        to_remove = []
        for k, v in self.transmitted_.iteritems():
            if v['TTL'] <= rospy.Time.now().secs:
                # Message expired.
                # Check if we should insert it again into the queue
                if v['id'] not in self.candidates_:
                    rospy.loginfo("Message failed to transmit. Rescheduling transmission.")
                    to_remove.append(k)
                    self.candidates_[v['id']] = v['priority']
                else:
                    rospy.loginfo("Message failed to transmit. Has obsolate info. Discarding.")
        for k in to_remove:
            del self.transmitted_[k]

    def gen_and_send_ack(self, msg_id, address):
        payload = struct.pack('L', self.msg_count_)
        payload += struct.pack('B', 3)
        payload += struct.pack('L', msg_id)
        msg = SunsetTransmission()
        msg.payload = payload
        msg.node_address = address
        msg.header.stamp = rospy.Time.now()
        self.msg_pub_.publish(msg)
        self.msg_flag_ = True
        self.msg_count_ += 1

    def process_ack(self, acked_msg_id, address):
        # It is an ack for a message we sent. Add this to acked messages.
        if acked_msg_id in self.transmitted_:
            self.transmitted_[acked_msg_id]['acked'].append(address)
            if set(self.transmitted_[acked_msg_id]['sent']).issubset(set(self.transmitted_[acked_msg_id]['acked'])):
                # Message was succsessfully received. Process that.
                self.finished_[acked_msg_id] = self.transmitted_[acked_msg_id]
                del self.transmitted_[acked_msg_id]
        else:
            # Got an ack for a message that is no longer valid. Too bad.
            rospy.loginfo("Got an ack for a non valid message.")

    def transmit_candidate(self):
        if rospy.Time.now().secs >= self.next_transmission_:
            # It is time to transmit.
            if not self.msg_flag_:
                # Choose the candidate with the highest priority.
                # Make a message
                # Transmit it
                if len(self.candidates_) > 0:
                    candidate_v = 0
                    candidate_k = 0
                    for k in self.candidates_.iterkeys():
                        v = self.candidates_[k]
                        if v >= candidate_v:
                            candidate_v = v
                            candidate_k = k
                    payload = struct.pack('L', self.msg_count_)
                    candidate = None
                    if candidate_v == 1:
                        candidate = self.get_db('vehicles', candidate_k)
                    elif candidate_v == 2:
                        candidate = self.get_db('targets', candidate_k)
                    payload += candidate.pack()
                    recipients = self.avail_nodes_[:]
                    msg = SunsetTransmission()
                    msg.header.stamp = rospy.Time.now()
                    msg.node_address = 0
                    msg.payload = payload
                    del self.candidates_[candidate_k]
                    self.transmitted_[self.msg_count_] = {'TTL':rospy.Time.now().secs+1, 'id':candidate_k, 'priority':candidate_v, 'sent':recipients, 'acked':[]}
                    self.msg_flag_ = True
                    self.msg_pub_.publish(msg)
                    self.msg_count_ += 1
                # Claculate new transmission time
                self.next_transmission_ = rospy.Time.now().secs + 10 + random.randint(-3, 3)

    def get_db(self, table, uid):
        item = None
        record = r.table(table).get(uid).run(self.connection_)
        if table == 'vehicles':
            item = VehicleInfo(record['id'], record['lat'], record['lon'], record['depth'], record['intention'], record['status'], record['timestamp'])
        elif table == 'targets':
            item = TargetInfo(record['id'], record['lat'], record['lon'], record['depth'], record['vehicle_id'], record['classification'], record['timestamp'])
        return item

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
            res = r.table(table).update(item.__dict__).run(self.connection_)
            if res['errors'] != 0:
                self.last_inserted_id_ = tmp_id
                self.last_timestamp_ = tmp_ts
                rospy.logerr("Error updating in table: " + table + ". With error: " + res['first_error'])

    def rethink_vehicle_cb(self):
        # We have a vehicle update. Queue it for transmission
        c = r.connect()
        print("Vehicle cb: Using sunset database")
        c.use("sunset")
        feed = r.table('vehicles').changes().run(c)
        while self.run_threads_:
            try:
                item = feed.next(wait=False)
                print(item)
                # Process item
                if item['new_val']['id'] != self.last_inserted_id_ or item['new_val']['timestamp'] != self.last_timestamp_:
                    self.db_out_.put((item['new_val']['id'], 1))
            except r.ReqlTimeoutError:
                time.sleep(0.01)  # Sleep thread

    def rethink_target_cb(self):
        # We have a target update. Queue it for transmission
        c = r.connect()
        print("Target cb: Using sunset database")
        c.use("sunset")
        feed = r.table('targets').changes().run(c)
        while self.run_threads_:
            try:
                item = feed.next(wait=False)
                print(item)
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
        while not rospy.is_shutdown():
            try:
                self.process_db_in()
                self.process_ack_out()
                self.process_ack_in()
                self.process_db_out()
                self.check_for_msg_ttl()
                self.transmit_candidate()
                self.rate_.sleep()
            except Exception as e:
                self.run_threads_ = False
                print '-'*60
                traceback.print_exc(file=sys.stdout)
                print '-'*60
                raise e
        self.run_threads_ = False
        th1.join()
        th2.join()


def main(argv):
    module_id = argv[0]
    if len(argv) > 1:
        port = argv[1]
    else:
        port = 9000 + int(module_id)
    rospy.init_node('dxm')
    d = Dxm(module_id, port)
    try:
        d.run()
    except rospy.ROSException:
        pass

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: dxm moduleId (port)"
    else:
        main(sys.argv[1:])
