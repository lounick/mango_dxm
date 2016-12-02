#!/usr/bin/env python

import os
import sys
import argparse
import subprocess
import time
import signal

class ROSBagPlayer(object):
    def __init__(self, args):
        self.bagfiles   = args.bag
        self.sunset_ids = args.sunset_id
        self.namespaces = args.namespace
        self.processes = []

        # attach a signal handler to the main process to catch keyboard interrupts
        signal.signal(signal.SIGINT, self.signal_term_handler)

    def play_bags(self):
        """ Spwan a rosbag play in a subprocess giving all topics a namespace """
        print("Bagfiles: {0}".format(self.bagfiles))
        print("Sunset IDs: {0}".format(self.sunset_ids))
        print("Namespaces: {0}".format(self.namespaces))

        for input_idx in range(len(self.bagfiles)):
            bagfile = self.bagfiles[input_idx]
            sunset_id = self.sunset_ids[input_idx]
            namespace = self.namespaces[input_idx]

            # Put here only topics that you want to come under the new namespace. If a topic name is incorrect and does
            # not exist, the rosbag will not throw an exception, it will just publish the topic under it's original name
            TOPICS = ["/clock",
                      "/forces/sim/body",
                      "/nav/nav_sts",
                      "/nav/odometry",
                      "/nav/sim/currents",
                      "/pilot/position_req",
                      "/pilot/forces",
                      "/pilot/status",
                      "/rosout",
                      "/tf",
                      "/thrusters/commands",
                      "/thrusters/status",
                      "/target_info",
                      "/sunset_networking/sunset_notification_{0}".format(sunset_id),
                      "/sunset_networking/sunset_reception_{0}".format(sunset_id),
                      "/sunset_networking/sunset_transmit_{0}".format(sunset_id)]

            remappings = ["{0}:=/{1}{2}".format(topic, namespace, topic) for topic in TOPICS]
            print("---------------------------------")
            cmd = "rosbag play {0} {1}".format(bagfile, " ".join(remappings))
            print(cmd)

            # Spawn a child process for each rosbag. shell=True ensures that bashrc/ros env variables are loaded
            self.processes.append(subprocess.Popen(cmd, shell=True))

    def signal_term_handler(self, sig, frame):
        """
        Callback for SIGINT signal. Sends SIGTERM to process IDs of the subprocesses
        """
        print("Time to Kill the subprocess ...")
        for pro in self.processes:
            os.killpg(os.getpgid(pro.pid), signal.SIGTERM)  # Send the signal to all the process groups

if __name__ == '__main__':
    args = sys.argv
    if "-h" in args or "--help" in args:
        print("USAGE: python namespace_rosbag_file.py [bag_file.bag] [sunset_id] [namespace]")
        print("E.g. : python namespace_rosbag_file.py example_bag.bag 1 sauv")
        exit(0)

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bag', action='append', type=str)
    parser.add_argument('-s', '--sunset-id', action='append', type=str)
    parser.add_argument('-n', '--namespace', action='append', type=str)
    args = parser.parse_args()

    print(args)

    bagplayer = ROSBagPlayer(args)
    bagplayer.play_bags()
    # Keep main process from exiting so that SIGINT can be caught
    while True:
        time.sleep(1)

