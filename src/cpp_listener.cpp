#include <iostream>
#include <string>
#include <ros/ros.h>
#include <ros/console.h>
#include <sunset_ros_networking_msgs/SunsetTransmission.h>
#include <sunset_ros_networking_msgs/SunsetReception.h>
#include <sunset_ros_networking_msgs/SunsetNotification.h>

union double2bin{
  double d;
  uint8_t bin[sizeof(double)];
};

union uint82bin{
  uint8_t i;
  uint8_t bin[sizeof(uint8_t)];
};

void rec_cb(sunset_ros_networking_msgs::SunsetReceptionConstPtr msg){
  double2bin conv;
  for(int i = 0; i < sizeof(double); ++i){
    conv.bin[i] = msg->payload[i];
  }
  ROS_WARN_STREAM("I heard " << conv.d << " from " << msg->node_address);
}

void notif_cb(sunset_ros_networking_msgs::SunsetNotificationConstPtr msg){
  ROS_WARN_STREAM("I got a notification");
}

int main(int argc, char* argv[]){
  std::string id(argv[1]);

  ros::init(argc,argv,"cpp_listener");
  ros::NodeHandle nh;

  ros::Subscriber rec_sub = nh.subscribe("sunset_networking/sunset_reception_" + id, 10, rec_cb);
  ros::Subscriber notif_sub = nh.subscribe("sunset_networking/sunset_notification_" + id, 1, notif_cb);
  ros::Publisher msg_pub_ = nh.advertise<sunset_ros_networking_msgs::SunsetTransmission>("sunset_networking/sunset_transmit_" + id, 1000);

  ros::Rate loop_rate(10);
  sleep(20);
  while (ros::ok()){
    ros::spinOnce();
    loop_rate.sleep();
  }
}