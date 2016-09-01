//
// Created by nick on 25/08/16.
//

#include <iostream>
#include <ros/ros.h>
#include <string>
#include <sunset_ros_networking_msgs/SunsetTransmission.h>
#include <sunset_ros_networking_msgs/SunsetReception.h>
#include <sunset_ros_networking_msgs/SunsetNotification.h>

union double2bin{
  double dbl;
  uint8_t bin[sizeof(dbl)];
};

class Talker {
 private:
  std::string id_;
  bool got_notification;
  ros::NodeHandle nh_;
  ros::Subscriber notif_sub_;
  ros::Publisher msg_pub_;
  ros::Subscriber rec_sub_;
 public:
  Talker(char* id) : id_(id) {
    notif_sub_ = nh_.subscribe("sunset_networking/sunset_notification_" + id_, 1, &Talker::notif_cb, this);
    rec_sub_ = nh_.subscribe("sunset_networking/sunset_reception_" + id_, 1, &Talker::rec_cb, this);
    msg_pub_ = nh_.advertise<sunset_ros_networking_msgs::SunsetTransmission>("sunset_networking/sunset_transmit_" + id_, 1000);
    got_notification = true;
  }
  void notif_cb(sunset_ros_networking_msgs::SunsetNotificationConstPtr msg) {
    if (msg->notification_type == 1) {
      if (msg->notification_subtype == 1) {
        ROS_WARN("Starting transmission");
      } else if (msg->notification_subtype == 2) {
        ROS_WARN("Transmission ended");
        got_notification = true;
      } else if (msg->notification_subtype == 3) {
        ROS_ERROR("Acoustic transmission error");
        got_notification = true;
      }
    } else if (msg->notification_type == 2) {
      if (msg->notification_subtype == 1) {
        ROS_ERROR("Message address is wrong");
      } else if (msg->notification_subtype == 2) {
        ROS_ERROR("Message is too long");
      }
      got_notification = true;
    }
  }

  void rec_cb(sunset_ros_networking_msgs::SunsetReceptionConstPtr msg){
    ROS_WARN_STREAM("I received something.");
  }

  void run(){
    ros::Rate loop_rate(10);
    int count = 0;
    while(ros::ok()){
      if(got_notification){
        //Create a message and send.
        ROS_WARN("Publishing a message");
        sunset_ros_networking_msgs::SunsetTransmission msg;
        std::vector<uint8_t> payload;
//        double2bin converter;
//        converter.dbl = 189.345;
//        payload.reserve(sizeof(converter.dbl));
//        for (int i = 0; i < sizeof(converter.dbl); i++){
//          payload.push_back(converter.bin[i]);
//        }
        payload.push_back(static_cast<uint8_t>(254));
        payload.push_back(static_cast<uint8_t>(9));
        payload.push_back(static_cast<uint8_t>(9));
        payload.push_back(static_cast<uint8_t>(9));
        payload.push_back(static_cast<uint8_t>(0));
        payload.push_back(static_cast<uint8_t>(6));
        payload.push_back(static_cast<uint8_t>(6));
        payload.push_back(static_cast<uint8_t>(6));
        msg.node_address = 2;
        msg.payload = payload;
        msg_pub_.publish(msg);
        got_notification = false;
        count = 0;
      }/*else{
        if (count < 100){
          ++count;
        }else{
          got_notification = true;
        }
      }*/
      ros::spinOnce();
      loop_rate.sleep();
    }
  }
};

int main(int argc, char* argv[]) {

  if(argc < 2){
    std::cout << "Usage: cpp_talker id" << std::endl;
    return -1;
  }
  ros::init(argc, argv, "cpp_talker");
  Talker t = Talker(argv[1]);
  sleep(5);
  t.run();
  return 0;
}
