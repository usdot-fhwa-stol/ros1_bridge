// Copyright 2015 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <unordered_set>

// include ROS 1
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wunused-parameter"
#endif
#include "ros/callback_queue.h"
#include "ros/ros.h"
#ifdef __clang__
# pragma clang diagnostic pop
#endif
#include "ros/this_node.h"
#include "ros/header.h"
#include "ros/service_manager.h"
#include "ros/transport/transport_tcp.h"

// include ROS 2
#include "rclcpp/rclcpp.hpp"
#include "rcpputils/scope_exit.hpp"

#include "ros1_bridge/bridge.hpp"
#include "parameter_bridge_helper.hpp"

std::mutex g_bridge_mutex;

struct Bridge1to2HandlesAndMessageTypes
{
  ros1_bridge::Bridge1to2Handles bridge_handles;
  std::string ros1_type_name;
  std::string ros2_type_name;
};

struct Bridge2to1HandlesAndMessageTypes
{
  ros1_bridge::Bridge2to1Handles bridge_handles;
  std::string ros1_type_name;
  std::string ros2_type_name;
};

bool find_command_option(const std::vector<std::string> & args, const std::string & option)
{
  return std::find(args.begin(), args.end(), option) != args.end();
}

bool get_flag_option(const std::vector<std::string> & args, const std::string & option)
{
  auto it = std::find(args.begin(), args.end(), option);
  return it != args.end();
}

bool parse_command_options(
  int argc, char ** argv, bool & output_topic_introspection,
  bool & bridge_all_1to2_topics, bool & bridge_all_2to1_topics,
  bool & multi_threads)
{
  std::vector<std::string> args(argv, argv + argc);

  if (find_command_option(args, "-h") || find_command_option(args, "--help")) {
    std::stringstream ss;
    ss << "Usage:" << std::endl;
    ss << " -h, --help: This message." << std::endl;
    ss << " --show-introspection: Print output of introspection of both sides of the bridge.";
    ss << std::endl;
    ss << " --print-pairs: Print a list of the supported ROS 2 <=> ROS 1 conversion pairs.";
    ss << std::endl;
    ss << " --bridge-all-topics: Bridge all topics in both directions, whether or not there is ";
    ss << "a matching subscriber." << std::endl;
    ss << " --bridge-all-1to2-topics: Bridge all ROS 1 topics to ROS 2, whether or not there is ";
    ss << "a matching subscriber." << std::endl;
    ss << " --bridge-all-2to1-topics: Bridge all ROS 2 topics to ROS 1, whether or not there is ";
    ss << "a matching subscriber." << std::endl;
    ss << " --multi-threads: Bridge with multiple threads for spinner of ROS 1 and ROS 2.";
    ss << std::endl;
    std::cout << ss.str();
    return false;
  }

  if (get_flag_option(args, "--print-pairs")) {
    auto mappings_2to1 = ros1_bridge::get_all_message_mappings_2to1();
    if (mappings_2to1.size() > 0) {
      printf("Supported ROS 2 <=> ROS 1 message type conversion pairs:\n");
      for (auto & pair : mappings_2to1) {
        printf("  - '%s' (ROS 2) <=> '%s' (ROS 1)\n", pair.first.c_str(), pair.second.c_str());
      }
    } else {
      printf("No message type conversion pairs supported.\n");
    }
    mappings_2to1 = ros1_bridge::get_all_service_mappings_2to1();
    if (mappings_2to1.size() > 0) {
      printf("Supported ROS 2 <=> ROS 1 service type conversion pairs:\n");
      for (auto & pair : mappings_2to1) {
        printf("  - '%s' (ROS 2) <=> '%s' (ROS 1)\n", pair.first.c_str(), pair.second.c_str());
      }
    } else {
      printf("No service type conversion pairs supported.\n");
    }
    return false;
  }

  output_topic_introspection = get_flag_option(args, "--show-introspection");

  bool bridge_all_topics = get_flag_option(args, "--bridge-all-topics");
  bridge_all_1to2_topics = bridge_all_topics || get_flag_option(args, "--bridge-all-1to2-topics");
  bridge_all_2to1_topics = bridge_all_topics || get_flag_option(args, "--bridge-all-2to1-topics");
  multi_threads = get_flag_option(args, "--multi-threads");

  return true;
}

void update_bridge(
  ros::NodeHandle & ros1_node,
  rclcpp::Node::SharedPtr ros2_node,
  const std::map<std::string, std::string> & ros1_publishers,
  const std::map<std::string, std::string> & ros1_subscribers,
  const std::map<std::string, std::string> & ros2_publishers,
  const std::map<std::string, std::string> & ros2_subscribers,
  const std::map<std::string, std::map<std::string, std::string>> & ros1_services,
  const std::map<std::string, std::map<std::string, std::string>> & ros2_services,
  std::map<std::string, Bridge1to2HandlesAndMessageTypes> & bridges_1to2,
  std::map<std::string, Bridge2to1HandlesAndMessageTypes> & bridges_2to1,
  std::map<std::string, ros1_bridge::ServiceBridge1to2> & service_bridges_1_to_2,
  std::map<std::string, ros1_bridge::ServiceBridge2to1> & service_bridges_2_to_1,
  const std::unordered_set<std::string> & parameter_reserved_connections,
  bool bridge_all_1to2_topics, bool bridge_all_2to1_topics,
  bool multi_threads = false)
{
  std::lock_guard<std::mutex> lock(g_bridge_mutex);

  // create 1to2 bridges
  for (auto ros1_publisher : ros1_publishers) {
    // identify topics available as ROS 1 publishers as well as ROS 2 subscribers
    auto topic_name = ros1_publisher.first;
    std::string ros1_type_name = ros1_publisher.second;
    std::string ros2_type_name;

    auto ros2_subscriber = ros2_subscribers.find(topic_name);
    if (ros2_subscriber == ros2_subscribers.end()) {
      if (!bridge_all_1to2_topics) {
        continue;
      }
      // update the ROS 2 type name to be that of the anticipated bridged type
      // TODO(dhood): support non 1-1 "bridge-all" mappings
      bool mapping_found = ros1_bridge::get_1to2_mapping(ros1_type_name, ros2_type_name);
      if (!mapping_found) {
        // printf("No known mapping for ROS 1 type '%s'\n", ros1_type_name.c_str());
        continue;
      }
      // printf("topic name '%s' has ROS 2 publishers\n", topic_name.c_str());
    } else {
      ros2_type_name = ros2_subscriber->second;
      // printf("topic name '%s' has ROS 1 publishers and ROS 2 subscribers\n", topic_name.c_str());
    }

    // check if 1to2 bridge for the topic exists
    if (bridges_1to2.find(topic_name) != bridges_1to2.end()) {
      auto bridge = bridges_1to2.find(topic_name)->second;
      if (bridge.ros1_type_name == ros1_type_name && bridge.ros2_type_name == ros2_type_name) {
        // skip if bridge with correct types is already in place
        continue;
      }
      // remove existing bridge with previous types
      bridges_1to2.erase(topic_name);
      printf("replace 1to2 bridge for topic '%s'\n", topic_name.c_str());
    }

    Bridge1to2HandlesAndMessageTypes bridge;
    bridge.ros1_type_name = ros1_type_name;
    bridge.ros2_type_name = ros2_type_name;

    auto ros2_publisher_qos = rclcpp::QoS(rclcpp::KeepLast(10));
    if (topic_name == "/tf_static") {
      ros2_publisher_qos.keep_all();
      ros2_publisher_qos.transient_local();
    }
    try {
      bridge.bridge_handles = ros1_bridge::create_bridge_from_1_to_2(
        ros1_node, ros2_node,
        bridge.ros1_type_name, topic_name, 10,
        bridge.ros2_type_name, topic_name, ros2_publisher_qos);
    } catch (std::runtime_error & e) {
      fprintf(
        stderr,
        "failed to create 1to2 bridge for topic '%s' "
        "with ROS 1 type '%s' and ROS 2 type '%s': %s\n",
        topic_name.c_str(), bridge.ros1_type_name.c_str(), bridge.ros2_type_name.c_str(), e.what());
      if (std::string(e.what()).find("No template specialization") != std::string::npos) {
        fprintf(stderr, "check the list of supported pairs with the `--print-pairs` option\n");
      }
      continue;
    }

    bridges_1to2[topic_name] = bridge;
    printf(
      "created 1to2 bridge for topic '%s' with ROS 1 type '%s' and ROS 2 type '%s'\n",
      topic_name.c_str(), bridge.ros1_type_name.c_str(), bridge.ros2_type_name.c_str());
  }

  // create 2to1 bridges
  for (auto ros2_publisher : ros2_publishers) {
    // identify topics available as ROS 1 subscribers as well as ROS 2 publishers
    auto topic_name = ros2_publisher.first;
    std::string ros2_type_name = ros2_publisher.second;
    std::string ros1_type_name;

    auto ros1_subscriber = ros1_subscribers.find(topic_name);
    if (ros1_subscriber == ros1_subscribers.end()) {
      if (!bridge_all_2to1_topics) {
        continue;
      }
      // update the ROS 1 type name to be that of the anticipated bridged type
      // TODO(dhood): support non 1-1 "bridge-all" mappings
      bool mapping_found = ros1_bridge::get_2to1_mapping(ros2_type_name, ros1_type_name);
      if (!mapping_found) {
        // printf("No known mapping for ROS 2 type '%s'\n", ros2_type_name.c_str());
        continue;
      }
      // printf("topic name '%s' has ROS 2 publishers\n", topic_name.c_str());
    } else {
      ros1_type_name = ros1_subscriber->second;
      // printf("topic name '%s' has ROS 1 subscribers and ROS 2 publishers\n", topic_name.c_str());
    }

    // check if 2to1 bridge for the topic exists
    if (bridges_2to1.find(topic_name) != bridges_2to1.end()) {
      auto bridge = bridges_2to1.find(topic_name)->second;
      if ((bridge.ros1_type_name == ros1_type_name || bridge.ros1_type_name == "") &&
        bridge.ros2_type_name == ros2_type_name)
      {
        // skip if bridge with correct types is already in place
        continue;
      }
      // remove existing bridge with previous types
      bridges_2to1.erase(topic_name);
      printf("replace 2to1 bridge for topic '%s'\n", topic_name.c_str());
    }

    Bridge2to1HandlesAndMessageTypes bridge;
    bridge.ros1_type_name = ros1_type_name;
    bridge.ros2_type_name = ros2_type_name;

    try {
      bridge.bridge_handles = ros1_bridge::create_bridge_from_2_to_1(
        ros2_node, ros1_node,
        bridge.ros2_type_name, topic_name, 10,
        bridge.ros1_type_name, topic_name, 10,
        nullptr,
        multi_threads);
    } catch (std::runtime_error & e) {
      fprintf(
        stderr,
        "failed to create 2to1 bridge for topic '%s' "
        "with ROS 2 type '%s' and ROS 1 type '%s': %s\n",
        topic_name.c_str(), bridge.ros2_type_name.c_str(), bridge.ros1_type_name.c_str(), e.what());
      if (std::string(e.what()).find("No template specialization") != std::string::npos) {
        fprintf(stderr, "check the list of supported pairs with the `--print-pairs` option\n");
      }
      continue;
    }

    bridges_2to1[topic_name] = bridge;
    printf(
      "created 2to1 bridge for topic '%s' with ROS 2 type '%s' and ROS 1 type '%s'\n",
      topic_name.c_str(), bridge.ros2_type_name.c_str(), bridge.ros1_type_name.c_str());
  }

  // remove obsolete bridges
  std::vector<std::string> to_be_removed_1to2;
  for (auto it : bridges_1to2) {
    std::string topic_name = it.first;
    if (
      parameter_reserved_connections.find(topic_name) == parameter_reserved_connections.end() // This topic is allowed to be removed
      && (ros1_publishers.find(topic_name) == ros1_publishers.end() ||
        (!bridge_all_1to2_topics && ros2_subscribers.find(topic_name) == ros2_subscribers.end())))
    {
      to_be_removed_1to2.push_back(topic_name);
    }
  }
  for (auto topic_name : to_be_removed_1to2) {
    bridges_1to2.erase(topic_name);
    printf("removed 1to2 bridge for topic '%s'\n", topic_name.c_str());
  }

  std::vector<std::string> to_be_removed_2to1;
  for (auto it : bridges_2to1) {
    std::string topic_name = it.first;
    if (
      parameter_reserved_connections.find(topic_name) == parameter_reserved_connections.end() // This topic is allowed to be removed
      && ((!bridge_all_2to1_topics && ros1_subscribers.find(topic_name) == ros1_subscribers.end()) ||
        ros2_publishers.find(topic_name) == ros2_publishers.end()))
    {
      to_be_removed_2to1.push_back(topic_name);
    }
  }
  for (auto topic_name : to_be_removed_2to1) {
    bridges_2to1.erase(topic_name);
    printf("removed 2to1 bridge for topic '%s'\n", topic_name.c_str());
  }

  // create bridges for ros1 services
  for (auto & service : ros1_services) {
    auto & name = service.first;
    auto & details = service.second;
    if (
      service_bridges_2_to_1.find(name) == service_bridges_2_to_1.end() &&
      service_bridges_1_to_2.find(name) == service_bridges_1_to_2.end())
    {
      auto factory = ros1_bridge::get_service_factory(
        "ros1", details.at("package"), details.at("name"));
      if (factory) {
        try {
          service_bridges_2_to_1[name] = factory->service_bridge_2_to_1(
            ros1_node, ros2_node, name, multi_threads);
          printf("Created 2 to 1 bridge for service %s\n", name.data());
        } catch (std::runtime_error & e) {
          fprintf(stderr, "Failed to created a bridge: %s\n", e.what());
        }
      }
    }
  }

  int service_execution_timeout{5};
  ros1_node.getParamCached(
    "ros1_bridge/dynamic_bridge/service_execution_timeout", service_execution_timeout);

  // create bridges for ros2 services
  for (auto & service : ros2_services) {
    auto & name = service.first;
    auto & details = service.second;
    if (
      service_bridges_1_to_2.find(name) == service_bridges_1_to_2.end() &&
      service_bridges_2_to_1.find(name) == service_bridges_2_to_1.end())
    {
      auto factory = ros1_bridge::get_service_factory(
        "ros2", details.at("package"), details.at("name"));
      if (factory) {
        try {
          service_bridges_1_to_2[name] = factory->service_bridge_1_to_2(
            ros1_node, ros2_node, name, service_execution_timeout, multi_threads);
          printf("Created 1 to 2 bridge for service %s\n", name.data());
        } catch (std::runtime_error & e) {
          fprintf(stderr, "Failed to created a bridge: %s\n", e.what());
        }
      }
    }
  }

  // remove obsolete ros1 services
  for (auto it = service_bridges_2_to_1.begin(); it != service_bridges_2_to_1.end(); ) {
    if (parameter_reserved_connections.find(it->first) == parameter_reserved_connections.end() // This topic is allowed to be removed
       && ros1_services.find(it->first) == ros1_services.end()) {

      printf("Removed 2 to 1 bridge for service %s\n", it->first.data());
      try {
        it = service_bridges_2_to_1.erase(it);
      } catch (std::runtime_error & e) {
        fprintf(stderr, "There was an error while removing 2 to 1 bridge: %s\n", e.what());
      }
    } else {
      ++it;
    }
  }

  // remove obsolete ros2 services
  for (auto it = service_bridges_1_to_2.begin(); it != service_bridges_1_to_2.end(); ) {
    if (parameter_reserved_connections.find(it->first) == parameter_reserved_connections.end() // This topic is allowed to be removed
        && ros2_services.find(it->first) == ros2_services.end()) {
      printf("Removed 1 to 2 bridge for service %s\n", it->first.data());
      try {
        it->second.server.shutdown();
        it = service_bridges_1_to_2.erase(it);
      } catch (std::runtime_error & e) {
        fprintf(stderr, "There was an error while removing 1 to 2 bridge: %s\n", e.what());
      }
    } else {
      ++it;
    }
  }
}

void get_ros1_service_info(
  const std::string name, std::map<std::string, std::map<std::string, std::string>> & ros1_services)
{
  // NOTE(rkozik):
  // I tried to use Connection class but could not make it work
  // auto callback = [](const ros::ConnectionPtr&, const ros::Header&)
  //                 { printf("Callback\n"); return true; };
  // ros::HeaderReceivedFunc f(callback);
  // ros::ConnectionPtr connection(new ros::Connection);
  // connection->initialize(transport, false, ros::HeaderReceivedFunc());
  ros::ServiceManager manager;
  std::string host;
  std::uint32_t port;
  if (!manager.lookupService(name, host, port)) {
    fprintf(stderr, "Failed to look up %s\n", name.data());
    return;
  }
  ros::TransportTCPPtr transport(new ros::TransportTCP(nullptr, ros::TransportTCP::SYNCHRONOUS));
  auto transport_exit = rcpputils::make_scope_exit(
    [transport]() {
      transport->close();
    });
  if (!transport->connect(host, port)) {
    fprintf(stderr, "Failed to connect to %s (%s:%d)\n", name.data(), host.data(), port);
    return;
  }
  ros::M_string header_out;
  header_out["probe"] = "1";
  header_out["md5sum"] = "*";
  header_out["service"] = name;
  header_out["callerid"] = ros::this_node::getName();
  boost::shared_array<uint8_t> buffer;
  uint32_t len;
  ros::Header::write(header_out, buffer, len);
  std::vector<uint8_t> message(len + 4);
  std::memcpy(&message[0], &len, 4);
  std::memcpy(&message[4], buffer.get(), len);
  transport->write(message.data(), message.size());
  uint32_t length;
  auto read = transport->read(reinterpret_cast<uint8_t *>(&length), 4);
  if (read != 4) {
    fprintf(stderr, "Failed to read a response from a service server\n");
    return;
  }
  std::vector<uint8_t> response(length);
  read = transport->read(response.data(), length);
  if (read < 0 || static_cast<uint32_t>(read) != length) {
    fprintf(stderr, "Failed to read a response from a service server\n");
    return;
  }
  std::string key = name;
  ros1_services[key] = std::map<std::string, std::string>();
  ros::Header header_in;
  std::string error;
  auto success = header_in.parse(response.data(), length, error);
  if (!success) {
    fprintf(stderr, "%s\n", error.data());
    return;
  }
  for (std::string field : {"type"}) {
    std::string value;
    auto success = header_in.getValue(field, value);
    if (!success) {
      fprintf(stderr, "Failed to read '%s' from a header for '%s'\n", field.data(), key.c_str());
      ros1_services.erase(key);
      return;
    }
    ros1_services[key][field] = value;
  }
  std::string t = ros1_services[key]["type"];
  ros1_services[key]["package"] = std::string(t.begin(), t.begin() + t.find("/"));
  ros1_services[key]["name"] = std::string(t.begin() + t.find("/") + 1, t.end());
}

int main(int argc, char * argv[])
{
  bool output_topic_introspection;
  bool bridge_all_1to2_topics;
  bool bridge_all_2to1_topics;
  bool multi_threads;
  if (!parse_command_options(
      argc, argv, output_topic_introspection, bridge_all_1to2_topics, bridge_all_2to1_topics,
      multi_threads))
  {
    return 0;
  }

  // ROS 2 node
  rclcpp::init(argc, argv);

  auto ros2_node = rclcpp::Node::make_shared("ros_bridge");

  // ROS 1 node
  ros::init(argc, argv, "ros_bridge");
  ros::NodeHandle ros1_node;
  std::unique_ptr<ros::CallbackQueue> ros1_callback_queue = nullptr;
  if (multi_threads) {
    ros1_callback_queue = std::make_unique<ros::CallbackQueue>();
    ros1_node.setCallbackQueue(ros1_callback_queue.get());
  }
  
  ////////////
  // TOPIC CACHE
  ////////////
  // mapping of available topic names to type names
  std::map<std::string, std::string> ros1_publishers;
  std::map<std::string, std::string> ros1_subscribers;
  std::map<std::string, std::string> ros2_publishers;
  std::map<std::string, std::string> ros2_subscribers;
  std::map<std::string, std::map<std::string, std::string>> ros1_services;
  std::map<std::string, std::map<std::string, std::string>> ros2_services;

  std::map<std::string, Bridge1to2HandlesAndMessageTypes> bridges_1to2;
  std::map<std::string, Bridge2to1HandlesAndMessageTypes> bridges_2to1;
  std::list<ros1_bridge::BridgeHandles> all_parameter_based_handles;
  std::map<std::string, ros1_bridge::ServiceBridge1to2> service_bridges_1_to_2;
  std::map<std::string, ros1_bridge::ServiceBridge2to1> service_bridges_2_to_1;
  std::unordered_set<std::string> parameter_reserved_connections;


  //////////////////////////
  //
  // LOAD FIXED TOPICS AND SERVICES FROM PARAMETERS
  //
  //////////////////////////

  { // parameter-bridge scope for variable name safety

    // bridge all topics listed in a ROS 1 parameter
    // the topics parameter needs to be an array
    // and each item needs to be a dictionary with the following keys;
    // topic: the name of the topic to bridge (e.g. '/topic_name')
    // type: the type of the topic to bridge (e.g. 'pkgname/msg/MsgName')
    // queue_size: the queue size to use (default: 100)
    const char * topics_parameter_name = "topics";
    // the services parameters need to be arrays
    // and each item needs to be a dictionary with the following keys;
    // service: the name of the service to bridge (e.g. '/service_name')
    // type: the type of the service to bridge (e.g. 'pkgname/srv/SrvName')
    const char * services_1_to_2_parameter_name = "services_1_to_2";
    const char * services_2_to_1_parameter_name = "services_2_to_1";

    // Topics
    XmlRpc::XmlRpcValue topics;
    if (
      ros1_node.getParam(topics_parameter_name, topics) &&
      topics.getType() == XmlRpc::XmlRpcValue::TypeArray)
    {
      for (size_t i = 0; i < static_cast<size_t>(topics.size()); ++i) {
        std::string topic_name = static_cast<std::string>(topics[i]["topic"]);
        std::string type_name = static_cast<std::string>(topics[i]["type"]);
        size_t queue_size = static_cast<int>(topics[i]["queue_size"]);
        if (!queue_size) {
          queue_size = 100;
        }
        printf(
          "Trying to create bridge for topic '%s' "
          "with ROS 2 type '%s'\n",
          topic_name.c_str(), type_name.c_str());

        try {

          // Define default qos settings for parameter bridged topics
          auto qos_settings = rclcpp::QoS(rclcpp::KeepLast(queue_size));

          // Update the qos settings if the user has specified them
          if (topics[i].hasMember("qos")) {
            printf("Setting up QoS for '%s': ", topic_name.c_str());
            qos_settings = ros1_bridge::qos_from_params(topics[i]["qos"]);
            printf("\n");
          }

          // Check the bridge direction
          if (topics[i].hasMember("direction") 
            && topics[i]["direction"].getType() == XmlRpc::XmlRpcValue::TypeString
            && static_cast<std::string>(topics[i]["direction"]) == "1to2") { // ROS1 to ROS2

            printf("Parameter bridging topic in direction: 1to2 '%s': ", topic_name.c_str());

            ros1_bridge::BridgeHandles handles;

            handles.bridge1to2 = ros1_bridge::create_bridge_from_1_to_2(
              ros1_node, ros2_node, "", topic_name, queue_size, type_name, topic_name, qos_settings);

            all_parameter_based_handles.push_back(handles);

          } else if (topics[i].hasMember("direction") 
            && topics[i]["direction"].getType() == XmlRpc::XmlRpcValue::TypeString
            && static_cast<std::string>(topics[i]["direction"]) == "2to1") { // ROS2 to ROS1

            printf("Parameter bridging topic in direction: 2to1 '%s': ", topic_name.c_str());

            ros1_bridge::BridgeHandles handles;

            handles.bridge2to1 = ros1_bridge::create_bridge_from_2_to_1(
              ros2_node, ros1_node, type_name, topic_name, qos_settings, "", topic_name, queue_size);

            all_parameter_based_handles.push_back(handles);


          } else { // bi-directional

            printf("Parameter bridging topic in direction: both '%s': ", topic_name.c_str());

            ros1_bridge::BridgeHandles handles = ros1_bridge::create_bidirectional_bridge(
              ros1_node, ros2_node, "", type_name, topic_name, queue_size, qos_settings);
            all_parameter_based_handles.push_back(handles);
          }
        } catch (std::runtime_error & e) {
          fprintf(
            stderr,
            "failed to create bridge for topic '%s' "
            "with ROS 2 type '%s': %s\n",
            topic_name.c_str(), type_name.c_str(), e.what());
        }
      }
    } else {
      fprintf(
        stderr,
        "The parameter '%s' either doesn't exist or isn't an array\n", topics_parameter_name);
    }

    // ROS 1 Services in ROS 2
    XmlRpc::XmlRpcValue services_1_to_2;
    if (
      ros1_node.getParam(services_1_to_2_parameter_name, services_1_to_2) &&
      services_1_to_2.getType() == XmlRpc::XmlRpcValue::TypeArray)
    {
      for (size_t i = 0; i < static_cast<size_t>(services_1_to_2.size()); ++i) {
        std::string service_name = static_cast<std::string>(services_1_to_2[i]["service"]);
        std::string type_name = static_cast<std::string>(services_1_to_2[i]["type"]);
        {
          // for backward compatibility
          std::string package_name = static_cast<std::string>(services_1_to_2[i]["package"]);
          if (!package_name.empty()) {
            fprintf(
              stderr,
              "The service '%s' uses the key 'package' which is deprecated for "
              "services. Instead prepend the 'type' value with '<package>/'.\n",
              service_name.c_str());
            type_name = package_name + "/" + type_name;
          }
        }
        printf(
          "Trying to create bridge for ROS 2 service '%s' with type '%s'\n",
          service_name.c_str(), type_name.c_str());

        const size_t index = type_name.find("/");
        if (index == std::string::npos) {
          fprintf(
            stderr,
            "the service '%s' has a type '%s' without a slash.\n",
            service_name.c_str(), type_name.c_str());
          continue;
        }
        auto factory = ros1_bridge::get_service_factory(
          "ros2", type_name.substr(0, index), type_name.substr(index + 1));
        if (factory) {
          try {
            service_bridges_1_to_2[service_name] = factory->service_bridge_1_to_2(
                ros1_node, ros2_node, service_name);
            printf("Created 1 to 2 bridge for service %s\n", service_name.c_str());
          } catch (std::runtime_error & e) {
            fprintf(
              stderr,
              "failed to create bridge ROS 1 service '%s' with type '%s': %s\n",
              service_name.c_str(), type_name.c_str(), e.what());
          }
        } else {
          fprintf(
            stderr,
            "failed to create bridge ROS 1 service '%s' no conversion for type '%s'\n",
            service_name.c_str(), type_name.c_str());
        }
      }

    } else {
      fprintf(
        stderr,
        "The parameter '%s' either doesn't exist or isn't an array\n",
        services_1_to_2_parameter_name);
    }


    // ROS 2 Services in ROS 1
    XmlRpc::XmlRpcValue services_2_to_1;
    if (
      ros1_node.getParam(services_2_to_1_parameter_name, services_2_to_1) &&
      services_2_to_1.getType() == XmlRpc::XmlRpcValue::TypeArray) 
    {
      for (size_t i = 0; i < static_cast<size_t>(services_2_to_1.size()); ++i) {
        std::string service_name = static_cast<std::string>(services_2_to_1[i]["service"]);
        std::string type_name = static_cast<std::string>(services_2_to_1[i]["type"]);
        {
          // for backward compatibility
          std::string package_name = static_cast<std::string>(services_2_to_1[i]["package"]);
          if (!package_name.empty()) {
            fprintf(
              stderr,
              "The service '%s' uses the key 'package' which is deprecated for "
              "services. Instead prepend the 'type' value with '<package>/'.\n",
              service_name.c_str());
            type_name = package_name + "/" + type_name;
          }
        }
        printf(
          "Trying to create bridge for ROS 1 service '%s' with type '%s'\n",
          service_name.c_str(), type_name.c_str());

        const size_t index = type_name.find("/");
        if (index == std::string::npos) {
          fprintf(
            stderr,
            "the service '%s' has a type '%s' without a slash.\n",
            service_name.c_str(), type_name.c_str());
          continue;
        }

        auto factory = ros1_bridge::get_service_factory(
          "ros1", type_name.substr(0, index), type_name.substr(index + 1));
        if (factory) {
          try {
            service_bridges_2_to_1[service_name] = 
              factory->service_bridge_2_to_1(ros1_node, ros2_node, service_name);
            printf("Created 2 to 1 bridge for service %s\n", service_name.c_str());
          } catch (std::runtime_error & e) {
            fprintf(
              stderr,
              "failed to create bridge ROS 2 service '%s' with type '%s': %s\n",
              service_name.c_str(), type_name.c_str(), e.what());
          }
        } else {
          fprintf(
            stderr,
            "failed to create bridge ROS 2 service '%s' no conversion for type '%s'\n",
            service_name.c_str(), type_name.c_str());
        }
      }

    } else {
      fprintf(
        stderr,
        "The parameter '%s' either doesn't exist or isn't an array\n",
        services_2_to_1_parameter_name);
    }


    // Add the parameter topics to the reserved set
    for (const auto& handle :  all_parameter_based_handles) {
      // All parameter topics bridges are bi-directional so just get the ros1 topic name
      // and add it to the reserved set
      if (handle.bridge1to2.ros2_publisher) { // If the ros2_publisher is set then we can get the topic from the 1to2 handle
        parameter_reserved_connections.insert(handle.bridge1to2.ros1_subscriber.getTopic());
      } else { // Otherwise we can get the topic from the 2to1 handle
        parameter_reserved_connections.insert(handle.bridge2to1.ros1_publisher.getTopic());
      }

    }

    // Add parameter services from 1 to 2 to the reserved set 
    for (const auto& service : service_bridges_1_to_2) {
      parameter_reserved_connections.insert(service.first);
    }

    // Add parameter services from 2 to 1 to the reserved set 
    for (const auto& service : service_bridges_2_to_1) {
      parameter_reserved_connections.insert(service.first);
    }

  } // parameter-bridge scope

  /////////////////////////////////
  //
  // SETUP DYNAMIC TOPIC BRIDGING
  //
  /////////////////////////////////

  // setup polling of ROS 1 master
  auto ros1_poll = [
    &ros1_node, ros2_node,
    &ros1_publishers, &ros1_subscribers,
    &ros2_publishers, &ros2_subscribers,
    &bridges_1to2, &bridges_2to1,
    &ros1_services, &ros2_services,
    &service_bridges_1_to_2, &service_bridges_2_to_1,
    &output_topic_introspection,
    &bridge_all_1to2_topics, &bridge_all_2to1_topics,
    &parameter_reserved_connections,
    multi_threads
    ](const ros::TimerEvent &) -> void
    {
      // collect all topics names which have at least one publisher or subscriber beside this bridge
      std::set<std::string> active_publishers;
      std::set<std::string> active_subscribers;

      XmlRpc::XmlRpcValue args, result, payload;
      args[0] = ros::this_node::getName();
      if (!ros::master::execute("getSystemState", args, result, payload, true)) {
        fprintf(stderr, "failed to get system state from ROS 1 master\n");
        return;
      }
      // check publishers
      if (payload.size() >= 1) {
        for (int j = 0; j < payload[0].size(); ++j) {
          std::string topic_name = payload[0][j][0];
          for (int k = 0; k < payload[0][j][1].size(); ++k) {
            // ignore publishers for reserved topics
            if (parameter_reserved_connections.find(topic_name) != parameter_reserved_connections.end()) {
              continue;
            }
            std::string node_name = payload[0][j][1][k];
            // ignore publishers from the bridge itself
            if (node_name == ros::this_node::getName()) {
              continue;
            }
            active_publishers.insert(topic_name);
            break;
          }
        }
      }
      // check subscribers
      if (payload.size() >= 2) {
        for (int j = 0; j < payload[1].size(); ++j) {
          std::string topic_name = payload[1][j][0];
          for (int k = 0; k < payload[1][j][1].size(); ++k) {

            // ignore subscribers for reserved topics
            if (parameter_reserved_connections.find(topic_name) != parameter_reserved_connections.end()) {
              continue;
            }

            std::string node_name = payload[1][j][1][k];
            // ignore subscribers from the bridge itself
            if (node_name == ros::this_node::getName()) {
              continue;
            }
            active_subscribers.insert(topic_name);
            break;
          }
        }
      }

      // check services
      std::map<std::string, std::map<std::string, std::string>> active_ros1_services;
      if (payload.size() >= 3) {
        for (int j = 0; j < payload[2].size(); ++j) {
          if (payload[2][j][0].getType() == XmlRpc::XmlRpcValue::TypeString) {
            std::string name = payload[2][j][0];

            // ignore reserved services
            if (parameter_reserved_connections.find(name) != parameter_reserved_connections.end()) {
              continue;
            }

            get_ros1_service_info(name, active_ros1_services);
          }
        }
      }
      {
        std::lock_guard<std::mutex> lock(g_bridge_mutex);
        ros1_services = active_ros1_services;
      }

      // get message types for all topics
      ros::master::V_TopicInfo topics;
      bool success = ros::master::getTopics(topics);
      if (!success) {
        fprintf(stderr, "failed to poll ROS 1 master\n");
        return;
      }

      std::map<std::string, std::string> current_ros1_publishers;
      std::map<std::string, std::string> current_ros1_subscribers;
      for (auto topic : topics) {
        auto topic_name = topic.name;
        bool has_publisher = active_publishers.find(topic_name) != active_publishers.end();
        bool has_subscriber = active_subscribers.find(topic_name) != active_subscribers.end();
        if (!has_publisher && !has_subscriber) {
          // skip inactive topics
          continue;
        }
        if (has_publisher) {
          current_ros1_publishers[topic_name] = topic.datatype;
        }
        if (has_subscriber) {
          current_ros1_subscribers[topic_name] = topic.datatype;
        }
        if (output_topic_introspection) {
          printf(
            "  ROS 1: %s (%s) [%s pubs, %s subs]\n",
            topic_name.c_str(), topic.datatype.c_str(),
            has_publisher ? ">0" : "0", has_subscriber ? ">0" : "0");
        }
      }

      // since ROS 1 subscribers don't report their type they must be added anyway
      for (auto active_subscriber : active_subscribers) {
        if (current_ros1_subscribers.find(active_subscriber) == current_ros1_subscribers.end()) {
          current_ros1_subscribers[active_subscriber] = "";
          if (output_topic_introspection) {
            printf("  ROS 1: %s (<unknown>) sub++\n", active_subscriber.c_str());
          }
        }
      }

      if (output_topic_introspection) {
        printf("\n");
      }

      {
        std::lock_guard<std::mutex> lock(g_bridge_mutex);
        ros1_publishers = current_ros1_publishers;
        ros1_subscribers = current_ros1_subscribers;
      }

      update_bridge(
        ros1_node, ros2_node,
        ros1_publishers, ros1_subscribers,
        ros2_publishers, ros2_subscribers,
        ros1_services, ros2_services,
        bridges_1to2, bridges_2to1,
        service_bridges_1_to_2, service_bridges_2_to_1,
        parameter_reserved_connections,
        bridge_all_1to2_topics, bridge_all_2to1_topics,
        multi_threads);
    };

  auto ros1_poll_timer = ros1_node.createTimer(ros::Duration(1.0), ros1_poll);

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // setup polling of ROS 2
  std::set<std::string> already_ignored_topics;
  std::set<std::string> already_ignored_services;
  auto ros2_poll = [
    &ros1_node, ros2_node,
    &ros1_publishers, &ros1_subscribers,
    &ros2_publishers, &ros2_subscribers,
    &ros1_services, &ros2_services,
    &bridges_1to2, &bridges_2to1,
    &service_bridges_1_to_2, &service_bridges_2_to_1,
    &output_topic_introspection,
    &bridge_all_1to2_topics, &bridge_all_2to1_topics,
    &already_ignored_topics, &already_ignored_services,
    &parameter_reserved_connections,
    multi_threads
    ]() -> void
    {
      auto ros2_topics = ros2_node->get_topic_names_and_types();

      std::set<std::string> ignored_topics;
      ignored_topics.insert("parameter_events");
      ignored_topics.insert(parameter_reserved_connections.begin(), parameter_reserved_connections.end());

      std::map<std::string, std::string> current_ros2_publishers;
      std::map<std::string, std::string> current_ros2_subscribers;
      for (auto topic_and_types : ros2_topics) {
        // ignore some common ROS 2 specific topics
        if (ignored_topics.find(topic_and_types.first) != ignored_topics.end()) {
          continue;
        }

        auto & topic_name = topic_and_types.first;
        auto & topic_type = topic_and_types.second[0];  // explicitly take the first

        // explicitly avoid topics with more than one type
        if (topic_and_types.second.size() > 1) {
          if (already_ignored_topics.count(topic_name) == 0) {
            std::string types = "";
            for (auto type : topic_and_types.second) {
              types += type + ", ";
            }
            fprintf(
              stderr,
              "warning: ignoring topic '%s', which has more than one type: [%s]\n",
              topic_name.c_str(),
              types.substr(0, types.length() - 2).c_str()
            );
            already_ignored_topics.insert(topic_name);
          }
          continue;
        }

        auto publisher_count = ros2_node->count_publishers(topic_name);
        auto subscriber_count = ros2_node->count_subscribers(topic_name);

        // ignore publishers from the bridge itself
        if (bridges_1to2.find(topic_name) != bridges_1to2.end()) {
          if (publisher_count > 0) {
            --publisher_count;
          }
        }
        // ignore subscribers from the bridge itself
        if (bridges_2to1.find(topic_name) != bridges_2to1.end()) {
          if (subscriber_count > 0) {
            --subscriber_count;
          }
        }

        if (publisher_count) {
          current_ros2_publishers[topic_name] = topic_type;
        }

        if (subscriber_count) {
          current_ros2_subscribers[topic_name] = topic_type;
        }

        if (output_topic_introspection) {
          printf(
            "  ROS 2: %s (%s) [%zu pubs, %zu subs]\n",
            topic_name.c_str(), topic_type.c_str(), publisher_count, subscriber_count);
        }
      }

      // collect available services (not clients)
      std::set<std::string> service_names;
      std::vector<std::pair<std::string, std::string>> node_names_and_namespaces =
        ros2_node->get_node_graph_interface()->get_node_names_and_namespaces();
      for (auto & pair : node_names_and_namespaces) {
        if (pair.first == ros2_node->get_name() && pair.second == ros2_node->get_namespace()) {
          continue;
        }
        std::map<std::string, std::vector<std::string>> services_and_types =
          ros2_node->get_service_names_and_types_by_node(pair.first, pair.second);
        for (auto & it : services_and_types) {
          service_names.insert(it.first);
        }
      }

      auto ros2_services_and_types = ros2_node->get_service_names_and_types();
      std::map<std::string, std::map<std::string, std::string>> active_ros2_services;
      for (const auto & service_and_types : ros2_services_and_types) {
        auto & service_name = service_and_types.first;
        auto & service_type = service_and_types.second[0];  // explicitly take the first

        // ignore reserved services
        if (parameter_reserved_connections.find(service_name) != parameter_reserved_connections.end()) {
          continue;
        }

        // explicitly avoid services with more than one type
        if (service_and_types.second.size() > 1) {
          if (already_ignored_services.count(service_name) == 0) {
            std::string types = "";
            for (auto type : service_and_types.second) {
              types += type + ", ";
            }
            fprintf(
              stderr,
              "warning: ignoring service '%s', which has more than one type: [%s]\n",
              service_name.c_str(),
              types.substr(0, types.length() - 2).c_str()
            );
            already_ignored_services.insert(service_name);
          }
          continue;
        }

        // TODO(wjwwood): this should be common functionality in the C++ rosidl package
        size_t separator_position = service_type.find('/');
        if (separator_position == std::string::npos) {
          fprintf(stderr, "invalid service type '%s', skipping...\n", service_type.c_str());
          continue;
        }

        // only bridge if there is a service, not for a client
        if (service_names.find(service_name) != service_names.end()) {
          auto service_type_package_name = service_type.substr(0, separator_position);
          auto service_type_srv_name = service_type.substr(separator_position + 1);
          active_ros2_services[service_name]["package"] = service_type_package_name;
          active_ros2_services[service_name]["name"] = service_type_srv_name;
        }
      }

      {
        std::lock_guard<std::mutex> lock(g_bridge_mutex);
        ros2_services = active_ros2_services;
      }

      if (output_topic_introspection) {
        printf("\n");
      }

      {
        std::lock_guard<std::mutex> lock(g_bridge_mutex);
        ros2_publishers = current_ros2_publishers;
        ros2_subscribers = current_ros2_subscribers;
      }

      update_bridge(
        ros1_node, ros2_node,
        ros1_publishers, ros1_subscribers,
        ros2_publishers, ros2_subscribers,
        ros1_services, ros2_services,
        bridges_1to2, bridges_2to1,
        service_bridges_1_to_2, service_bridges_2_to_1,
        parameter_reserved_connections,
        bridge_all_1to2_topics, bridge_all_2to1_topics,
        multi_threads);
    };

  auto check_ros1_flag = [&ros1_node] {
      if (!ros1_node.ok()) {
        rclcpp::shutdown();
      }
    };

  auto ros2_poll_timer = ros2_node->create_wall_timer(
    std::chrono::seconds(1), [&ros2_poll, &check_ros1_flag] {
      ros2_poll();
      check_ros1_flag();
    });

  //////////////////////////////
  //
  // Setup Spinners
  //
  //////////////////////////////

  // ROS 1 asynchronous spinner
  std::unique_ptr<ros::AsyncSpinner> async_spinner = nullptr;
  if (!multi_threads) {
    RCLCPP_INFO_STREAM(ros2_node->get_logger(), "Using single-threaded ROS1 spinner");
    async_spinner = std::make_unique<ros::AsyncSpinner>(1);
  } else {
    RCLCPP_INFO_STREAM(ros2_node->get_logger(), "Using multi-threaded ROS1 spinner");
    async_spinner = std::make_unique<ros::AsyncSpinner>(0, ros1_callback_queue.get());
  }
  async_spinner->start();

  // ROS 2 spinning loop
  std::unique_ptr<rclcpp::Executor> executor = nullptr;
  if (!multi_threads) {
    RCLCPP_INFO_STREAM(ros2_node->get_logger(), "Using single-threaded ROS2 executor");
    executor = std::make_unique<rclcpp::executors::SingleThreadedExecutor>();
  } else {
    RCLCPP_INFO_STREAM(ros2_node->get_logger(), "Using multi-threaded ROS2 executor");
    executor = std::make_unique<rclcpp::executors::MultiThreadedExecutor>();
  }
  executor->add_node(ros2_node);
  executor->spin();

  return 0;
}
