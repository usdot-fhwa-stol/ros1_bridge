#pragma once


#include <xmlrpcpp/XmlRpcException.h>

#include "rclcpp/rclcpp.hpp"

#include "ros1_bridge/bridge.hpp"

#include <list>
#include <string>

namespace ros1_bridge {

inline rclcpp::QoS qos_from_params(XmlRpc::XmlRpcValue qos_params)
{
  auto ros2_publisher_qos = rclcpp::QoS(rclcpp::KeepLast(10));

  printf("Qos(");

  if (qos_params.getType() == XmlRpc::XmlRpcValue::TypeStruct) {
    if (qos_params.hasMember("history")) {
      auto history = static_cast<std::string>(qos_params["history"]);
      printf("history: ");
      if (history == "keep_all") {
        ros2_publisher_qos.keep_all();
        printf("keep_all, ");
      } else if (history == "keep_last") {
        if (qos_params.hasMember("depth")) {
          auto depth = static_cast<int>(qos_params["depth"]);
          ros2_publisher_qos.keep_last(depth);
          printf("keep_last(%i), ", depth);
        } else {
          fprintf(
            stderr,
            "history: keep_last requires that also a depth is set\n");
        }
      } else {
        fprintf(
          stderr,
          "invalid value for 'history': '%s', allowed values are 'keep_all',"
          "'keep_last' (also requires 'depth' to be set)\n",
          history.c_str());
      }
    }

    if (qos_params.hasMember("reliability")) {
      auto reliability = static_cast<std::string>(qos_params["reliability"]);
      printf("reliability: ");
      if (reliability == "best_effort") {
        ros2_publisher_qos.best_effort();
        printf("best_effort, ");
      } else if (reliability == "reliable") {
        ros2_publisher_qos.reliable();
        printf("reliable, ");
      } else {
        fprintf(
          stderr,
          "invalid value for 'reliability': '%s', allowed values are 'best_effort', 'reliable'\n",
          reliability.c_str());
      }
    }

    if (qos_params.hasMember("durability")) {
      auto durability = static_cast<std::string>(qos_params["durability"]);
      printf("durability: ");
      if (durability == "transient_local") {
        ros2_publisher_qos.transient_local();
        printf("transient_local, ");
      } else if (durability == "volatile") {
        ros2_publisher_qos.durability_volatile();
        printf("volatile, ");
      } else {
        fprintf(
          stderr,
          "invalid value for 'durability': '%s', allowed values are 'best_effort', 'volatile'\n",
          durability.c_str());
      }
    }

    if (qos_params.hasMember("deadline")) {
      try {
        rclcpp::Duration dur = rclcpp::Duration(
          static_cast<int>(qos_params["deadline"]["secs"]),
          static_cast<int>(qos_params["deadline"]["nsecs"]));
        ros2_publisher_qos.deadline(dur);
        printf("deadline: Duration(nsecs: %ld), ", dur.nanoseconds());
      } catch (std::runtime_error & e) {
        fprintf(
          stderr,
          "failed to parse deadline: '%s'\n",
          e.what());
      } catch (XmlRpc::XmlRpcException & e) {
        fprintf(
          stderr,
          "failed to parse deadline: '%s'\n",
          e.getMessage().c_str());
      }
    }

    if (qos_params.hasMember("lifespan")) {
      try {
        rclcpp::Duration dur = rclcpp::Duration(
          static_cast<int>(qos_params["lifespan"]["secs"]),
          static_cast<int>(qos_params["lifespan"]["nsecs"]));
        ros2_publisher_qos.lifespan(dur);
        printf("lifespan: Duration(nsecs: %ld), ", dur.nanoseconds());
      } catch (std::runtime_error & e) {
        fprintf(
          stderr,
          "failed to parse lifespan: '%s'\n",
          e.what());
      } catch (XmlRpc::XmlRpcException & e) {
        fprintf(
          stderr,
          "failed to parse lifespan: '%s'\n",
          e.getMessage().c_str());
      }
    }

    if (qos_params.hasMember("liveliness")) {
      if (qos_params["liveliness"].getType() == XmlRpc::XmlRpcValue::TypeInt) {
        try {
          auto liveliness = static_cast<int>(qos_params["liveliness"]);
          ros2_publisher_qos.liveliness(static_cast<rmw_qos_liveliness_policy_t>(liveliness));
          printf("liveliness: %i, ", static_cast<int>(liveliness));
        } catch (std::runtime_error & e) {
          fprintf(
            stderr,
            "failed to parse liveliness: '%s'\n",
            e.what());
        } catch (XmlRpc::XmlRpcException & e) {
          fprintf(
            stderr,
            "failed to parse liveliness: '%s'\n",
            e.getMessage().c_str());
        }
      } else if (qos_params["liveliness"].getType() == XmlRpc::XmlRpcValue::TypeString) {
        try {
          rmw_qos_liveliness_policy_t liveliness =
            rmw_qos_liveliness_policy_t::RMW_QOS_POLICY_LIVELINESS_SYSTEM_DEFAULT;
          auto liveliness_str = static_cast<std::string>(qos_params["liveliness"]);
          if (liveliness_str == "LIVELINESS_SYSTEM_DEFAULT" ||
            liveliness_str == "liveliness_system_default")
          {
            liveliness = rmw_qos_liveliness_policy_t::RMW_QOS_POLICY_LIVELINESS_SYSTEM_DEFAULT;
          } else if (liveliness_str == "LIVELINESS_AUTOMATIC" ||  // NOLINT
            liveliness_str == "liveliness_automatic")
          {
            liveliness = rmw_qos_liveliness_policy_t::RMW_QOS_POLICY_LIVELINESS_AUTOMATIC;
          } else if (liveliness_str == "LIVELINESS_MANUAL_BY_TOPIC" ||  // NOLINT
            liveliness_str == "liveliness_manual_by_topic")
          {
            liveliness = rmw_qos_liveliness_policy_t::RMW_QOS_POLICY_LIVELINESS_MANUAL_BY_TOPIC;
          } else {
            fprintf(
              stderr,
              "invalid value for 'liveliness': '%s', allowed values are "
              "LIVELINESS_{SYSTEM_DEFAULT, AUTOMATIC, MANUAL_BY_TOPIC}, upper or lower case\n",
              liveliness_str.c_str());
          }

          ros2_publisher_qos.liveliness(liveliness);
          printf("liveliness: %s, ", liveliness_str.c_str());
        } catch (std::runtime_error & e) {
          fprintf(
            stderr,
            "failed to parse liveliness: '%s'\n",
            e.what());
        } catch (XmlRpc::XmlRpcException & e) {
          fprintf(
            stderr,
            "failed to parse liveliness: '%s'\n",
            e.getMessage().c_str());
        }
      } else {
        fprintf(
          stderr,
          "failed to parse liveliness, parameter was not a string or int \n");
      }
    }

    if (qos_params.hasMember("liveliness_lease_duration")) {
      try {
        rclcpp::Duration dur = rclcpp::Duration(
          static_cast<int>(qos_params["liveliness_lease_duration"]["secs"]),
          static_cast<int>(qos_params["liveliness_lease_duration"]["nsecs"]));
        ros2_publisher_qos.liveliness_lease_duration(dur);
        printf("liveliness_lease_duration: Duration(nsecs: %ld), ", dur.nanoseconds());
      } catch (std::runtime_error & e) {
        fprintf(
          stderr,
          "failed to parse liveliness_lease_duration: '%s'\n",
          e.what());
      } catch (XmlRpc::XmlRpcException & e) {
        fprintf(
          stderr,
          "failed to parse liveliness_lease_duration: '%s'\n",
          e.getMessage().c_str());
      }
    }
  } else {
    fprintf(
      stderr,
      "QoS parameters could not be read\n");
  }

  printf(")");
  return ros2_publisher_qos;
}

} // ros1_bridge