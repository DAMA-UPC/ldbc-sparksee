#include "Remote.h"

#include <iostream>
#include <thread>
#include <string>
//#include <codecvt>
#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/locale.hpp>

#include "server/Worker.h"
#include "server/QueryStatistics.h"

namespace ldbc_server {
namespace query_strategy {
Remote::Remote(sparksee::Database *database, unsigned int port,
               unsigned int shutdown_port)
    : acceptor_(io_service_, boost::asio::ip::tcp::endpoint(
                                 boost::asio::ip::tcp::v4(), static_cast<unsigned short>(port))),
      shutdown_(io_service_, boost::asio::ip::tcp::endpoint(
                                 boost::asio::ip::tcp::v4(), static_cast<unsigned short>(shutdown_port))),
      serverup_(io_service_, boost::asio::ip::tcp::endpoint(
                                 boost::asio::ip::tcp::v4(), static_cast<unsigned short>(9997)))
      {
  database_ = database;
  end_ = false;
  std::thread shutdown_thread([this]() { this->shutdown_wait(); });
  shutdown_thread.detach();
  std::thread up_thread([this]() { this->server_start_conn(); });
  up_thread.detach();
}

void Remote::shutdown_wait() {
  boost::asio::ip::tcp::socket socket(io_service_);
  shutdown_.accept(socket);
  std::cout << "Received shutdown signal" << std::endl;
  end_ = true;
  acceptor_.close();
}

void Remote::server_start_conn() {
  boost::asio::ip::tcp::socket socket(io_service_);
  while(!end_) {
    serverup_.accept(socket);
    std::cout << "Received status connection" << std::endl;
  }
  serverup_.close();
}

std::map<std::string, std::vector<std::string>> Remote::next_parameter(int id) {
  return parameters_[static_cast<size_t>(id)][parameters_[static_cast<size_t>(id)].size() - 1];
}

Worker::Task Remote::next() {
  try {
    auto socket = std::make_shared<boost::asio::ip::tcp::socket>(io_service_);
//    clock_t start = clock();
    acceptor_.accept(*socket);
    std::array<char, 8192> buffer{};
    boost::system::error_code error;
    socket->read_some(boost::asio::buffer(buffer), error);
    std::string data(buffer.data());
    //std::cout << "QUERY RECEIVED" << std::endl;
    //std::cout << data << std::endl;
    std::stringstream stream(data);
    boost::property_tree::ptree tree = read_json(stream);
    populate_parameter_list(tree);
    int id = static_cast<int>(get_first_id(tree));
    std::map<std::string, std::vector<std::string>> parameters = next_parameter(id);
    Worker::Task task = function_factory(parameters, std::bind([=](const datatypes::Buffer &query_result) {
                               boost::system::error_code send_error;
                               if(query_result.buffer_type_ == datatypes::E_STRING) {
                                  if(query_result.buffer_length_ > 0) {

                                  boost::locale::generator g;
                                  g.locale_cache_enabled(true);
                                  std::locale loc = g(boost::locale::util::get_system_locale()); 
                                  std::string utf8str = boost::locale::conv::to_utf<char>(query_result.buffer_, loc);
                                  std::vector<char> send_buffer(utf8str.begin(), utf8str.end());
                                  //socket->write_some(boost::asio::buffer(send_buffer), send_error);
                                  boost::asio::write(*socket, boost::asio::buffer(send_buffer), send_error);
                                  } 
                                } else { 
                                std::vector<char> send_buffer(query_result.buffer_, query_result.buffer_ + query_result.buffer_length_);
                                //socket->write_some(boost::asio::buffer(send_buffer), send_error);
                                  boost::asio::write(*socket, boost::asio::buffer(send_buffer), send_error);
                               }
                               socket->shutdown(boost::asio::ip::tcp::socket::shutdown_send);
                               socket->close();
                             },
                             std::placeholders::_1));
    return task;
  }
  catch (std::exception &e) {
    std::cerr << "Remote error " << e.what() << std::endl;
    exit(-1); // NOTE: I do not want to implement an empty worker task. If someone needs to be less faulty tolerant implement one.
  }
}
}
}
