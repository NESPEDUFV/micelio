#ifndef NS3_RUNTIME_H
#define NS3_RUNTIME_H

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "ns3/socket.h"
#include "rust/cxx.h"
#include <cstdint>
#include <deque>
#pragma GCC diagnostic pop

namespace nsrs {

double now();
void stop(double delay);
void stop_now();
void schedule_awake(uintptr_t key, double dt);
uint32_t get_context();

struct SocketAddr;
enum class SocketErrno : uint8_t;

SocketAddr addr_from_ns3(ns3::Address addr);
SocketAddr addr_from_ns3(ns3::InetSocketAddress addr);
SocketAddr addr_from_ns3(ns3::Inet6SocketAddress addr);
ns3::Address addr_to_ns3(SocketAddr addr);

class Ns3UdpSocket {
public:
    Ns3UdpSocket(ns3::Ptr<ns3::Socket> ptr);
    static std::shared_ptr<Ns3UdpSocket> create(uint32_t node_id);
    void bind(nsrs::SocketAddr addr);
    void connect(nsrs::SocketAddr addr);
    int32_t send(rust::Slice<const uint8_t> buf);
    int32_t send_to(rust::Slice<const uint8_t> buf, nsrs::SocketAddr addr);
    int32_t recv(rust::Slice<uint8_t> buf);
    int32_t recv_from(rust::Slice<uint8_t> buf, nsrs::SocketAddr &addr);

    void set_recv_key(uintptr_t key);
private:
    ns3::Ptr<ns3::Socket> ptr;
    uintptr_t recv_key;

    void handle_recv(ns3::Ptr<ns3::Socket> socket);
};

class Ns3TcpSocket {
public:
    Ns3TcpSocket(ns3::Ptr<ns3::Socket> ptr);
    static std::unique_ptr<Ns3TcpSocket> create(uint32_t node_id);
    void bind(nsrs::SocketAddr addr);
    void connect(nsrs::SocketAddr addr);
    nsrs::SocketAddr local_addr() const;
    nsrs::SocketAddr peer_addr() const;
    nsrs::SocketErrno get_errno() const;
    uint32_t get_nodeid() const;
    int32_t send(rust::Slice<const uint8_t> buf);
    int32_t recv(rust::Slice<uint8_t> buf);
    void close();
    void clear_callbacks();
    
    void push_accept_key(uintptr_t key);
    void push_connect_key(uintptr_t key);
    void push_send_key(uintptr_t key);
    void push_recv_key(uintptr_t key);
    void push_sent_key(uintptr_t key);
    std::unique_ptr<Ns3TcpSocket> pop_accepted();
    uintptr_t get_pending() const;
    int8_t get_connected_status() const;
private:
    ns3::Ptr<ns3::Socket> ptr;
    std::deque<uintptr_t> accept_keys;
    std::deque<ns3::Ptr<ns3::Socket>> accepted;
    uintptr_t connect_key;
    int8_t connected_status;
    std::deque<uintptr_t> send_keys;
    std::deque<uintptr_t> recv_keys;
    std::deque<uintptr_t> sent_keys;
    uintptr_t pending_bytes;
    bool closed;

    void handle_accept(ns3::Ptr<ns3::Socket> socket, const ns3::Address &from);
    void handle_connect_ok(ns3::Ptr<ns3::Socket> socket);
    void handle_connect_err(ns3::Ptr<ns3::Socket> socket);
    void handle_send(ns3::Ptr<ns3::Socket> socket, uint32_t bytes);
    void handle_sent(ns3::Ptr<ns3::Socket> socket, uint32_t bytes);
    void handle_recv(ns3::Ptr<ns3::Socket> socket);
    void handle_close_ok(ns3::Ptr<ns3::Socket> socket);
    void handle_close_err(ns3::Ptr<ns3::Socket> socket);
};

} // namespace nsrs

#endif