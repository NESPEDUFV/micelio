#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "nsrs/include/runtime.h"
#include "ns3/node-list.h"
#include "ns3/node.h"
#include "ns3/simulator.h"
#include "ns3/tag.h"
#include "ns3/uinteger.h"
#include "nsrs/src/lib.rs.h"
#include <exception>
#include <iostream>
#pragma GCC diagnostic pop

#define DEBUG_VEC(V)                                                                               \
    std::cout << '[';                                                                              \
    for (auto item : V) {                                                                          \
        std::cout << item << ", ";                                                                 \
    };                                                                                             \
    std::cout << "]\n";

#define DEBUG_SOCKET(S) S << "(" << (S != nullptr ? S->GetNode()->GetId() : 0) << ")"
// #define DEBUG_OP(OP, T, S) std::cout << "[" << T << "(" << get_context() << ")][" << OP << "] this: " << DEBUG_SOCKET(S) << "\n"
// #define DEBUG_OP2(OP, T, S1, S2) std::cout << "[" << T << "(" << get_context() << ")][" << OP << "]" << (S1 == S2 ? '=' : '!') << " this: " << DEBUG_SOCKET(S1) << ", that: " << DEBUG_SOCKET(S2) << "\n"

// #define DEBUG_OP(OP, T, S) std::cout << "[" << T << "(" << get_context() << ")][" << OP << "]\n"
// #define DEBUG_OP2(OP, T, S1, S2) std::cout << "[" << T << "(" << get_context() << ")][" << OP << "]\n"

#define DEBUG_OP(OP, T, S) ;
#define DEBUG_OP2(OP, T, S1, S2) ;

namespace nsrs {

double now() { return ns3::Simulator::Now().GetSeconds(); }

void stop(double delay) { ns3::Simulator::Stop(ns3::Seconds(delay)); }

void stop_now() { stop(0); }

void schedule_awake(uintptr_t key, double dt) {
    auto ctx = ns3::Simulator::GetContext();
    ns3::Simulator::ScheduleWithContext(ctx, ns3::Seconds(dt), [key]() {
        wake(key);
        run();
    });
}

uint32_t get_context() { return ns3::Simulator::GetContext(); }

SocketAddr invalid_addr() {
    SocketAddr addr = {.version = IpAddrType::None, .host = "", .port = 0};
    return addr;
}

SocketAddr addr_from_ns3(ns3::Address addr) {
    if (ns3::Inet6SocketAddress::IsMatchingType(addr)) {
        return addr_from_ns3(ns3::Inet6SocketAddress::ConvertFrom(addr));
    } else {
        return addr_from_ns3(ns3::InetSocketAddress::ConvertFrom(addr));
    }
}

SocketAddr addr_from_ns3(ns3::InetSocketAddress addr) {
    std::stringstream ss;
    ss << addr.GetIpv4();
    SocketAddr addr_out;
    addr_out.version = IpAddrType::V4;
    addr_out.host    = ss.str();
    addr_out.port    = addr.GetPort();
    return addr_out;
}

SocketAddr addr_from_ns3(ns3::Inet6SocketAddress addr) {
    std::stringstream ss;
    ss << addr.GetIpv6();
    SocketAddr addr_out;
    addr_out.version = IpAddrType::V6;
    addr_out.host    = ss.str();
    addr_out.port    = addr.GetPort();
    return addr_out;
}

ns3::Address addr_to_ns3(SocketAddr addr) {
    if (addr.version == IpAddrType::V6) {
        ns3::Inet6SocketAddress addr_out(addr.host.c_str(), addr.port);
        return addr_out;
    } else {
        ns3::InetSocketAddress addr_out(addr.host.c_str(), addr.port);
        return addr_out;
    }
}

/// @brief Global `Node` lookup, with an optimization that assumes no node
/// deletion occurs at runtime. Uses a linear search in case `node_id` does not
/// match its index in the `NodeList`.
/// @param node_id The id returned by `Node::GetId()`
ns3::Ptr<ns3::Node> find_node_by_id(uint32_t node_id) {
    if (node_id < ns3::NodeList::GetNNodes()) {
        auto node = ns3::NodeList::GetNode(node_id);
        if (node != nullptr && node->GetId() == node_id) {
            return node;
        }
    }
    for (auto it = ns3::NodeList::Begin(); it != ns3::NodeList::End(); ++it) {
        if ((*it)->GetId() == node_id) {
            return *it;
        }
    }
    return nullptr;
}

#pragma region UDP

Ns3UdpSocket::Ns3UdpSocket(ns3::Ptr<ns3::Socket> ptr) : ptr(ptr), recv_key(0) {}

std::shared_ptr<Ns3UdpSocket> Ns3UdpSocket::create(uint32_t node_id) {
    auto tid  = ns3::TypeId::LookupByName("ns3::UdpSocketFactory");
    auto node = find_node_by_id(node_id);
    if (node == nullptr) {
        std::stringstream ss;
        ss << "could not find node with ID " << node_id;
        throw std::runtime_error(ss.str());
    }
    auto socket  = ns3::Socket::CreateSocket(node, tid);
    auto wrapper = std::make_shared<Ns3UdpSocket>(socket);
    socket->SetRecvCallback(ns3::MakeCallback(&Ns3UdpSocket::handle_recv, wrapper.get()));
    return wrapper;
}

void Ns3UdpSocket::bind(nsrs::SocketAddr addr_) {
    auto addr = addr_to_ns3(addr_);
    this->ptr->SetIpTos(0);
    if (this->ptr->Bind(addr) == -1) {
        std::stringstream ss;
        ss << "failed to bind socket";
        throw std::runtime_error(ss.str());
    }
}

void Ns3UdpSocket::connect(nsrs::SocketAddr addr_) {
    auto addr = addr_to_ns3(addr_);
    if (this->ptr->Connect(addr) != 0) {
        std::stringstream ss;
        ss << "failed to connect to " << addr_.host << ", port " << addr_.port;
        throw std::runtime_error(ss.str());
    }
}

int32_t Ns3UdpSocket::send(rust::Slice<const uint8_t> buf) {
    auto packet = ns3::Create<ns3::Packet>(buf.data(), buf.length());
    auto sent   = this->ptr->Send(packet);
    if (sent < 0) {
        std::stringstream ss;
        ss << "got ERRNO: " << this->ptr->GetErrno();
        throw std::runtime_error(ss.str());
    }
    return sent;
    // TODO: use SetSendCallback for situations when the buffer is full
}

int32_t Ns3UdpSocket::send_to(rust::Slice<const uint8_t> buf, nsrs::SocketAddr addr_) {
    auto addr   = addr_to_ns3(addr_);
    auto packet = ns3::Create<ns3::Packet>(buf.data(), buf.length());
    auto sent   = this->ptr->SendTo(packet, 0, addr);
    if (sent < 0) {
        std::stringstream ss;
        ss << "got ERRNO: " << this->ptr->GetErrno();
        throw std::runtime_error(ss.str());
    }
    return sent;
    // TODO: use SetSendCallback for situations when the buffer is full
}

int32_t Ns3UdpSocket::recv(rust::Slice<uint8_t> buf) {
    auto packet = this->ptr->Recv();
    if (packet == nullptr)
        return 0;
    packet->CopyData(buf.data(), buf.length());
    return packet->GetSize();
}

int32_t Ns3UdpSocket::recv_from(rust::Slice<uint8_t> buf, nsrs::SocketAddr &addr) {
    ns3::Address from;
    auto packet = this->ptr->RecvFrom(from);
    if (packet == nullptr)
        return 0;
    packet->CopyData(buf.data(), buf.length());
    addr = addr_from_ns3(from);
    return packet->GetSize();
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
void Ns3UdpSocket::handle_recv(ns3::Ptr<ns3::Socket> socket) {
    if (this->recv_key) {
        wake(this->recv_key);
        run();
    }
}
#pragma GCC diagnostic pop

void Ns3UdpSocket::set_recv_key(uintptr_t key) { this->recv_key = key; }

#pragma endregion UDP

#pragma region TCP

Ns3TcpSocket::Ns3TcpSocket(ns3::Ptr<ns3::Socket> ptr)
    : ptr(ptr), accept_keys(), accepted(), connect_key(0), connected_status(0), send_keys(),
      recv_keys(), sent_keys(), pending_bytes(0), closed(false) {
    this->ptr->SetCloseCallbacks(
        ns3::MakeCallback(&Ns3TcpSocket::handle_close_ok, this),
        ns3::MakeCallback(&Ns3TcpSocket::handle_close_err, this)
    );
    this->ptr->SetSendCallback(ns3::MakeCallback(&Ns3TcpSocket::handle_send, this));
    this->ptr->SetDataSentCallback(ns3::MakeCallback(&Ns3TcpSocket::handle_sent, this));
    this->ptr->SetRecvCallback(ns3::MakeCallback(&Ns3TcpSocket::handle_recv, this));
}

std::unique_ptr<Ns3TcpSocket> Ns3TcpSocket::create(uint32_t node_id) {
    auto tid  = ns3::TypeId::LookupByName("ns3::TcpSocketFactory");
    auto node = find_node_by_id(node_id);
    if (node == nullptr) {
        std::stringstream ss;
        ss << "could not find node with ID " << node_id;
        throw std::runtime_error(ss.str());
    }
    auto socket  = ns3::Socket::CreateSocket(node, tid);
    auto wrapper = std::make_unique<Ns3TcpSocket>(socket);
    return wrapper;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
bool tcp_accept(ns3::Ptr<ns3::Socket> _socket, const ns3::Address &_from) { return true; }
#pragma GCC diagnostic pop

void Ns3TcpSocket::bind(nsrs::SocketAddr addr_) {
    DEBUG_OP("Ns3TcpSocket::bind", this, this->ptr);
    auto addr = addr_to_ns3(addr_);
    this->ptr->SetIpTos(0);
    if (this->ptr->Bind(addr) == -1) {
        std::stringstream ss;
        ss << "failed to bind socket";
        throw std::runtime_error(ss.str());
    }
    if (this->ptr->Listen() == -1) {
        std::stringstream ss;
        ss << "failed to bind socket";
        throw std::runtime_error(ss.str());
    }

    this->ptr->SetAcceptCallback(
        ns3::MakeCallback(tcp_accept), ns3::MakeCallback(&Ns3TcpSocket::handle_accept, this)
    );
}

void Ns3TcpSocket::connect(nsrs::SocketAddr addr_) {
    DEBUG_OP("Ns3TcpSocket::connect", this, this->ptr);
    this->ptr->SetConnectCallback(
        ns3::MakeCallback(&Ns3TcpSocket::handle_connect_ok, this),
        ns3::MakeCallback(&Ns3TcpSocket::handle_connect_err, this)
    );
    auto addr = addr_to_ns3(addr_);
    int ok    = 0;
    if (ns3::Inet6SocketAddress::IsMatchingType(addr)) {
        ok = this->ptr->Bind6();
    } else {
        ok = this->ptr->Bind();
    }
    if (ok != 0) {
        std::stringstream ss;
        ss << "failed to bind socket";
        throw std::runtime_error(ss.str());
    }
    if (this->ptr->Connect(addr) != 0) {
        auto e = this->ptr->GetErrno();
        std::stringstream ss;
        ss << "failed to connect to peer " << addr_.host << ", port " << addr_.port << " (errno: " << e << ")";
        throw std::runtime_error(ss.str());
    }
}

SocketAddr Ns3TcpSocket::local_addr() const {
    DEBUG_OP("Ns3TcpSocket::local_addr", this, this->ptr);
    ns3::Address addr_;
    if (this->ptr->GetSockName(addr_) != 0)
        return invalid_addr();
    auto addr = addr_from_ns3(addr_);
    return addr;
}

SocketAddr Ns3TcpSocket::peer_addr() const {
    DEBUG_OP("Ns3TcpSocket::peer_addr", this, this->ptr);
    ns3::Address addr_;
    if (this->ptr->GetPeerName(addr_) != 0)
        return invalid_addr();
    auto addr = addr_from_ns3(addr_);
    return addr;
}

SocketErrno Ns3TcpSocket::get_errno() const {
    DEBUG_OP("Ns3TcpSocket::get_errno", this, this->ptr);
    if (this->closed)
        return SocketErrno::ERROR_SHUTDOWN;
    return (SocketErrno)(unsigned char)this->ptr->GetErrno();
}

uint32_t Ns3TcpSocket::get_nodeid() const { return this->ptr->GetNode()->GetId(); }

int32_t Ns3TcpSocket::send(rust::Slice<const uint8_t> buf) {
    DEBUG_OP("Ns3TcpSocket::send", this, this->ptr);
    auto accepted = this->ptr->Send(buf.data(), (uint32_t)buf.size(), 0);
    if (accepted < 0) {
        std::stringstream ss;
        ss << "failed to accept bytes to send, errno " << this->ptr->GetErrno();
        throw std::runtime_error(ss.str());
    }
    this->pending_bytes += accepted;
    return accepted;
}

int32_t Ns3TcpSocket::recv(rust::Slice<uint8_t> buf) {
    DEBUG_OP("Ns3TcpSocket::recv", this, this->ptr);
    auto n = this->ptr->Recv(buf.data(), buf.length(), 0);
    if (this->ptr->GetErrno() != 0)
        throw std::runtime_error("failed to recv");
    if (n == 0) {
        if (this->closed)
            throw std::runtime_error("closed");
        return 0;
    }
    return n;
}

void Ns3TcpSocket::close() {
    DEBUG_OP("Ns3TcpSocket::close", this, this->ptr);
    if (this->ptr->Close() != 0) {
        std::stringstream ss;
        ss << "failed to close, errno " << this->ptr->GetErrno();
        throw std::runtime_error(ss.str());
    }
}

void Ns3TcpSocket::clear_callbacks() {
    this->ptr->SetCloseCallbacks(
        ns3::MakeNullCallback<void, ns3::Ptr<ns3::Socket>>(),
        ns3::MakeNullCallback<void, ns3::Ptr<ns3::Socket>>()
    );
    this->ptr->SetSendCallback(ns3::MakeNullCallback<void, ns3::Ptr<ns3::Socket>, uint32_t>());
    this->ptr->SetDataSentCallback(ns3::MakeNullCallback<void, ns3::Ptr<ns3::Socket>, uint32_t>());
    this->ptr->SetRecvCallback(ns3::MakeNullCallback<void, ns3::Ptr<ns3::Socket>>());
    this->ptr->SetAcceptCallback(
        ns3::MakeNullCallback<bool, ns3::Ptr<ns3::Socket>, const ns3::Address &>(),
        ns3::MakeNullCallback<void, ns3::Ptr<ns3::Socket>, const ns3::Address &>()
    );
    this->ptr->SetConnectCallback(
        ns3::MakeNullCallback<void, ns3::Ptr<ns3::Socket>>(),
        ns3::MakeNullCallback<void, ns3::Ptr<ns3::Socket>>()
    );
}

void Ns3TcpSocket::push_accept_key(uintptr_t key) { this->accept_keys.push_back(key); }

void Ns3TcpSocket::push_connect_key(uintptr_t key) { this->connect_key = key; }

void Ns3TcpSocket::push_send_key(uintptr_t key) { this->send_keys.push_back(key); }

void Ns3TcpSocket::push_recv_key(uintptr_t key) { this->recv_keys.push_back(key); }

void Ns3TcpSocket::push_sent_key(uintptr_t key) { this->sent_keys.push_back(key); }

std::unique_ptr<Ns3TcpSocket> Ns3TcpSocket::pop_accepted() {
    if (this->accepted.empty())
        return nullptr;
    ns3::Ptr<ns3::Socket> socket = this->accepted.front();
    this->accepted.pop_front();
    auto wrapper = std::make_unique<Ns3TcpSocket>(socket);
    return wrapper;
}

uintptr_t Ns3TcpSocket::get_pending() const { return this->pending_bytes; }

int8_t Ns3TcpSocket::get_connected_status() const { return this->connected_status; }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
void Ns3TcpSocket::handle_accept(ns3::Ptr<ns3::Socket> socket, const ns3::Address &from) {
    DEBUG_OP2("Ns3TcpSocket::handle_accept", this, this->ptr, socket);
    this->accepted.push_back(socket);
    if (this->accept_keys.empty())
        return;
    auto key = this->accept_keys.front();
    wake(key);
    this->accept_keys.pop_front();
    run();
}

void Ns3TcpSocket::handle_connect_ok(ns3::Ptr<ns3::Socket> socket) {
    DEBUG_OP2("Ns3TcpSocket::handle_connect_ok", this, this->ptr, socket);
    this->ptr              = socket;
    this->connected_status = 1;
    if (this->connect_key) {
        wake(this->connect_key);
        run();
    }
}

void Ns3TcpSocket::handle_connect_err(ns3::Ptr<ns3::Socket> socket) {
    DEBUG_OP2("Ns3TcpSocket::handle_connect_err", this, this->ptr, socket);
    this->ptr              = socket;
    this->connected_status = -1;
    if (this->connect_key) {
        wake(this->connect_key);
        run();
    }
}

void Ns3TcpSocket::handle_send(ns3::Ptr<ns3::Socket> socket, uint32_t _bytes) {
    DEBUG_OP2("Ns3TcpSocket::handle_send", this, this->ptr, socket);
    this->ptr = socket;
    if (this->send_keys.empty())
        return;
    auto key = this->send_keys.front();
    wake(key);
    this->send_keys.pop_front();
    run();
}

void Ns3TcpSocket::handle_sent(ns3::Ptr<ns3::Socket> socket, uint32_t bytes) {
    DEBUG_OP2("Ns3TcpSocket::handle_sent", this, this->ptr, socket);
    this->ptr = socket;
    if ((this->pending_bytes -= bytes) > 0) {
        return;
    }
    if (this->sent_keys.empty())
        return;
    auto key = this->sent_keys.front();
    wake(key);
    this->sent_keys.pop_front();
    run();
}

void Ns3TcpSocket::handle_recv(ns3::Ptr<ns3::Socket> socket) {
    DEBUG_OP2("Ns3TcpSocket::handle_recv", this, this->ptr, socket);
    this->ptr = socket;
    if (this->recv_keys.empty())
        return;
    auto key = this->recv_keys.front();
    wake(key);
    this->recv_keys.pop_front();
    run();
}

void Ns3TcpSocket::handle_close_ok(ns3::Ptr<ns3::Socket> socket) {
    DEBUG_OP2("Ns3TcpSocket::handle_close_ok", this, this->ptr, socket);
    // this->ptr = socket;
    // DEBUG_VEC(this->recv_keys);
    this->closed = true;
    while (!this->recv_keys.empty()) {
        auto key = this->recv_keys.front();
        wake(key);
        this->recv_keys.pop_front();
    }
    run();
}

void Ns3TcpSocket::handle_close_err(ns3::Ptr<ns3::Socket> socket) {
    DEBUG_OP2("Ns3TcpSocket::handle_close_err", this, this->ptr, socket);
    // this->ptr = socket;
    // DEBUG_VEC(this->recv_keys);
    this->closed = true;
    while (!this->recv_keys.empty()) {
        auto key = this->recv_keys.front();
        wake(key);
        this->recv_keys.pop_front();
    }
    run();
}
#pragma GCC diagnostic pop

#pragma endregion TCP

} // namespace nsrs
