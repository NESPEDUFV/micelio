#include "edge.h"
#include "ns3/inet-socket-address.h"
#include "ns3/inet6-socket-address.h"
#include "ns3/ipv4-address.h"
#include "ns3/ipv6-address.h"
#include "ns3/log.h"
#include "ns3/nstime.h"
#include "ns3/packet.h"
#include "ns3/simulator.h"
#include "ns3/socket-factory.h"
#include "ns3/socket.h"
#include "ns3/trace-source-accessor.h"
#include "ns3/uinteger.h"
#include <cstring>

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("EdgeApp");

NS_OBJECT_ENSURE_REGISTERED(EdgeApp);

TypeId EdgeApp::GetTypeId() {
    static TypeId tid =
        TypeId("ns3::EdgeApp")
            .SetParent<Application>()
            .AddConstructor<EdgeApp>()
            .AddTraceSource(
                "TxWithAddresses",
                "A new packet is created and is sent",
                MakeTraceSourceAccessor(&EdgeApp::m_txTraceWithAddresses),
                "ns3::Packet::TwoAddressTracedCallback"
            )
            .AddTraceSource(
                "RxWithAddresses",
                "A packet has been received",
                MakeTraceSourceAccessor(&EdgeApp::m_rxTraceWithAddresses),
                "ns3::Packet::TwoAddressTracedCallback"
            );
    return tid;
}

EdgeApp::EdgeApp() { NS_LOG_FUNCTION(this); }

EdgeApp::~EdgeApp() { NS_LOG_FUNCTION(this); }

void EdgeApp::StartApplication() {
    NS_LOG_FUNCTION(this);
    micelio::EdgeApp::spawn(*this->sim_params, this->params);
    nsrs::run();
}

void EdgeApp::StopApplication() { NS_LOG_FUNCTION(this); }

void EdgeApp::SetParams(micelio::SimulationParams *sim_params, micelio::EdgeAppParams params) {
    this->sim_params = sim_params;
    this->params = params;
}


} // Namespace ns3