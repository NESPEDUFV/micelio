#include "fog.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("FogApp");

NS_OBJECT_ENSURE_REGISTERED(FogApp);

TypeId FogApp::GetTypeId() {
    static TypeId tid =
        TypeId("ns3::FogApp")
            .SetParent<Application>()
            .AddConstructor<FogApp>()
            .AddTraceSource(
                "RxWithAddresses",
                "A packet has been received",
                MakeTraceSourceAccessor(&FogApp::m_rxTraceWithAddresses),
                "ns3::Packet::TwoAddressTracedCallback"
            )
            .AddTraceSource(
                "TxWithAddresses",
                "A packet has been sent",
                MakeTraceSourceAccessor(&FogApp::m_txTraceWithAddresses),
                "ns3::Packet::TwoAddressTracedCallback"
            );
    return tid;
}

FogApp::FogApp() { NS_LOG_FUNCTION(this); }

FogApp::~FogApp() { NS_LOG_FUNCTION(this); }

void FogApp::StartApplication() {
    NS_LOG_FUNCTION(this);
    micelio::FogApp::spawn(*this->sim_params, this->params);
    nsrs::run();
}

void FogApp::StopApplication() { NS_LOG_FUNCTION(this); }

void FogApp::SetParams(micelio::SimulationParams *sim_params, micelio::FogAppParams params) {
    this->params = params;
    this->sim_params = sim_params;
}

} // Namespace ns3