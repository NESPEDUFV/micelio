#include "cloud.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("CloudApp");

NS_OBJECT_ENSURE_REGISTERED(CloudApp);

TypeId CloudApp::GetTypeId() {
    static TypeId tid =
        TypeId("ns3::CloudApp")
            .SetParent<Application>()
            .AddConstructor<CloudApp>()
            .AddTraceSource(
                "RxWithAddresses",
                "A packet has been received",
                MakeTraceSourceAccessor(&CloudApp::m_rxTraceWithAddresses),
                "ns3::Packet::TwoAddressTracedCallback"
            )
            .AddTraceSource(
                "TxWithAddresses",
                "A packet has been sent",
                MakeTraceSourceAccessor(&CloudApp::m_txTraceWithAddresses),
                "ns3::Packet::TwoAddressTracedCallback"
            );
    return tid;
}

CloudApp::CloudApp() { NS_LOG_FUNCTION(this); }

CloudApp::~CloudApp() { NS_LOG_FUNCTION(this); }

void CloudApp::StartApplication() {
    NS_LOG_FUNCTION(this);
    micelio::CloudApp::spawn(*this->sim_params, this->params);
    nsrs::run();
}

void CloudApp::StopApplication() { NS_LOG_FUNCTION(this); }

void CloudApp::SetParams(micelio::SimulationParams *sim_params, micelio::CloudAppParams params) {
    this->params = params;
    this->sim_params = sim_params;
}

} // Namespace ns3