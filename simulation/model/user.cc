#include "user.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("UserApp");

NS_OBJECT_ENSURE_REGISTERED(UserApp);

TypeId UserApp::GetTypeId() {
    static TypeId tid =
        TypeId("ns3::UserApp")
            .SetParent<Application>()
            .AddConstructor<UserApp>()
            .AddTraceSource(
                "RxWithAddresses",
                "A packet has been received",
                MakeTraceSourceAccessor(&UserApp::m_rxTraceWithAddresses),
                "ns3::Packet::TwoAddressTracedCallback"
            )
            .AddTraceSource(
                "TxWithAddresses",
                "A packet has been sent",
                MakeTraceSourceAccessor(&UserApp::m_txTraceWithAddresses),
                "ns3::Packet::TwoAddressTracedCallback"
            );
    return tid;
}

UserApp::UserApp() { NS_LOG_FUNCTION(this); }

UserApp::~UserApp() { NS_LOG_FUNCTION(this); }

void UserApp::StartApplication() {
    NS_LOG_FUNCTION(this);
    switch (this->appOption)
    {
    case USER_TRASH:
        micelio::UserApp::spawn_trash(*this->sim_params, this->params);
        break;
    case USER_BIKES:
        micelio::UserApp::spawn_bikes(*this->sim_params, this->params);
        break;
    default:
        NS_ASSERT_MSG(false, "invalid app option");
    }
    nsrs::run();
}

void UserApp::StopApplication() { NS_LOG_FUNCTION(this); }

void UserApp::SetParams(micelio::SimulationParams *sim_params, int appOption, micelio::UserAppParams params) {
    this->params = params;
    this->sim_params = sim_params;
    this->appOption = appOption;
}

} // Namespace ns3