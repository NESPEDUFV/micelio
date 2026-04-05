#ifndef EDGE_APP_H
#define EDGE_APP_H

#include "micelio-ns3/src/lib.rs.h"
#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/ipv4-address.h"
#include "ns3/ptr.h"
#include "ns3/traced-callback.h"

namespace ns3 {

class Socket;
class Packet;

class EdgeApp : public Application {
public:
    static TypeId GetTypeId();

    EdgeApp();
    ~EdgeApp() override;

    void SetParams(micelio::SimulationParams *sim_params, micelio::EdgeAppParams params);

private:
    void StartApplication() override;
    void StopApplication() override;

    micelio::SimulationParams *sim_params;
    micelio::EdgeAppParams params;

    /// Callbacks for tracing the packet Tx events, includes source and
    /// destination addresses
    TracedCallback<Ptr<const Packet>, const Address &, const Address &>
        m_txTraceWithAddresses;

    /// Callbacks for tracing the packet Rx events, includes source and
    /// destination addresses
    TracedCallback<Ptr<const Packet>, const Address &, const Address &>
        m_rxTraceWithAddresses;
};

} // namespace ns3

#endif /* EDGE_APP_H */
