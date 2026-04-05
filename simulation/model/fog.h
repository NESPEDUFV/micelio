#ifndef FOG_APP_H
#define FOG_APP_H

#include "micelio-ns3/src/lib.rs.h"
#include "ns3/address.h"
#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/log.h"
#include "ns3/ptr.h"
#include "ns3/traced-callback.h"

namespace ns3 {

class Socket;
class Packet;

class FogApp : public Application {
public:
    /**
     * \brief Get the type ID.
     * \return the object TypeId
     */
    static TypeId GetTypeId();
    FogApp();
    ~FogApp() override;

    void SetParams(micelio::SimulationParams *sim_params, micelio::FogAppParams params);

private:
    void StartApplication() override;
    void StopApplication() override;

    micelio::FogAppParams params;
    micelio::SimulationParams *sim_params;

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

#endif /* FOG_APP_H */