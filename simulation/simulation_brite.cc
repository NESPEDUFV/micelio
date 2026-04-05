#include "Brite.h"
#include "micelio-ns3/src/lib.rs.h"
#include "model/cloud.h"
#include "model/edge.h"
#include "model/fog.h"
#include "model/user.h"
#include "ns3/application-container.h"
#include "ns3/application-helper.h"
#include "ns3/applications-module.h"
#include "ns3/brite-module.h"
#include "ns3/core-module.h"
#include "ns3/csma-module.h"
#include "ns3/epc-helper.h"
#include "ns3/internet-module.h"
#include "ns3/ipv4-static-routing-helper.h"
#include "ns3/mobility-module.h"
#include "ns3/network-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/ssid.h"
#include "ns3/yans-wifi-helper.h"
#include "nsrs/src/lib.rs.h"

#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>

using namespace ns3;

typedef std::unordered_map<uint32_t, NodeContainer> ApMap;
typedef std::unordered_map<uint32_t, uint32_t> ApInverseMap;

NS_LOG_COMPONENT_DEFINE("MicelioSimulator");

void configureTrainTestData(micelio::SimulationParams *simParams, NodeContainer &edgeNodes);

void assignAccessPoints(
    BriteTopologyHelper &brite,
    ApMap &apMap,
    NodeContainer &fogNodes,
    NodeContainer &edgeNodes,
    NodeContainer &userNodes,
    size_t nodesPerAp
);

void configureFogPosition(
    BriteTopologyHelper &brite,
    Ptr<ListPositionAllocator> alloc,
    NodeContainer &nodes,
    double radius
);

void configureEdgePosition(
    BriteTopologyHelper &brite,
    ApInverseMap &apInvMap,
    Ptr<ListPositionAllocator> alloc,
    NodeContainer &nodes,
    double radius
);

void configureWifiNetwork(
    InternetStackHelper &stack,
    Ipv4StaticRoutingHelper &routing,
    Ptr<Node> routerNode,
    NodeContainer &staNodes,
    Ipv4AddressHelper &addr
);

Vector addRandomVecOffset(double x, double y, double minR, double maxR);

ApplicationContainer configureCloudApp(micelio::SimulationParams *simParams, Ptr<Node> cloudNode);

ApplicationContainer configureFogApps(
    micelio::SimulationParams *simParams,
    std::vector<Ipv4Address> &fogAddrs,
    Ipv4Address &cloudAddr,
    NodeContainer &fogNodes
);

ApplicationContainer configureEdgeApps(
    micelio::SimulationParams *simParams, Ipv4Address &cloudAddr, NodeContainer &edgeNodes
);

ApplicationContainer configureUserApps(
    micelio::SimulationParams *simParams,
    Ipv4Address &cloudAddr,
    NodeContainer &edgeNodes,
    NodeContainer &userNodes
);

int main(int argc, char *argv[]) {
    LogComponentEnable("MicelioSimulator", LOG_LEVEL_ALL);
    srand(42);

    NodeContainer userNodes;
    NodeContainer edgeNodes;
    NodeContainer fogNodes;
    Ptr<Node> cloudNode = CreateObject<Node>();
    ApMap apMap;
    ApInverseMap apInvMap;

    NS_LOG_INFO("[setup] Reading simulation parameters...");
    auto simParams    = micelio::read_params().into_raw();
    size_t nUserNodes = simParams->n_user_nodes();
    size_t nEdgeNodes = simParams->n_edge_nodes();
    size_t nFogNodes  = simParams->n_fog_nodes();
    size_t nodesPerAp = simParams->nodes_per_ap();

    double maxDistanceOffset = 3.0;

    fogNodes.Create(nFogNodes);
    edgeNodes.Create(nEdgeNodes);
    userNodes.Create(nUserNodes);

    configureTrainTestData(simParams, edgeNodes);

    NS_LOG_INFO("[setup] BRITE init...");
    BriteTopologyHelper brite("scratch/micelio/RTWaxman.conf");
    brite.AssignStreams(3);
    InternetStackHelper stack;
    Ipv4AddressHelper routersAddr;
    routersAddr.SetBase("13.0.0.0", "255.255.255.252");
    brite.BuildBriteTopology(stack);
    brite.AssignIpv4Addresses(routersAddr);

    NS_LOG_INFO("[setup] Access point assignment...");
    assignAccessPoints(brite, apMap, fogNodes, edgeNodes, userNodes, nodesPerAp);
    for (auto pair : apMap) {
        auto staNodes = pair.second;
        for (auto it = staNodes.Begin(); it != staNodes.End(); ++it) {
            apInvMap.emplace((*it)->GetId(), pair.first);
        }
    }
    NodeContainer apNodes;
    for (auto pair : apMap) {
        apNodes.Add(brite.GetLeafNodeForAs(0, pair.first));
    }

    NS_LOG_INFO("[setup] Fog nodes positioning...");
    MobilityHelper fogMobility;
    Ptr<ListPositionAllocator> fogPosAlloc = CreateObject<ListPositionAllocator>();
    configureFogPosition(brite, fogPosAlloc, fogNodes, maxDistanceOffset);
    fogMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    fogMobility.SetPositionAllocator(fogPosAlloc);
    fogMobility.Install(fogNodes);

    NS_LOG_INFO("[setup] Edge nodes positioning...");
    MobilityHelper edgeMobility;
    Ptr<ListPositionAllocator> edgePosAlloc = CreateObject<ListPositionAllocator>();
    configureEdgePosition(brite, apInvMap, edgePosAlloc, edgeNodes, maxDistanceOffset);
    edgeMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    edgeMobility.SetPositionAllocator(edgePosAlloc);
    edgeMobility.Install(edgeNodes);

    NS_LOG_INFO("[setup] User nodes positioning...");
    MobilityHelper userMobility;
    Ptr<ListPositionAllocator> userPosAlloc = CreateObject<ListPositionAllocator>();
    configureEdgePosition(brite, apInvMap, userPosAlloc, userNodes, maxDistanceOffset);
    userMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    userMobility.SetPositionAllocator(userPosAlloc);
    userMobility.Install(userNodes);

    NS_LOG_INFO("[setup] Access points positioning...");
    MobilityHelper apMobility;
    Ptr<ListPositionAllocator> apPosAlloc = CreateObject<ListPositionAllocator>();
    for (auto it = apNodes.Begin(); it != apNodes.End(); ++it) {
        auto node = *it;
        auto pos  = brite.GetNodePosition(node->GetId());
        apPosAlloc->Add(Vector(pos.x, pos.y, 0.0));
    }
    apMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    apMobility.SetPositionAllocator(apPosAlloc);
    apMobility.Install(apNodes);

    NS_LOG_INFO("[setup] Internet stack...");
    stack.Install(cloudNode);
    stack.Install(fogNodes);
    stack.Install(edgeNodes);
    stack.Install(userNodes);

    NS_LOG_INFO("[setup] Cloud links...");
    PointToPointHelper p2pCloud;
    auto cloudLinkParams = simParams->link_cloud_to_edge();
    p2pCloud.SetDeviceAttribute("DataRate", DataRateValue(cloudLinkParams.data_rate));
    p2pCloud.SetChannelAttribute("Delay", TimeValue(MilliSeconds(cloudLinkParams.delay)));
    NetDeviceContainer cloudDevs = p2pCloud.Install(cloudNode, brite.GetLeafNodeForAs(0, 0));
    Ipv4AddressHelper cloudAddrHelper;
    cloudAddrHelper.SetBase("10.42.0.0", "255.255.255.252");
    Ipv4InterfaceContainer cloudIf = cloudAddrHelper.Assign(cloudDevs);
    Ipv4Address cloudAddr          = cloudIf.GetAddress(0);
    NS_LOG_INFO("[setup] Cloud address: " << cloudAddr);

    NS_LOG_INFO("[setup] Fog links...");
    PointToPointHelper p2pFog;
    auto fogLinkParams = simParams->link_fog_to_edge();
    p2pFog.SetDeviceAttribute("DataRate", DataRateValue(fogLinkParams.data_rate));
    p2pFog.SetChannelAttribute("Delay", TimeValue(MilliSeconds(fogLinkParams.delay)));
    std::vector<Ipv4Address> fogAddrs;
    Ipv4AddressHelper fogAddrHelper;
    fogAddrHelper.SetBase("10.23.0.0", "255.255.255.252");
    for (uint32_t i = 0; i < fogNodes.GetN(); ++i) {
        auto node = fogNodes.Get(i);
        fogAddrHelper.NewNetwork();
        NetDeviceContainer fogDevs   = p2pFog.Install(node, brite.GetLeafNodeForAs(0, 1 + i));
        Ipv4InterfaceContainer fogIf = fogAddrHelper.Assign(fogDevs);
        fogAddrs.push_back(fogIf.GetAddress(0));
    }

    NS_LOG_INFO("[setup] Access points links and routing...");
    Ipv4StaticRoutingHelper routing;
    Ipv4AddressHelper wifiAddr;
    wifiAddr.SetBase("192.168.0.0", "255.255.255.0");
    for (auto pair : apMap) {
        auto apNode   = brite.GetLeafNodeForAs(0, pair.first);
        auto staNodes = pair.second;
        configureWifiNetwork(stack, routing, apNode, staNodes, wifiAddr);
    }

    NS_LOG_INFO("[setup] Populating global route table...");
    Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    configureCloudApp(simParams, cloudNode);
    configureFogApps(simParams, fogAddrs, cloudAddr, fogNodes);
    configureEdgeApps(simParams, cloudAddr, edgeNodes);
    configureUserApps(simParams, cloudAddr, edgeNodes, userNodes);

    NS_LOG_INFO("[setup] Starting simulation...");
    auto setup = micelio::setup(*simParams);
    Simulator::Run();
    auto now = Simulator::Now().GetSeconds();
    std::cout << "finished at " << now << " seconds\n";
    Simulator::Destroy();
    micelio::teardown(std::move(setup));
    return 0;
}

void configureTrainTestData(micelio::SimulationParams *simParams, NodeContainer &edgeNodes) {
    std::vector<uint32_t> nodes;
    for (auto it = edgeNodes.Begin(); it != edgeNodes.End(); ++it) {
        auto node = (*it)->GetId();
        nodes.push_back(node);
    }
    rust::Slice<const uint32_t> nodeslice{nodes.data(), nodes.size()};
    simParams->setup_train_data(nodeslice);
}

void assignAccessPointsInner(
    ApMap &apMap, NodeContainer &nodes, uint32_t nReserved, uint32_t nLeafNodes, size_t nodesPerAp
) {
    for (auto it = nodes.Begin(); it != nodes.End(); ++it) {
        auto node = *it;
        uint32_t rLeaf;
        NodeContainer *apList = nullptr;
        do {
            rLeaf = nReserved + (rand() % nLeafNodes);
            if (apMap.find(rLeaf) == apMap.end())
                apMap.emplace(
                    std::piecewise_construct, std::forward_as_tuple(rLeaf), std::forward_as_tuple()
                );

            apList = &apMap.at(rLeaf);
        } while (apList->GetN() >= nodesPerAp);
        apList->Add(node);
    }
}

void assignAccessPoints(
    BriteTopologyHelper &brite,
    ApMap &apMap,
    NodeContainer &fogNodes,
    NodeContainer &edgeNodes,
    NodeContainer &userNodes,
    size_t nodesPerAp
) {
    auto nReserved  = 1 + fogNodes.GetN();
    auto nLeafNodes = brite.GetNLeafNodesForAs(0) - nReserved;
    auto minNodes =
        (size_t)(ceil((double)(edgeNodes.GetN() + userNodes.GetN()) / (double)(nodesPerAp)));
    NS_ASSERT_MSG(
        nLeafNodes >= minNodes,
        "not enough leaf nodes for edge, expected at least " << minNodes << ", but got "
                                                             << nLeafNodes
    );
    assignAccessPointsInner(apMap, edgeNodes, nReserved, minNodes, nodesPerAp);
    assignAccessPointsInner(apMap, userNodes, nReserved, minNodes, nodesPerAp);
    for (auto it = apMap.begin(); it != apMap.end();) {
        if (it->second.GetN() == 0) {
            it = apMap.erase(it);
        } else {
            ++it;
        }
    }
}

void configureFogPosition(
    BriteTopologyHelper &brite,
    Ptr<ListPositionAllocator> alloc,
    NodeContainer &nodes,
    double radius
) {
    for (uint32_t i = 0; i < nodes.GetN(); ++i) {
        auto leafNum  = 1 + i;
        auto leafNode = brite.GetLeafNodeForAs(0, leafNum);
        auto leafPos  = brite.GetNodePosition(leafNode->GetId());
        alloc->Add(addRandomVecOffset(leafPos.x, leafPos.y, 1.0, radius));
    }
}

void configureEdgePosition(
    BriteTopologyHelper &brite,
    ApInverseMap &apInvMap,
    Ptr<ListPositionAllocator> alloc,
    NodeContainer &nodes,
    double radius
) {
    for (auto it = nodes.Begin(); it != nodes.End(); ++it) {
        auto node     = *it;
        auto leafNode = brite.GetLeafNodeForAs(0, apInvMap[node->GetId()]);
        auto leafPos  = brite.GetNodePosition(leafNode->GetId());
        alloc->Add(addRandomVecOffset(leafPos.x, leafPos.y, 1.0, radius));
    }
}

Vector addRandomVecOffset(double x, double y, double minR, double maxR) {
    auto angle   = ((double)(rand() % 360) / 360.0) * M_PI * 2.0;
    double rFrac = (double)(rand() % 100) / 100.0;
    double r     = minR + (maxR - minR) * rFrac;
    auto dx      = x + r * std::cos(angle);
    auto dy      = y + r * std::sin(angle);
    return Vector(dx, dy, 0.0);
}

void configureWifiNetwork(
    InternetStackHelper &stack,
    Ipv4StaticRoutingHelper &routing,
    Ptr<Node> apNode,
    NodeContainer &staNodes,
    Ipv4AddressHelper &addrHelper
) {
    YansWifiChannelHelper channel = YansWifiChannelHelper::Default();
    YansWifiPhyHelper phy;
    phy.SetChannel(channel.Create());

    WifiMacHelper mac;
    Ssid ssid = Ssid("net" + std::to_string(apNode->GetId()));

    WifiHelper wifi;
    wifi.SetStandard(WIFI_STANDARD_80211n);

    NetDeviceContainer staDevices;
    mac.SetType("ns3::StaWifiMac", "Ssid", SsidValue(ssid), "ActiveProbing", BooleanValue(false));
    staDevices = wifi.Install(phy, mac, staNodes);

    NetDeviceContainer apDevices;
    mac.SetType("ns3::ApWifiMac", "Ssid", SsidValue(ssid));
    apDevices = wifi.Install(phy, mac, apNode);

    addrHelper.NewNetwork();
    auto apAddr = addrHelper.Assign(apDevices);
    addrHelper.Assign(staDevices);

    auto _ = routing;
    // for (auto it = staNodes.Begin(); it != staNodes.End(); ++it) {
    //     auto node = *it;
    //     auto ipv4 = node->GetObject<Ipv4>();
    //     auto sr   = routing.GetStaticRouting(ipv4);
    //     sr->SetDefaultRoute(apAddr.GetAddress(0), 1);
    // }
}

ApplicationContainer configureCloudApp(micelio::SimulationParams *simParams, Ptr<Node> node) {
    micelio::CloudAppParams params{.node_id = node->GetId(), .port = simParams->cloud_port()};
    ApplicationHelper helper(CloudApp::GetTypeId());
    ApplicationContainer apps = helper.Install(node);
    apps.Get(0)->GetObject<CloudApp>()->SetParams(simParams, params);
    apps.Start(Seconds(0.5));
    return apps;
}

ApplicationContainer configureFogApps(
    micelio::SimulationParams *simParams,
    std::vector<Ipv4Address> &fogAddrs,
    Ipv4Address &cloudIp,
    NodeContainer &nodes
) {
    ApplicationHelper helper(FogApp::GetTypeId());
    ApplicationContainer apps = helper.Install(nodes);
    InetSocketAddress cloudAddr(cloudIp, simParams->cloud_port());
    for (uint32_t i = 0; i < apps.GetN(); ++i) {
        auto node    = nodes.Get(i);
        auto pos     = node->GetObject<MobilityModel>()->GetPosition();
        auto localIp = fogAddrs.at(i);
        InetSocketAddress localAddr(localIp, simParams->fog_port());
        micelio::FogAppParams params{
            .node_id    = node->GetId(),
            .position   = std::array{pos.x, pos.y, pos.z},
            .cloud_addr = nsrs::addr_from_ns3(cloudAddr),
            .local_addr = nsrs::addr_from_ns3(localAddr),
        };
        auto app = apps.Get(i)->GetObject<FogApp>();
        app->SetParams(simParams, params);
    }
    apps.Start(Seconds(5.0));
    return apps;
}

ApplicationContainer configureEdgeApps(
    micelio::SimulationParams *simParams, Ipv4Address &cloudIp, NodeContainer &nodes
) {
    ApplicationHelper helper(EdgeApp::GetTypeId());
    ApplicationContainer apps = helper.Install(nodes);
    InetSocketAddress cloudAddr(cloudIp, simParams->cloud_port());
    for (uint32_t i = 0; i < apps.GetN(); ++i) {
        auto node = nodes.Get(i);
        auto pos  = node->GetObject<MobilityModel>()->GetPosition();
        micelio::EdgeAppParams params{
            .node_id    = node->GetId(),
            .position   = std::array{pos.x, pos.y, pos.z},
            .cloud_addr = nsrs::addr_from_ns3(cloudAddr),
        };
        auto app = apps.Get(i)->GetObject<EdgeApp>();
        app->SetParams(simParams, params);
        app->SetStartTime(Seconds(1.5 + (rand() % 200) * 0.01));
    }
    return apps;
}

ApplicationContainer configureUserApps(
    micelio::SimulationParams *simParams,
    Ipv4Address &cloudIp,
    NodeContainer &edgeNodes,
    NodeContainer &nodes
) {
    ApplicationHelper helper(UserApp::GetTypeId());
    ApplicationContainer apps = helper.Install(nodes);
    InetSocketAddress cloudAddr(cloudIp, simParams->cloud_port());
    for (uint32_t i = 0; i < apps.GetN(); ++i) {
        auto node  = nodes.Get(i);
        auto pos   = node->GetObject<MobilityModel>()->GetPosition();
        auto node0 = edgeNodes.Get(0)->GetId();
        micelio::UserAppParams params{
            .node_id           = node->GetId(),
            .position          = std::array{pos.x, pos.y, pos.z},
            .cloud_addr        = nsrs::addr_from_ns3(cloudAddr),
            .initial_edge_node = node0,
            .is_leader         = i == 0,
        };
        auto app = apps.Get(i)->GetObject<UserApp>();
        app->SetParams(simParams, params);
    }
    apps.Start(Seconds(10.0));
    return apps;
}