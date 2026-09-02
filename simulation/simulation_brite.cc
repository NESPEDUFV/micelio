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
#include "ns3/nix-vector-routing-module.h"
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

typedef std::unordered_map<uint32_t, Ptr<Node>> NodeIdMap;
typedef std::unordered_map<uint32_t, NodeContainer> ApMap;
typedef std::unordered_map<uint32_t, uint32_t> ApInverseMap;

const double C = 299792458;

NS_LOG_COMPONENT_DEFINE("MicelioSimulator");

Vector addRandomVecOffset(double x, double y, double minR, double maxR) {
    auto angle   = ((double)(rand() % 360) / 360.0) * M_PI * 2.0;
    double rFrac = (double)(rand() % 100) / 100.0;
    double r     = minR + (maxR - minR) * rFrac;
    auto dx      = x + r * std::cos(angle);
    auto dy      = y + r * std::sin(angle);
    return Vector(dx, dy, 0.0);
}

void setupTrashData(micelio::SimulationParams *simParams, NodeContainer &edgeTrashNodes) {
    std::vector<uint32_t> nodes;
    for (auto it = edgeTrashNodes.Begin(); it != edgeTrashNodes.End(); ++it) {
        auto node = (*it)->GetId();
        nodes.push_back(node);
    }
    rust::Slice<const uint32_t> nodeslice{nodes.data(), nodes.size()};
    simParams->setup_trash_data(nodeslice);
}

void setupBriteNodesLocation(
    BriteTopologyHelper &brite,
    micelio::CoordSpace *coordSpace,
    NodeIdMap &nodeIdMap,
    uint32_t nReserved,
    uint32_t minNodes,
    NodeContainer &edgeNodes,
    NodeContainer &userNodes,
    size_t nodesPerAp
) {
    auto nLeafNodes = brite.GetNLeafNodesForAs(0);
    NS_ASSERT_MSG(
        nLeafNodes >= minNodes + nReserved,
        "not enough leaf nodes for edge, expected at least " << minNodes + nReserved << ", but got "
                                                             << nLeafNodes
    );
    NS_LOG_INFO("[setup] Brite nodes positioning...");
    MobilityHelper apMobility;
    Ptr<ListPositionAllocator> apPosAlloc = CreateObject<ListPositionAllocator>();
    NodeContainer apNodes;
    for (uint32_t i = nReserved; i < nLeafNodes; ++i) {
        auto leafNode   = brite.GetLeafNodeForAs(0, i);
        auto leafNodeId = leafNode->GetId();
        auto leafPos    = brite.GetNodePosition(leafNodeId);
        nodeIdMap.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(leafNodeId),
            std::forward_as_tuple(leafNode)
        );
        coordSpace->add_node(leafNodeId, leafPos.x, leafPos.y);
        auto posXy = coordSpace->brite_to_euclid(leafPos.x, leafPos.y);
        apPosAlloc->Add(Vector(posXy[0], posXy[1], 0.0));
        apNodes.Add(leafNode);
    }
    apMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    apMobility.SetPositionAllocator(apPosAlloc);
    apMobility.Install(apNodes);
}

void assignAccessPointsBikes(
    micelio::SimulationParams *simParams,
    NodeIdMap &nodeIdMap,
    ApMap &apMap,
    micelio::CoordSpace *coordSpace,
    NodeContainer &nodes,
    NodeContainer &bssApNodes,
    NodeContainer &bssApRouterNodes
) {
    MobilityHelper bssMobility;
    auto bssPosAlloc = CreateObject<ListPositionAllocator>();
    uint32_t i       = 0;
    for (auto it = nodes.Begin(); it != nodes.End(); ++it, ++i) {
        auto pos     = simParams->get_station_geopos(i);
        auto xyPos   = coordSpace->geo_to_euclid(pos[1], pos[0]);
        auto nearest = coordSpace->nearest_node(pos[1], pos[0]);
        // auto removed = coordSpace->remove_node(nearest.id, nearest.lat, nearest.lng);
        // NS_ASSERT_MSG(removed > 0, "should have removed used node from tree!");
        NS_LOG_DEBUG("[setup] nearest ID: " << nearest.id);
        auto auxNode = CreateObject<Node>();
        uint32_t id  = auxNode->GetId();
        nodeIdMap.emplace(
            std::piecewise_construct, std::forward_as_tuple(id), std::forward_as_tuple(auxNode)
        );
        bssApNodes.Add(auxNode);
        bssApRouterNodes.Add(nodeIdMap[nearest.id]);
        bssPosAlloc->Add(addRandomVecOffset(xyPos[0], xyPos[1], 1.0, 3.0));
        if (apMap.find(id) == apMap.end())
            apMap.emplace(
                std::piecewise_construct, std::forward_as_tuple(id), std::forward_as_tuple()
            );
        auto apList = &apMap.at(id);
        apList->Add(*it);
    }
    bssMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    bssMobility.SetPositionAllocator(bssPosAlloc);
    bssMobility.Install(bssApNodes);
}

void assignAccessPointsRandom(
    BriteTopologyHelper &brite,
    ApMap &apMap,
    NodeContainer &nodes,
    uint32_t nReserved,
    uint32_t nLeafNodes,
    size_t nodesPerAp
) {
    for (auto it = nodes.Begin(); it != nodes.End(); ++it) {
        NodeContainer *apList = nullptr;
        do {
            uint32_t rLeaf = nReserved + (rand() % nLeafNodes);
            auto rLeafNode = brite.GetLeafNodeForAs(0, rLeaf);
            auto rLeafId   = rLeafNode->GetId();
            // creates an empty container in the map
            if (apMap.find(rLeafId) == apMap.end())
                apMap.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(rLeafId),
                    std::forward_as_tuple()
                );

            apList = &apMap.at(rLeafId);
            // loop until a container that isn't "full" is randomly found
        } while (apList->GetN() >= nodesPerAp);
        apList->Add(*it);
    }
}

void cleanUnusedInApMap(ApMap &apMap) {
    for (auto it = apMap.begin(); it != apMap.end();) {
        if (it->second.GetN() == 0) {
            NS_LOG_DEBUG("[setup] clean unused ap list " << it->first);
            it = apMap.erase(it);
        } else {
            ++it;
        }
    }
}

void configureFogPosition(
    micelio::CoordSpace *coordSpace,
    BriteTopologyHelper &brite,
    Ptr<ListPositionAllocator> alloc,
    NodeContainer &nodes,
    double radius
) {
    for (uint32_t i = 0; i < nodes.GetN(); ++i) {
        auto leafNum  = 1 + i;
        auto leafNode = brite.GetLeafNodeForAs(0, leafNum);
        auto leafPos  = brite.GetNodePosition(leafNode->GetId());
        auto posXy    = coordSpace->brite_to_euclid(leafPos.x, leafPos.y);
        auto nodePos  = addRandomVecOffset(posXy[0], posXy[1], 1.0, radius);
        NS_LOG_DEBUG("[setup] fog node index " << i << ", position: " << nodePos);
        alloc->Add(nodePos);
    }
}

void configureEdgeBikesPosition(
    micelio::SimulationParams *simParams,
    micelio::CoordSpace *coordSpace,
    Ptr<ListPositionAllocator> alloc,
    NodeContainer &bikesNodes
) {
    uint32_t i = 0;
    for (auto it = bikesNodes.Begin(); it != bikesNodes.End(); ++it, ++i) {
        auto node     = *it;
        auto bssPos   = simParams->get_station_geopos(i);
        auto bssXyPos = coordSpace->geo_to_euclid(bssPos[1], bssPos[0]);
        NS_LOG_DEBUG(
            "[setup] edge bikes node " << node->GetId() << ", position: " << bssXyPos[0] << ":"
                                       << bssXyPos[1]
        );
        alloc->Add(Vector(bssXyPos[0], bssXyPos[1], 0.0));
    }
}

void configureEdgePosition(
    NodeIdMap &nodeIdMap,
    ApInverseMap &apInvMap,
    Ptr<ListPositionAllocator> alloc,
    NodeContainer &nodes,
    double radius
) {
    for (auto it = nodes.Begin(); it != nodes.End(); ++it) {
        auto node     = *it;
        auto apNodeId = apInvMap[node->GetId()];
        auto apNode   = nodeIdMap[apNodeId];
        auto apPos    = apNode->GetObject<MobilityModel>()->GetPosition();
        auto nodePos  = addRandomVecOffset(apPos.x, apPos.y, 1.0, radius);
        NS_LOG_DEBUG("[setup] edge/user node " << node->GetId() << ", position: " << nodePos);
        alloc->Add(nodePos);
    }
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
    micelio::CoordSpace *coordSpace,
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
            .position   = coordSpace->euclid_to_geo(pos.x, pos.y),
            .cloud_addr = nsrs::addr_from_ns3(cloudAddr),
            .local_addr = nsrs::addr_from_ns3(localAddr),
        };
        auto app = apps.Get(i)->GetObject<FogApp>();
        app->SetParams(simParams, params);
    }
    apps.Start(Seconds(5.0));
    return apps;
}

rust::String getEdgeNodeName(micelio::SimulationParams *simParams, int appOption, uint32_t i) {
    if (appOption == APP_TRASH) {
        std::stringstream ss;
        ss << "TrashBinEdgeNode" << i;
        return rust::String(ss.str());
    } else if (appOption == APP_BIKES) {
        auto name = simParams->get_station_name(i);
        return rust::String(name.data(), name.length());
    } else {
        NS_ASSERT_MSG(false, "invalid app option!");
        return rust::String("");
    }
}

ApplicationContainer configureEdgeApps(
    micelio::SimulationParams *simParams,
    micelio::CoordSpace *coordSpace,
    Ipv4Address &cloudIp,
    NodeContainer &nodes,
    int appOption
) {
    ApplicationHelper helper(EdgeApp::GetTypeId());
    ApplicationContainer apps = helper.Install(nodes);
    InetSocketAddress cloudAddr(cloudIp, simParams->cloud_port());
    for (uint32_t i = 0; i < apps.GetN(); ++i) {
        auto node = nodes.Get(i);
        auto pos  = node->GetObject<MobilityModel>()->GetPosition();
        micelio::EdgeAppParams params{
            .node_id    = node->GetId(),
            .node_name  = getEdgeNodeName(simParams, appOption, i),
            .position   = coordSpace->euclid_to_geo(pos.x, pos.y),
            .cloud_addr = nsrs::addr_from_ns3(cloudAddr),
        };
        auto app = apps.Get(i)->GetObject<EdgeApp>();
        app->SetParams(simParams, appOption, params);
        app->SetStartTime(Seconds(1.5 + (rand() % 200) * 0.01));
    }
    return apps;
}

ApplicationContainer configureUserApps(
    micelio::SimulationParams *simParams,
    micelio::CoordSpace *coordSpace,
    Ipv4Address &cloudIp,
    NodeContainer &edgeNodes,
    NodeContainer &nodes,
    int appOption
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
            .position          = coordSpace->euclid_to_geo(pos.x, pos.y),
            .cloud_addr        = nsrs::addr_from_ns3(cloudAddr),
            .initial_edge_node = node0,
            .is_leader         = i == 0,
        };
        auto app = apps.Get(i)->GetObject<UserApp>();
        app->SetParams(simParams, appOption, params);
    }
    apps.Start(Seconds(10.0));
    return apps;
}

int main(int argc, char *argv[]) {
    LogComponentEnable("MicelioSimulator", LOG_LEVEL_INFO);
    LogComponentEnable("UdpEchoServerApplication", LOG_LEVEL_INFO);
    LogComponentEnable("UdpEchoClientApplication", LOG_LEVEL_INFO);
    LogComponentEnable("UdpServer", LOG_LEVEL_INFO);
    LogComponentEnable("UdpClient", LOG_LEVEL_INFO);
    // LogComponentEnable("NixVectorRouting", LOG_LEVEL_ALL);

    NS_LOG_INFO("[setup] Reading simulation parameters...");
    auto simParams = micelio::read_params().into_raw();
    
    const char *debugEnv = std::getenv("MICELIO_DEBUG");
    const char *baseline = std::getenv("MICELIO_BASELINE");
    if (baseline != nullptr && std::strcmp(baseline, "bikes") == 0) {
        simParams->run_baseline_bikes();
        return 0;
    } else if (baseline != nullptr && std::strcmp(baseline, "trash") == 0) {
        simParams->run_baseline_trash();
        return 0;
    }

    bool debugMode = debugEnv != nullptr && *debugEnv == '1';
    srand(42);

    NodeContainer userNodes;
    NodeContainer userTrashNodes;
    NodeContainer userBikesNodes;
    NodeContainer edgeNodes;
    NodeContainer edgeTrashNodes;
    NodeContainer edgeBikesNodes;
    NodeContainer fogNodes;
    NodeContainer bssApNodes;
    NodeContainer bssApRouterNodes;
    Ptr<Node> cloudNode = CreateObject<Node>();
    NodeIdMap nodeIdMap;
    ApMap apMap;
    ApInverseMap apInvMap;

    auto coordSpace        = simParams->coord_space().into_raw();
    size_t nUserTrashNodes = simParams->n_trash_user_nodes();
    size_t nUserBikesNodes = simParams->n_bikes_user_nodes();
    size_t nEdgeTrashNodes = simParams->n_trash_edge_nodes();
    size_t nEdgeBikesNodes = simParams->n_bikes_edge_nodes();
    size_t nFogNodes       = simParams->n_fog_nodes();
    size_t nodesPerAp      = simParams->nodes_per_ap();

    double maxDistanceOffset = 3.0;

    fogNodes.Create(nFogNodes);
    edgeTrashNodes.Create(nEdgeTrashNodes);
    edgeBikesNodes.Create(nEdgeBikesNodes);
    edgeNodes.Add(edgeTrashNodes);
    edgeNodes.Add(edgeBikesNodes);
    userTrashNodes.Create(nUserTrashNodes);
    userBikesNodes.Create(nUserBikesNodes);
    userNodes.Add(userTrashNodes);
    userNodes.Add(userBikesNodes);

    simParams->setup_bikes_stations();
    setupTrashData(simParams, edgeTrashNodes);

    NS_LOG_INFO("[setup] BRITE init...");
    auto rsBriteParams = simParams->brite_params();
    std::string briteParams(rsBriteParams.data(), rsBriteParams.length());
    BriteTopologyHelper brite(briteParams);
    brite.AssignStreams(3);
    InternetStackHelper stack;
    Ipv4NixVectorHelper nixRouting;
    stack.SetRoutingHelper(nixRouting);
    Ipv4AddressHelper routersAddr;
    routersAddr.SetBase("13.0.0.0", "255.255.255.252");
    brite.BuildBriteTopology(stack);
    brite.AssignIpv4Addresses(routersAddr);

    uint32_t nReserved = 1 + fogNodes.GetN();
    uint32_t minNodes =
        (uint32_t)(ceil((double)(edgeNodes.GetN() + userNodes.GetN()) / (double)(nodesPerAp)));
    setupBriteNodesLocation(
        brite, coordSpace, nodeIdMap, nReserved, minNodes, edgeNodes, userNodes, nodesPerAp
    );
    NS_LOG_INFO("[setup] Access point assignment...");
    assignAccessPointsBikes(
        simParams, nodeIdMap, apMap, coordSpace, edgeBikesNodes, bssApNodes, bssApRouterNodes
    );
    assignAccessPointsRandom(brite, apMap, edgeTrashNodes, nReserved, minNodes, nodesPerAp);
    assignAccessPointsRandom(brite, apMap, userNodes, nReserved, minNodes, nodesPerAp);
    cleanUnusedInApMap(apMap);
    for (auto pair : apMap) {
        auto staNodes = pair.second;
        for (auto it = staNodes.Begin(); it != staNodes.End(); ++it) {
            apInvMap.emplace((*it)->GetId(), pair.first);
        }
    }

    NodeContainer apNodes;
    for (auto pair : apMap) {
        auto nodeId = pair.first;
        auto node   = nodeIdMap.at(nodeId);
        apNodes.Add(node);
    }

    NS_LOG_INFO("[setup] Fog nodes positioning...");
    MobilityHelper fogMobility;
    Ptr<ListPositionAllocator> fogPosAlloc = CreateObject<ListPositionAllocator>();
    configureFogPosition(coordSpace, brite, fogPosAlloc, fogNodes, maxDistanceOffset);
    fogMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    fogMobility.SetPositionAllocator(fogPosAlloc);
    fogMobility.Install(fogNodes);

    NS_LOG_INFO("[setup] Edge nodes positioning...");
    MobilityHelper edgeTrashMobility;
    Ptr<ListPositionAllocator> edgeTrashPosAlloc = CreateObject<ListPositionAllocator>();
    configureEdgePosition(
        nodeIdMap, apInvMap, edgeTrashPosAlloc, edgeTrashNodes, maxDistanceOffset
    );
    edgeTrashMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    edgeTrashMobility.SetPositionAllocator(edgeTrashPosAlloc);
    edgeTrashMobility.Install(edgeTrashNodes);

    MobilityHelper edgeBikesMobility;
    Ptr<ListPositionAllocator> edgeBikesPosAlloc = CreateObject<ListPositionAllocator>();
    configureEdgeBikesPosition(simParams, coordSpace, edgeBikesPosAlloc, edgeBikesNodes);
    edgeBikesMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    edgeBikesMobility.SetPositionAllocator(edgeBikesPosAlloc);
    edgeBikesMobility.Install(edgeBikesNodes);

    NS_LOG_INFO("[setup] User nodes positioning...");
    MobilityHelper userMobility;
    Ptr<ListPositionAllocator> userPosAlloc = CreateObject<ListPositionAllocator>();
    configureEdgePosition(nodeIdMap, apInvMap, userPosAlloc, userNodes, maxDistanceOffset);
    userMobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    userMobility.SetPositionAllocator(userPosAlloc);
    userMobility.Install(userNodes);

    NS_LOG_INFO("[setup] Internet stack...");
    stack.Install(cloudNode);
    stack.Install(fogNodes);
    stack.Install(edgeNodes);
    stack.Install(userNodes);
    stack.Install(bssApNodes);

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

    NS_LOG_INFO("[setup] BSS access point to router links...");
    Ipv4AddressHelper bssAddr;
    bssAddr.SetBase("13.1.0.0", "255.255.255.252");
    for (uint32_t i = 0; i < bssApNodes.GetN(); ++i) {
        auto bssAp       = bssApNodes.Get(i);
        auto bssApRouter = bssApRouterNodes.Get(i);
        auto posAp       = bssAp->GetObject<MobilityModel>()->GetPosition();
        auto posApRouter = bssApRouter->GetObject<MobilityModel>()->GetPosition();
        auto distance    = (posAp - posApRouter).GetLength();
        PointToPointHelper p2p;
        p2p.SetDeviceAttribute("DataRate", DataRateValue(fogLinkParams.data_rate));
        p2p.SetChannelAttribute("Delay", TimeValue(Seconds(distance / C)));
        auto devs = p2p.Install(bssAp, bssApRouter);
        bssAddr.NewNetwork();
        bssAddr.Assign(devs);
    }

    NS_LOG_INFO("[setup] Access points links and routing...");
    Ipv4StaticRoutingHelper routing;
    Ipv4AddressHelper wifiAddr;
    wifiAddr.SetBase("192.168.0.0", "255.255.255.0");
    for (auto pair : apMap) {
        auto nodeId   = pair.first;
        auto apNode   = nodeIdMap.at(nodeId);
        auto staNodes = pair.second;
        configureWifiNetwork(stack, routing, apNode, staNodes, wifiAddr);
    }

    if (debugMode) {
        NS_LOG_INFO("[setup] Starting debug...");
        UdpEchoClientHelper udpClient(cloudAddr, 42);
        udpClient.SetAttribute("MaxPackets", UintegerValue(2));
        udpClient.SetAttribute("Interval", TimeValue(Seconds(1.0)));
        udpClient.SetAttribute("PacketSize", UintegerValue(1024));
        auto c = udpClient.Install(edgeBikesNodes.Get(0));
        c.Start(Seconds(2.0));
        c.Stop(Seconds(5.0));
        UdpEchoServerHelper udpServer(42);
        auto s = udpServer.Install(cloudNode);
        s.Start(Seconds(1.0));
        s.Stop(Seconds(5.0));
        Simulator::Stop(Seconds(5.0));
        Simulator::Run();
        Simulator::Destroy();
        return 0;
    }

    configureCloudApp(simParams, cloudNode);
    configureFogApps(simParams, coordSpace, fogAddrs, cloudAddr, fogNodes);
    configureEdgeApps(simParams, coordSpace, cloudAddr, edgeTrashNodes, APP_TRASH);
    configureEdgeApps(simParams, coordSpace, cloudAddr, edgeBikesNodes, APP_BIKES);
    configureUserApps(simParams, coordSpace, cloudAddr, edgeTrashNodes, userTrashNodes, USER_TRASH);
    configureUserApps(simParams, coordSpace, cloudAddr, edgeBikesNodes, userBikesNodes, USER_BIKES);

    NS_LOG_INFO("[setup] Starting simulation...");
    auto setup = micelio::setup(*simParams);
    Simulator::Run();
    auto now = Simulator::Now().GetSeconds();
    std::cout << "finished at " << now << " seconds\n";
    Simulator::Destroy();
    micelio::teardown(std::move(setup), *simParams);
    return 0;
}
