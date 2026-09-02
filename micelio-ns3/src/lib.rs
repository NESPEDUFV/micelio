mod baseline;
mod cloud;
mod coord;
mod dto;
mod edge;
mod fog;
mod params;
mod user;

use cloud::CloudApp;
use coord::CoordSpace;
use edge::EdgeApp;
use fog::FogApp;
use nsrs::sync::Barrier;
use params::SimulationParams;
use std::{path::PathBuf, sync::RwLock, time::Duration};
use testcontainers::{
    Container, GenericImage, ImageExt,
    core::{CmdWaitFor, ExecCommand, IntoContainerPort, Mount, WaitFor},
    runners::SyncRunner,
};
use user::UserApp;

#[cxx::bridge(namespace = "micelio")]
mod ffi {
    pub struct CloudAppParams {
        pub node_id: u32,
        pub port: u16,
    }

    pub struct FogAppParams {
        pub node_id: u32,
        pub position: [f64; 2],
        pub cloud_addr: SocketAddr,
        pub local_addr: SocketAddr,
    }

    pub struct EdgeAppParams {
        pub node_id: u32,
        pub node_name: String,
        pub position: [f64; 2],
        pub cloud_addr: SocketAddr,
    }

    pub struct UserAppParams {
        pub node_id: u32,
        pub position: [f64; 2],
        pub cloud_addr: SocketAddr,
        pub initial_edge_node: u32,
        pub is_leader: bool,
    }

    pub struct WiredParams {
        pub data_rate: u64,
        pub delay: f64,
    }

    pub struct BriteNodeResult {
        pub distance: f64,
        pub id: u32,
        pub lat: f64,
        pub lng: f64,
    }

    extern "Rust" {
        type SimulationParams;
        type SimSetup;

        fn debug_mode(self: &SimulationParams) -> bool;
        fn run_baseline_bikes(self: &SimulationParams);
        fn run_baseline_trash(self: &SimulationParams);
        fn setup_trash_data(self: &mut SimulationParams, nodes: &[u32]) -> Result<()>;
        fn setup_bikes_stations(self: &mut SimulationParams) -> Result<()>;
        fn get_station_name(self: &SimulationParams, i: usize) -> Result<&str>;
        fn get_station_geopos(self: &SimulationParams, i: usize) -> Result<[f64; 2]>;
        fn n_edge_nodes(self: &SimulationParams) -> usize;
        fn n_trash_edge_nodes(self: &SimulationParams) -> usize;
        fn n_bikes_edge_nodes(self: &SimulationParams) -> usize;
        fn n_user_nodes(self: &SimulationParams) -> usize;
        fn n_trash_user_nodes(self: &SimulationParams) -> usize;
        fn n_bikes_user_nodes(self: &SimulationParams) -> usize;
        fn n_fog_nodes(self: &SimulationParams) -> usize;
        fn nodes_per_ap(self: &SimulationParams) -> usize;
        fn link_cloud_to_fog(self: &SimulationParams) -> WiredParams;
        fn link_cloud_to_edge(self: &SimulationParams) -> WiredParams;
        fn link_fog_to_edge(self: &SimulationParams) -> WiredParams;
        fn cloud_port(self: &SimulationParams) -> u16;
        fn fog_port(self: &SimulationParams) -> u16;
        fn coord_space(self: &SimulationParams) -> Box<CoordSpace>;
        fn brite_params(self: &SimulationParams) -> &str;

        fn read_params() -> Box<SimulationParams>;
        fn setup(params: &SimulationParams) -> Box<SimSetup>;
        fn teardown(setup_cfg: Box<SimSetup>, params: &SimulationParams);

        type CloudApp;
        #[Self = "CloudApp"]
        fn spawn(sim_params: &SimulationParams, params: CloudAppParams);

        type FogApp;
        #[Self = "FogApp"]
        fn spawn(sim_params: &SimulationParams, params: FogAppParams);

        type EdgeApp;
        #[Self = "EdgeApp"]
        fn spawn_trash(sim_params: &SimulationParams, params: EdgeAppParams);
        #[Self = "EdgeApp"]
        fn spawn_bikes(sim_params: &SimulationParams, params: EdgeAppParams);

        type UserApp;
        #[Self = "UserApp"]
        fn spawn_trash(sim_params: &SimulationParams, params: UserAppParams);
        #[Self = "UserApp"]
        fn spawn_bikes(sim_params: &SimulationParams, params: UserAppParams);

        type CoordSpace;
        fn euclid_to_geo(self: &CoordSpace, x: f64, y: f64) -> [f64; 2];
        fn brite_to_euclid(self: &CoordSpace, x: f64, y: f64) -> [f64; 2];
        fn brite_to_geo(self: &CoordSpace, x: f64, y: f64) -> [f64; 2];
        fn geo_to_euclid(self: &CoordSpace, lat: f64, lng: f64) -> [f64; 2];
        fn add_node(self: &mut CoordSpace, id: u32, x: f64, y: f64);
        fn remove_node(self: &mut CoordSpace, id: u32, lat: f64, lng: f64) -> Result<usize>;
        fn nearest_node(self: &CoordSpace, lat: f64, lng: f64) -> Result<BriteNodeResult>;
    }

    #[namespace = "nsrs"]
    unsafe extern "C++" {
        include!("nsrs/src/lib.rs.h");

        type IpAddrType = nsrs::ffi::IpAddrType;
        type SocketAddr = nsrs::ffi::SocketAddr;
    }
}

pub struct SimSetup {
    pub container: Container<GenericImage>,
}

pub fn read_params() -> Box<SimulationParams> {
    let path = std::env::var_os("SIM_PARAMS").expect("the SIM_PARAMS env variable must be set");
    match SimulationParams::open(&path) {
        Ok(params) => Box::new(params),
        Err(e) => panic!("failed to open params at {path:?}: {e}"),
    }
}

pub async fn throttle(dt: Duration) {
    nsrs::time::sleep(Duration::from_secs_f64(
        rand::random::<f64>() * dt.as_secs_f64(),
    ))
    .await
}

pub fn setup(params: &SimulationParams) -> Box<SimSetup> {
    let _ = std::env::var("MICELIO_ML_DIRECTORY").expect("failed to get MICELIO_ML_DIRECTORY");
    let jena_image = std::env::var("JENA_FUSEKI_IMAGE").expect("failed to get JENA_FUSEKI_IMAGE");
    let jena_home =
        PathBuf::from(std::env::var("JENA_FUSEKI_HOME").expect("failed to get JENA_FUSEKI_HOME"));
    let data_path = jena_home.join("data");
    let db2 = jena_home.join("databases/DB2");
    std::fs::remove_dir_all(&db2).expect("should clean DB2");
    std::fs::create_dir(&db2).expect("should create empty DB2");
    let container = GenericImage::new(jena_image, "latest".into())
        .with_wait_for(WaitFor::message_on_stdout("Start Fuseki"))
        .with_mapped_port(3030, 3030.tcp())
        .with_container_name("jena-fuseki")
        .with_cmd(["--conf", "config.ttl"])
        .with_mount(Mount::bind_mount(
            jena_home.join("logs").to_str().unwrap(),
            "/fuseki/logs",
        ))
        .with_mount(Mount::bind_mount(
            jena_home.join("databases").to_str().unwrap(),
            "/fuseki/databases",
        ))
        .with_mount(Mount::bind_mount(
            data_path.to_str().unwrap(),
            "/fuseki/data",
        ))
        .with_mount(Mount::bind_mount(
            jena_home.join("config.ttl").to_str().unwrap(),
            "/fuseki/config.ttl",
        ))
        .with_mount(Mount::bind_mount(
            jena_home.join("scripts").to_str().unwrap(),
            "/usr/local/bin",
        ))
        .start()
        .expect("failed to start jena fuseki");

    adjust_config(&container, &jena_home, &data_path, params);
    setup_barriers(params);

    let store_path = std::env::var("STORE_PATH").expect("STORE_PATH should be set");
    std::fs::create_dir_all(&store_path).expect("should create local store path dirs");

    if !container.is_running().unwrap_or(false) {
        let exit_code = container.exit_code().unwrap_or(Some(-1)).unwrap_or(-1);
        container.stop().expect("should stop");
        container.rm().expect("should remove");
        panic!("container is not running!\n exit code: {}", exit_code);
    }
    Box::new(SimSetup { container })
}

fn adjust_config(
    container: &Container<GenericImage>,
    home_path: &PathBuf,
    data_path: &PathBuf,
    params: &SimulationParams,
) {
    for f in params.cloud_layer.init_with.iter() {
        let fname = f.file_name().expect("file should be named");
        std::fs::copy(f, data_path.join(fname)).expect("should copy file");
    }
    let mut ttl_files = walkdir::WalkDir::new(data_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file() && e.file_name().to_str().unwrap_or_default().ends_with(".ttl")
        })
        .map(|e| {
            e.into_path()
                .strip_prefix(home_path)
                .expect("should be relative")
                .to_owned()
        });
    if let Some(ttl) = ttl_files.next() {
        let cmd = ExecCommand::new([
            String::from("s-put"),
            String::from("http://localhost:3030/dataset/data"),
            String::from("default"),
            String::from(ttl.to_string_lossy()),
        ])
        .with_cmd_ready_condition(CmdWaitFor::exit_code(0));
        if let Err(_) = container.exec(cmd) {
            let out = container.stdout_to_vec().unwrap_or_default();
            let out = String::from_utf8_lossy(&out);
            panic!("failed to put data:\n{out}");
        }
    }
    for ttl in ttl_files {
        let cmd = ExecCommand::new([
            String::from("s-post"),
            String::from("http://localhost:3030/dataset/data"),
            String::from("default"),
            String::from(ttl.to_string_lossy()),
        ])
        .with_cmd_ready_condition(CmdWaitFor::exit_code(0));
        if let Err(_) = container.exec(cmd) {
            let out = container.stdout_to_vec().unwrap_or_default();
            let out = String::from_utf8_lossy(&out);
            panic!("failed to post data:\n{out}");
        }
    }
}

pub static INIT_BARRIER: RwLock<Option<Barrier>> = RwLock::new(None);
pub static TRASH_TASK_BARRIER: RwLock<Option<Barrier>> = RwLock::new(None);
pub static BIKES_TASK_BARRIER: RwLock<Option<Barrier>> = RwLock::new(None);
pub static USER_BARRIER: RwLock<Option<Barrier>> = RwLock::new(None);

#[macro_export]
macro_rules! read_barrier {
    ($B:ident) => {
        $B.read()
            .expect(&concat!("should get ", stringify!($B), " barrier lock"))
            .as_ref()
            .expect(&concat!(stringify!($B), " must be initialized"))
            .clone()
    };
}

fn setup_barriers(params: &SimulationParams) {
    setup_barrier(
        &INIT_BARRIER,
        1 + params.n_fog_nodes() + params.n_edge_nodes() + params.n_user_nodes(),
    );
    setup_barrier(&TRASH_TASK_BARRIER, 1 + params.n_trash_user_nodes());
    setup_barrier(&BIKES_TASK_BARRIER, 1 + params.n_bikes_user_nodes());
    setup_barrier(&USER_BARRIER, params.n_user_nodes());
}

fn setup_barrier(barrier: &RwLock<Option<Barrier>>, n: usize) {
    let mut barrier_lock = barrier.write().expect("should get the barrier rwlock");
    *barrier_lock = Some(Barrier::new(n));
}

pub fn teardown(_setup_cfg: Box<SimSetup>, params: &SimulationParams) {
    println!("[teardown]");
    let jena_home =
        PathBuf::from(std::env::var("JENA_FUSEKI_HOME").expect("failed to get JENA_FUSEKI_HOME"));
    let data_path = jena_home.join("data");
    for f in params.cloud_layer.init_with.iter() {
        let fname = f.file_name().expect("file should be named");
        std::fs::remove_file(data_path.join(fname)).expect("should delete init file");
    }
    // let container = setup_cfg.container;
    // let db_path = {
    //     let mut labels = vec!["DB".to_string()];
    //     if let Ok(sim_id) = std::env::var("SIM_ID") {
    //         labels.push(sim_id);
    //     }
    //     format!("/fuseki/databases/{}", labels.join("_"))
    // };
    // let cmd = ExecCommand::new(["cp", "-r", "/fuseki/databases/DB2/", &db_path])
    //     .with_cmd_ready_condition(CmdWaitFor::exit_code(0));
    // if let Err(_) = container.exec(cmd) {
    //     let out = container.stderr_to_vec().unwrap_or_default();
    //     let out = String::from_utf8_lossy(&out);
    //     panic!("failed to copy DB:\n{out}");
    // }
}
