use crate::dto::{
    BikeShareDemand, BikeShareEvent, BikeShareEventsAggregate, BikeShareStation, TrashBin,
    TrashItem, WeatherMetrics,
};
use crate::{INIT_BARRIER, read_barrier};
use crate::{ffi, params::SimulationParams};
use chrono::Datelike;
use micelio::dto::{CategorizedImage, EntityImage, Geolocation};
use micelio::edge::client::EdgeClient;
use micelio::kdb::ContextBuffer;
use micelio_rdf::{Namespaced, PrefixedName, RdfType};
use nsrs::sync::Barrier;
use oxiri::Iri;
use oxrdf::{Literal, Term};
use std::collections::HashSet;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};

pub struct EdgeApp;

impl EdgeApp {
    pub fn spawn_trash(sim_params: &SimulationParams, params: ffi::EdgeAppParams) {
        let sim_init_ctx = sim_params.edge_layer.simulate_init_ctx_acquisition;
        let trash_data: Vec<_> = sim_params
            .trash_data(params.node_id)
            .expect("data should be set")
            .iter()
            .cloned()
            .map(|item| item.get_context_instances())
            .collect::<Vec<_>>();

        let barrier: Barrier = read_barrier!(INIT_BARRIER);
        nsrs::spawn_on_context(params.node_id, async move {
            if let Err(e) = Self::run_trash(sim_init_ctx, barrier, trash_data, params).await {
                panic!("[EdgeClient][trash] error:\n{e}");
            }
        });
    }

    async fn run_trash(
        sim_init_ctx: bool,
        barrier: Barrier,
        trash_data: Vec<(EntityImage, Option<CategorizedImage>)>,
        params: ffi::EdgeAppParams,
    ) -> Result<(), Box<dyn Error>> {
        nsrs::log!("[EdgeApp][trash] start");
        let cloud_addr: SocketAddr = params.cloud_addr.try_into()?;
        let client = Arc::new(
            EdgeClient::new(cloud_addr)
                .try_with_prefix(
                    "trash",
                    "http://nesped1.caf.ufv.br/micelio/simulation/trash#",
                )?
                .try_with_prefix("sim", "http://nesped1.caf.ufv.br/micelio/simulation#")?
                .with_name(PrefixedName::new("trash", params.node_name))
                .acquiring_many(vec![
                    Geolocation::rdf_type(),
                    EntityImage::rdf_type(),
                    CategorizedImage::rdf_type(),
                    TrashItem::rdf_type(),
                    TrashBin::rdf_type(),
                ])
                .init()
                .await?,
        );
        let name = client.name();
        let bin_iri = Iri::parse_unchecked(format!(
            "http://nesped1.caf.ufv.br/micelio/simulation/trash#TrashbBin{}",
            params.node_id
        ));
        nsrs::log!("[EdgeApp][trash] {name} created.");
        nsrs::spawn_on_context(params.node_id, client.clone().listen());
        nsrs::log!(
            "[EdgeApp][trash] {name} acquiring {} entries...",
            trash_data.len()
        );
        let client_iri = client.iri();
        let geolocation = Geolocation::new_rad(params.position, client_iri);
        let bin = TrashBin {
            iri: bin_iri.as_ref(),
            node: client_iri,
        };
        if sim_init_ctx {
            let ctx_buffer = client.start_acquisition();
            Self::acquire_trash_context(ctx_buffer, &geolocation, &bin, &trash_data).await?;
        } else {
            let (local_buffer, global_buffer) = client.mock_acquisition();
            Self::acquire_trash_context(local_buffer, &geolocation, &bin, &trash_data).await?;
            Self::acquire_trash_context(global_buffer, &geolocation, &bin, &trash_data).await?;
        }
        nsrs::log!("[EdgeApp][trash] {name} acquired all context.");
        barrier.wait().await;
        Ok(())
    }

    async fn acquire_trash_context(
        mut ctx_buffer: ContextBuffer,
        geolocation: &Geolocation<'_>,
        bin: &TrashBin<'_>,
        data: &[(EntityImage, Option<CategorizedImage>)],
    ) -> Result<(), Box<dyn Error>> {
        ctx_buffer.acquire(geolocation)?;
        ctx_buffer.acquire(bin)?;
        for (ei, ci) in data {
            ctx_buffer.acquire(ei)?;
            if let Some(ci) = ci {
                ctx_buffer.acquire(ci)?;
            }
        }
        ctx_buffer.finish().await?;
        Ok(())
    }

    pub fn spawn_bikes(sim_params: &SimulationParams, params: ffi::EdgeAppParams) {
        let sim_init_ctx = sim_params.edge_layer.simulate_init_ctx_acquisition;
        let barrier: Barrier = read_barrier!(INIT_BARRIER);
        let mut agg_data = sim_params
            .bikes_agg_data(&params.node_name, true)
            .expect("should get agg data");
        agg_data.extend(
            sim_params
                .bikes_agg_data(&params.node_name, false)
                .expect("should get agg data"),
        );
        nsrs::spawn_on_context(params.node_id, async move {
            if let Err(e) = Self::run_bikes(sim_init_ctx, barrier, agg_data, params).await {
                panic!("[EdgeApp][bikes] error:\n{e}");
            }
        });
    }

    async fn run_bikes(
        sim_init_ctx: bool,
        barrier: Barrier,
        agg_data: Vec<BikeShareEventsAggregate>,
        params: ffi::EdgeAppParams,
    ) -> Result<(), Box<dyn Error>> {
        nsrs::log!("[EdgeApp][bikes] start");
        let cloud_addr: SocketAddr = params.cloud_addr.try_into()?;
        let client = Arc::new(
            EdgeClient::new(cloud_addr)
                .try_with_prefix(
                    "bikes",
                    "http://nesped1.caf.ufv.br/micelio/simulation/bikes#",
                )?
                .try_with_prefix("sim", "http://nesped1.caf.ufv.br/micelio/simulation#")?
                .with_name(PrefixedName::new(
                    "bikes",
                    format!("EdgeNode-{}", params.node_name),
                ))
                .acquiring_many(vec![
                    Geolocation::rdf_type(),
                    BikeShareStation::rdf_type(),
                    BikeShareEvent::rdf_type(),
                    WeatherMetrics::rdf_type(),
                    BikeShareDemand::rdf_type(),
                    BikeShareEventsAggregate::rdf_type(),
                ])
                .with_sparql_function(
                    PrefixedName::new("bikes", "isUsHoliday"),
                    &sparql_is_us_holiday,
                )?
                .init()
                .await?,
        );
        let name = client.name();
        nsrs::log!("[EdgeApp][bikes] {name} created.");
        let bss_iri = Iri::parse_unchecked(format!(
            "http://nesped1.caf.ufv.br/micelio/simulation/bikes#{}",
            params.node_name
        ));
        let client_iri = client.iri();
        nsrs::spawn_on_context(params.node_id, client.clone().listen());
        nsrs::log!("[EdgeApp][bikes] {name} acquiring context...");
        let geolocation = Geolocation::new_rad(params.position, client_iri);
        let bss_ctx = BikeShareStation {
            iri: bss_iri.as_ref(),
            node: client_iri,
        };
        if sim_init_ctx {
            nsrs::log!(
                "[EdgeApp][bikes] {name} acquiring {} entries (simulated)...",
                agg_data.len()
            );
            let ctx_buffer = client.start_acquisition();
            Self::acquire_bikes_context(ctx_buffer, &geolocation, &bss_ctx, &agg_data, 150).await?;
        } else {
            nsrs::log!(
                "[EdgeApp][bikes] {name} acquiring {} entries (mocked)...",
                agg_data.len()
            );
            let (local_buffer, global_buffer) = client.mock_acquisition();
            Self::acquire_bikes_context(local_buffer, &geolocation, &bss_ctx, &agg_data, 1000)
                .await?;
            Self::acquire_bikes_context(global_buffer, &geolocation, &bss_ctx, &agg_data, 1000)
                .await?;
        }
        nsrs::log!("[EdgeApp][bikes] {name} acquired all context.");
        barrier.wait().await;
        Ok(())
    }

    async fn acquire_bikes_context(
        mut ctx_buffer: ContextBuffer,
        geolocation: &Geolocation<'_>,
        bss_ctx: &BikeShareStation<'_>,
        data: &[BikeShareEventsAggregate],
        chunk_size: usize,
    ) -> Result<(), Box<dyn Error>> {
        ctx_buffer.acquire(geolocation)?;
        ctx_buffer.acquire(bss_ctx)?;
        for (i, e) in data.iter().enumerate() {
            if let Some(d) = e.demand {
                ctx_buffer.acquire(&BikeShareDemand {
                    demand: d,
                    hourslot: e.hourslot,
                    located_at: e.located_at.clone(),
                })?;
            }
            ctx_buffer.acquire(e)?;
            if i % chunk_size == 0 {
                ctx_buffer.finish().await?;
            }
        }
        ctx_buffer.finish().await?;
        Ok(())
    }
}

fn sparql_is_us_holiday(args: &[Term]) -> Option<Term> {
    let [Term::Literal(ts)] = args else {
        return None;
    };
    let ts = chrono::DateTime::parse_from_rfc3339(ts.value()).ok()?;
    let month = ts.month() as u8;
    let day = ts.day() as u8;
    let weekday = ts.weekday();
    let occurrence = ((day - 1) / 7) as u8;
    let is_election_day = month == 11 && weekday == chrono::Weekday::Tue && (2..=8).contains(&day);
    let result = is_election_day
        || FIXED_HOLIDAYS.contains(&(month, day))
        || NTH_WEEKDAY_HOLIDAYS.contains(&(month, weekday, occurrence))
        || (is_last_weekday_of_month(ts) && LAST_WEEKDAY_HOLIDAYS.contains(&(month, weekday)));
    Some(Literal::from(result).into())
}

fn is_last_weekday_of_month(dt: chrono::DateTime<chrono::FixedOffset>) -> bool {
    dt.checked_add_days(chrono::Days::new(7))
        .map(|next_week| next_week.month() != dt.month())
        .unwrap_or(true)
}

static FIXED_HOLIDAYS: LazyLock<HashSet<(u8, u8)>> = LazyLock::new(|| {
    HashSet::from([
        (1, 1),   // New Year's Day
        (2, 12),  // Lincoln's Birthday
        (6, 19),  // Juneteenth
        (7, 4),   // Independence Day
        (11, 11), // Veterans Day
        (12, 25), // Christmas
    ])
});

static NTH_WEEKDAY_HOLIDAYS: LazyLock<HashSet<(u8, chrono::Weekday, u8)>> = LazyLock::new(|| {
    HashSet::from([
        (1, chrono::Weekday::Mon, 2),  // MLK Day (3rd Monday of Jan)
        (2, chrono::Weekday::Mon, 2),  // Washington's Birthday
        (9, chrono::Weekday::Mon, 0),  // Labor Day
        (10, chrono::Weekday::Mon, 1), // Columbus Day
        (11, chrono::Weekday::Thu, 3), // Thanksgiving (4th Thursday)
    ])
});

static LAST_WEEKDAY_HOLIDAYS: LazyLock<HashSet<(u8, chrono::Weekday)>> = LazyLock::new(|| {
    HashSet::from([
        (5, chrono::Weekday::Mon), // Memorial Day (last monday)
    ])
});
