use chrono::{DateTime, Utc};
use micelio::dto::{CategorizedImage, EntityImage};
use micelio_derive::{FromRdf, ToRdf};
use oxiri::Iri;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct CategorizedTrashImage {
    pub image: String,
    pub category: Iri<String>,
    pub for_training: bool,
}

impl CategorizedTrashImage {
    pub fn get_context_instances(self) -> (EntityImage, Option<CategorizedImage>) {
        let path = std::path::Path::new(self.image.as_str()).with_extension("");
        let name = path
            .file_name()
            .expect("path should have file name")
            .to_string_lossy();
        let trash_piece = Iri::parse_unchecked(format!(
            "http://nesped1.caf.ufv.br/micelio/simulation/trash#{name}",
        ));
        let entity_image = EntityImage {
            represents: trash_piece.clone(),
            file_path: self.image,
        };
        let categorized_image = if self.for_training {
            Some(CategorizedImage {
                represents: trash_piece,
                category: self.category,
                predict_prob: None,
            })
        } else {
            None
        };
        (entity_image, categorized_image)
    }
}

#[allow(unused)]
#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(trash:"http://nesped1.caf.ufv.br/micelio/simulation/trash#")]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(trash:TrashItem)]
pub struct TrashItem<'a> {
    #[subject]
    pub iri: Iri<&'a str>,
}

#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(trash:"http://nesped1.caf.ufv.br/micelio/simulation/trash#")]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(trash:TrashBin)]
pub struct TrashBin<'a> {
    #[subject]
    pub iri: Iri<&'a str>,
    #[predicate(mcl:contains)]
    pub node: Iri<&'a str>,
}

#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(bikes:"http://nesped1.caf.ufv.br/micelio/simulation/bikes#")]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(bikes:BikeShareStation)]
pub struct BikeShareStation<'a> {
    #[subject]
    pub iri: Iri<&'a str>,
    #[predicate(mcl:contains)]
    pub node: Iri<&'a str>,
}

#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(bikes:"http://nesped1.caf.ufv.br/micelio/simulation/bikes#")]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(bikes:BikeShareEvent)]
pub struct BikeShareEvent<'a> {
    #[predicate(mcl:acquiredAt)]
    pub acquired_at: DateTime<Utc>,
    #[predicate(mcl:locatedAt)]
    pub located_at: Iri<&'a str>,
}

#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(bikes:"http://nesped1.caf.ufv.br/micelio/simulation/bikes#")]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[rdftype(bikes:WeatherMetrics)]
pub struct WeatherMetrics {
    #[predicate(bikes:hourSlot)]
    pub hourslot: DateTime<Utc>,
    #[predicate(bikes:temperature)]
    pub temperature: f32,
    #[predicate(bikes:relativeHumidity)]
    pub relative_humidity: f32,
    #[predicate(bikes:precipitation)]
    pub precipitation: f32,
    #[predicate(bikes:windSpeed)]
    pub wind_speed: f32,
    #[predicate(bikes:cloudCoverage)]
    pub cloud_coverage: f32,
    #[predicate(bikes:airPressure)]
    pub air_pressure: f32,
    #[predicate(bikes:weatherCondition)]
    pub weather_condition: u16,
}


#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(bikes:"http://nesped1.caf.ufv.br/micelio/simulation/bikes#")]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[rdftype(bikes:BikeShareEventsAggregate)]
pub struct BikeShareEventsAggregate {
    #[predicate(bikes:hourSlot)]
    pub hourslot: DateTime<Utc>,
    #[predicate(mcl:locatedAt)]
    pub located_at: Iri<String>,
    #[predicate(bikes:temperature)]
    pub temperature: f32,
    #[predicate(bikes:relativeHumidity)]
    pub relative_humidity: f32,
    #[predicate(bikes:precipitation)]
    pub precipitation: f32,
    #[predicate(bikes:windSpeed)]
    pub wind_speed: f32,
    #[predicate(bikes:cloudCoverage)]
    pub cloud_coverage: f32,
    #[predicate(bikes:airPressure)]
    pub air_pressure: f32,
    // #[predicate(bikes:weatherCondition)]
    // pub weather_condition: u16,
    #[predicate(bikes:isHoliday)]
    pub is_holiday: f32,
    #[predicate(bikes:isWeekend)]
    pub is_weekend: f32,
    #[predicate(bikes:month)]
    pub month: f32,
    #[predicate(bikes:hour)]
    pub hour: f32,
    #[predicate(mcl:latitude)]
    pub latitude: f32,
    #[predicate(mcl:longitude)]
    pub longitude: f32,
    #[predicates(bikes:demand)]
    pub demand: Option<u32>,
}


#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(bikes:"http://nesped1.caf.ufv.br/micelio/simulation/bikes#")]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[rdftype(bikes:BikeShareDemand)]
pub struct BikeShareDemand {
    #[predicate(bikes:hourSlot)]
    pub hourslot: DateTime<Utc>,
    #[predicate(mcl:locatedAt)]
    pub located_at: Iri<String>,
    #[predicate(bikes:demand)]
    pub demand: u32,
}
