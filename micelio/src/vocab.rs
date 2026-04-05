pub mod mcl {
    use oxiri::Iri;
    use oxrdf::NamedNodeRef;

    #[macro_export]
    macro_rules! mcl {
        ($name:literal) => {
            NamedNodeRef::new_unchecked(concat!(
                "http://nesped1.caf.ufv.br/micelio/ontology#",
                $name
            ))
        };
    }

    pub(crate) use mcl;

    pub fn custom(name: impl AsRef<str>) -> Iri<String> {
        Iri::parse_unchecked(format!(
            "http://nesped1.caf.ufv.br/micelio/ontology#{}",
            name.as_ref()
        ))
    }

    pub const VISIBILITY: NamedNodeRef<'static> = mcl!("visibility");
    pub const PRIVATE: NamedNodeRef<'static> = mcl!("Private");
    pub const PUBLIC: NamedNodeRef<'static> = mcl!("Public");
    pub const EDGE_NODE: NamedNodeRef<'static> = mcl!("EdgeNode");
    pub const FOG_NODE: NamedNodeRef<'static> = mcl!("FogNode");
    pub const CLOUD_NODE: NamedNodeRef<'static> = mcl!("CloudNode");
    pub const ML_ALGORITHM: NamedNodeRef<'static> = mcl!("MlAlgorithm");
    pub const FL_TASK: NamedNodeRef<'static> = mcl!("LearningTaskLayout");
    pub const DEPENDS_ON: NamedNodeRef<'static> = mcl!("dependsOn");
}

pub mod task {
    use oxiri::Iri;

    #[macro_export]
    macro_rules! task {
        ($name:literal) => {
            NamedNodeRef::new_unchecked(concat!("http://nesped1.caf.ufv.br/micelio/tasks#", $name))
        };
    }
    pub(crate) use task;

    pub fn new() -> Iri<String> {
        let id = uuid::Uuid::new_v4().as_hyphenated().to_string();
        Iri::parse_unchecked(format!("http://nesped1.caf.ufv.br/micelio/tasks#{id}"))
    }
}

pub mod model {
    use oxiri::Iri;

    // #[macro_export]
    // macro_rules! model {
    //     ($name:literal) => {
    //         NamedNodeRef::new_unchecked(concat!("http://nesped1.caf.ufv.br/micelio/models#", $name))
    //     };
    // }
    // pub(crate) use model;

    pub fn new() -> Iri<String> {
        let id = uuid::Uuid::new_v4().as_hyphenated().to_string();
        Iri::parse_unchecked(format!("http://nesped1.caf.ufv.br/micelio/models#{id}"))
    }
}
