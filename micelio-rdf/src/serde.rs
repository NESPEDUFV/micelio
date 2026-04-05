use crate::prefix::{Name, PrefixedName};
use serde::de::{self, Deserialize, Visitor};
use serde::ser::{self, Serialize};

struct NameVisitor;

impl<'de> Visitor<'de> for NameVisitor {
    type Value = Name;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an IRI reference enclosed in <>, or a TTL prefixed name.")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        v.parse()
            .map_err(|e| E::custom(format!("failed to parse name: {e}")))
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(NameVisitor)
    }
}

impl Serialize for Name {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct PrefixedNameVisitor;

impl<'de> Visitor<'de> for PrefixedNameVisitor {
    type Value = PrefixedName;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a TTL prefixed name.")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        PrefixedName::parse(v).map_err(|e| E::custom(format!("failed to parse prefixed name: {e}")))
    }
}

impl<'de> Deserialize<'de> for PrefixedName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(PrefixedNameVisitor)
    }
}

impl Serialize for PrefixedName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
