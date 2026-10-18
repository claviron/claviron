use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Organization {
    pub name: String,
    pub email: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub country: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub state: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub city: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub unit: String,
}

impl Organization {
    pub fn from_name(name: &str) -> Self {
        Self { name: name.to_string(), ..Default::default() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn email(&self) -> &str {
        &self.email
    }

    pub fn country(&self) -> &str {
        &self.country
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn city(&self) -> &str {
        &self.city
    }

    pub fn unit(&self) -> &str {
        &self.unit
    }

    pub fn to_org_name(&self) -> String {
        self.name.clone()
    }
}
