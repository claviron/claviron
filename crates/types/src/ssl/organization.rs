use rcgen::{DistinguishedName, DnType};
use serde::{Deserialize, Serialize};
use x509_parser::certificate::X509Certificate;

use super::Error;

/// The organization an issuer states in the subjects it signs.
///
/// Declared in configuration and read from certificates. Every attribute is
/// optional; an empty one is absent and is not written to a subject.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Organization {
    pub name: Box<str>,
    pub email: Box<str>,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    pub country: Box<str>,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    pub state: Box<str>,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    pub city: Box<str>,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    pub unit: Box<str>,
}

impl Organization {
    pub fn from_name(name: &str) -> Self {
        Self { name: name.into(), ..Default::default() }
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

    /// Add the attributes a signed subject carries: organization name and
    /// country, each only when stated.
    pub(super) fn write(
        &self,
        subject: &mut DistinguishedName,
    ) {
        if !self.name.is_empty() {
            subject.push(DnType::OrganizationName, &*self.name);
        }
        if !self.country.is_empty() {
            subject.push(DnType::CountryName, &*self.country);
        }
    }
}

/// The organization stated in a certificate's subject.
impl<'a> TryFrom<&'a X509Certificate<'a>> for Organization {
    type Error = Error;

    fn try_from(cert: &'a X509Certificate<'a>) -> Result<Self, Self::Error> {
        let subject = cert.subject();
        let mut org = Organization::default();
        if let Some(attr) = subject.iter_organization().next() {
            org.name = attr.as_str()?.into();
        }
        if let Some(attr) = subject.iter_email().next() {
            org.email = attr.as_str()?.into();
        }
        if let Some(attr) = subject.iter_country().next() {
            org.country = attr.as_str()?.into();
        }
        if let Some(attr) = subject.iter_locality().next() {
            org.city = attr.as_str()?.into();
        }
        if let Some(attr) = subject.iter_organizational_unit().next() {
            org.unit = attr.as_str()?.into();
        }
        Ok(org)
    }
}
