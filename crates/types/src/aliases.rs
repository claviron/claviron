use std::ops::Deref;
use std::slice::Iter;
use std::vec::IntoIter;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{Host, KeyString, Sni};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Aliases(pub Vec<Host>);

impl Aliases {
    /// Create a new empty alias list.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Push a new alias host, returns true if actually inserted.
    pub fn push_unique(
        &mut self,
        host: Host,
    ) -> bool {
        if self.0.contains(&host) {
            false
        } else {
            self.0.push(host);
            true
        }
    }

    /// Extend with unique hosts.
    pub fn extend_unique<I: IntoIterator<Item = Host>>(
        &mut self,
        iter: I,
    ) {
        for h in iter {
            let _ = self.push_unique(h);
        }
    }

    /// Return as a slice.
    pub fn as_slice(&self) -> &[Host] {
        &self.0
    }

    /// Iterate over SNI hostnames only (borrowed; no allocation)
    pub fn domains(&self) -> impl Iterator<Item = &Sni> {
        self.0.iter().filter_map(|h| match h {
            Host::Dns(sni) => Some(sni),
            _ => None,
        })
    }

    /// Convert into inner Vec.
    pub fn into_inner(self) -> Vec<Host> {
        self.0
    }
}

impl Deref for Aliases {
    type Target = [Host];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<Host>> for Aliases {
    fn from(v: Vec<Host>) -> Self {
        Self(v)
    }
}

impl From<&[Sni]> for Aliases {
    fn from(v: &[Sni]) -> Self {
        Self(v.iter().cloned().map(Host::Dns).collect())
    }
}

impl From<&Vec<Sni>> for Aliases {
    fn from(v: &Vec<Sni>) -> Self {
        Aliases::from(v.as_slice())
    }
}

impl From<Vec<Sni>> for Aliases {
    fn from(v: Vec<Sni>) -> Self {
        // Map each SNI into a Host::Dns and collect
        Self(v.into_iter().map(Host::Dns).collect())
    }
}

// Also allow collecting directly from any iterator of Sni
impl FromIterator<Sni> for Aliases {
    fn from_iter<I: IntoIterator<Item = Sni>>(iter: I) -> Self {
        Aliases(iter.into_iter().map(Host::Dns).collect())
    }
}

impl From<Aliases> for Vec<String> {
    fn from(a: Aliases) -> Self {
        a.into_inner().into_iter().map(|h| h.key_string()).collect()
    }
}

impl From<&Aliases> for Vec<String> {
    fn from(a: &Aliases) -> Self {
        a.iter().map(|h| h.key_string()).collect()
    }
}

impl IntoIterator for Aliases {
    type Item = Host;
    type IntoIter = IntoIter<Host>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Aliases {
    type Item = &'a Host;
    type IntoIter = Iter<'a, Host>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'de> Deserialize<'de> for Aliases {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum OneOrMany {
            One(Host),
            Many(Vec<Host>),
        }

        match OneOrMany::deserialize(de)? {
            OneOrMany::One(x) => Ok(Aliases(vec![x])),
            OneOrMany::Many(xs) => Ok(Aliases(xs)),
        }
    }
}
