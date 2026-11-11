use std::ffi::{CStr, CString};
use std::fmt;
use std::io;
use std::sync::Arc;

use nix::unistd::{Gid, Group, Uid, User, getegid, geteuid, setgid, setuid};
use serde::Deserialize;

use super::CommandSpec;

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum IdentitySelector {
    Id(u32),
    Name(Box<str>),
}

impl fmt::Display for IdentitySelector {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Id(id) => id.fmt(formatter),
            Self::Name(name) => formatter.write_str(name),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommandIdentity {
    uid: Uid,
    gid: Gid,
    switch: bool,
    groups: Option<Box<[Gid]>>,
}

impl CommandSpec {
    pub(crate) fn prepare_identity(
        &mut self,
        task: &str,
        link_worker: bool,
    ) -> Result<(), String> {
        if self.user.is_none() && self.group.is_none() {
            return Ok(());
        }
        self.identity = Some(Arc::new(CommandIdentity::resolve(
            task,
            self,
            link_worker,
            geteuid(),
            getegid(),
        )?));
        Ok(())
    }
}

impl CommandIdentity {
    fn resolve(
        task: &str,
        spec: &CommandSpec,
        link_worker: bool,
        effective_uid: Uid,
        effective_gid: Gid,
    ) -> Result<Self, String> {
        let user = spec.user.as_ref().map(|selector| {
            let found = match selector {
                IdentitySelector::Id(id) => User::from_uid(Uid::from_raw(*id)),
                IdentitySelector::Name(name) => User::from_name(name),
            };
            found.map_err(|error| {
                format!("task '{task}': command.user '{selector}': {error}")
            })?.ok_or_else(|| {
                format!("task '{task}': command.user '{selector}': unknown user")
            })
        }).transpose()?;
        let group = spec.group.as_ref().map(|selector| {
            let found = match selector {
                IdentitySelector::Id(id) => Group::from_gid(Gid::from_raw(*id)),
                IdentitySelector::Name(name) => Group::from_name(name),
            };
            found.map_err(|error| {
                format!("task '{task}': command.group '{selector}': {error}")
            })?.ok_or_else(|| {
                format!("task '{task}': command.group '{selector}': unknown group")
            })
        }).transpose()?;

        let uid = user.as_ref().map_or(effective_uid, |user| user.uid);
        let gid = group
            .as_ref()
            .map(|group| group.gid)
            .or_else(|| user.as_ref().map(|user| user.gid))
            .unwrap_or(effective_gid);
        let switch = uid != effective_uid || gid != effective_gid;
        if switch {
            let (field, selector) = match &spec.group {
                Some(group) if uid == effective_uid => ("group", group),
                _ => (
                    "user",
                    spec.user.as_ref().expect("a user selected the identity"),
                ),
            };
            if link_worker {
                return Err(format!(
                    "task '{task}': command.{field} '{selector}': the Link worker controller watches the fleet as its own user; the fleet is owner-only by default, and a different worker identity requires fleet group access, which a Link task does not declare"
                ));
            }
            if !effective_uid.is_root() {
                return Err(format!(
                    "task '{task}': command.{field} '{selector}': switching from effective UID {effective_uid} and GID {effective_gid} to UID {uid} and GID {gid} requires root"
                ));
            }
        }

        let groups = match (user, &spec.user, switch) {
            (Some(user), Some(selector), true) => {
                let groups = supplementary_groups(&user, gid).map_err(|error| {
                        format!("task '{task}': command.user '{selector}': resolve supplementary groups: {error}")
                    })?;
                Some(groups)
            }
            _ => None,
        };

        Ok(Self { uid, gid, switch, groups })
    }

    pub(super) fn switches(&self) -> bool {
        self.switch
    }

    /// Called in the child after fork. Every credential is already resolved.
    pub(super) fn apply(&self) -> io::Result<()> {
        if let Some(groups) = &self.groups {
            install_groups(groups)?;
        }
        setgid(self.gid)?;
        if self.groups.is_some() {
            setuid(self.uid)?;
        }
        Ok(())
    }

    pub(super) fn hash(
        &self,
        hasher: &mut blake3::Hasher,
    ) {
        hasher.update(&self.uid.as_raw().to_be_bytes());
        hasher.update(&self.gid.as_raw().to_be_bytes());
        if let Some(groups) = &self.groups {
            for group in groups {
                hasher.update(&group.as_raw().to_be_bytes());
            }
        }
    }
}

fn supplementary_groups(
    user: &User,
    gid: Gid,
) -> io::Result<Box<[Gid]>> {
    let name = CString::new(user.name.as_str())?;
    let mut groups = lookup_groups(&name, user.gid)?;
    groups.sort_unstable_by_key(|group| group.as_raw());
    groups.dedup();

    // Darwin and FreeBSD before 15 treat the first entry as the effective
    // GID. Put the target GID there so setgid cannot replace a membership.
    if cfg!(any(target_vendor = "apple", target_os = "freebsd")) {
        groups.retain(|group| *group != gid);
        groups.insert(0, gid);
        if groups.len() > group_limit()? as usize {
            return Err(io::Error::other(
                "supplementary groups exceed NGROUPS_MAX",
            ));
        }
    }

    Ok(groups.into_boxed_slice())
}

fn group_limit() -> io::Result<nix::libc::c_int> {
    use nix::libc;

    let capacity = unsafe { libc::sysconf(libc::_SC_NGROUPS_MAX) };
    libc::c_int::try_from(capacity)
        .ok()
        .filter(|count| *count > 0)
        .ok_or_else(|| io::Error::other("could not determine NGROUPS_MAX"))
}

#[cfg(not(target_vendor = "apple"))]
fn lookup_groups(
    user: &CStr,
    primary: Gid,
) -> io::Result<Vec<Gid>> {
    Ok(nix::unistd::getgrouplist(user, primary)?)
}

#[cfg(target_vendor = "apple")]
fn lookup_groups(
    user: &CStr,
    primary: Gid,
) -> io::Result<Vec<Gid>> {
    use nix::libc;

    // Darwin exposes getgrouplist with int group IDs and setgroups with gid_t.
    let mut count = group_limit()?;
    let mut groups = vec![0; count as usize];
    let result = unsafe {
        libc::getgrouplist(
            user.as_ptr(),
            primary.as_raw() as libc::c_int,
            groups.as_mut_ptr(),
            &mut count,
        )
    };
    if result < 0 || count < 0 || count as usize > groups.len() {
        return Err(io::Error::other(
            "supplementary groups exceed NGROUPS_MAX",
        ));
    }
    groups.truncate(count as usize);
    Ok(groups
        .into_iter()
        .map(|group| Gid::from_raw(group as libc::gid_t))
        .collect())
}

#[cfg(not(target_vendor = "apple"))]
fn install_groups(groups: &[Gid]) -> io::Result<()> {
    nix::unistd::setgroups(groups)?;
    Ok(())
}

#[cfg(target_vendor = "apple")]
fn install_groups(groups: &[Gid]) -> io::Result<()> {
    use nix::libc;

    // Gid is transparent over gid_t. The list was bounded before fork.
    let result = unsafe {
        libc::setgroups(groups.len() as libc::c_int, groups.as_ptr().cast())
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/process/identity.rs"]
mod tests;
