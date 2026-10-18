use crate::ApplicationId;

/// Boot-prepared execution facts, independent of application identity and transport.
/// Missing facts remain absent; a client must not infer a framework from a name.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize,
)]
#[serde(default, deny_unknown_fields)]
pub struct ApplicationDescription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    #[serde(
        alias = "framework",
        deserialize_with = "claviron_helpers::de::one_or_many"
    )]
    pub frameworks: Vec<String>,
    /// Symbolic client icon code, never a filename or URL. Unknown codes use
    /// the client's runtime default; execution does not depend on this hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runner: Option<String>,
    /// Engine-owned vocabulary: e.g. application/script, ASGI/WSGI or fetch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_mode: Option<String>,
}

impl ApplicationDescription {
    pub fn new(
        runtime: impl Into<String>,
        request_mode: impl Into<String>,
    ) -> Self {
        Self {
            runtime: Some(runtime.into()),
            request_mode: Some(request_mode.into()),
            ..Self::default()
        }
    }

    pub fn framework(
        mut self,
        framework: impl Into<String>,
    ) -> Self {
        self.frameworks = vec![framework.into()];
        self
    }

    /// Informational tags prepared by the execution owner, not capabilities.
    pub fn frameworks(
        mut self,
        frameworks: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.frameworks = frameworks.into_iter().map(Into::into).collect();
        self
    }

    pub fn icon(
        mut self,
        icon: impl Into<String>,
    ) -> Self {
        self.icon = Some(icon.into());
        self
    }

    pub fn runner(
        mut self,
        runner: impl Into<String>,
    ) -> Self {
        self.runner = Some(runner.into());
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationLifecycle {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

/// Cross-runtime pool counters.
///
/// `None` means the execution engine does not expose that counter. It must not
/// be presented as zero by management clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationPoolStatus {
    pub accepting: Option<bool>,
    pub instances: u32,
    pub ready: Option<u32>,
    pub busy: Option<u32>,
    pub retiring: Option<u32>,
    pub backing_off: Option<u32>,
    pub exhausted: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSnapshot {
    pub id: ApplicationId,
    pub description: ApplicationDescription,
    pub lifecycle: ApplicationLifecycle,
    pub enabled: bool,
    pub generation: u64,
    pub pool: Option<ApplicationPoolStatus>,
    pub last_error: Option<String>,
}
