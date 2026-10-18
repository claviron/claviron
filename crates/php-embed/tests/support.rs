use std::path::Path;

/// Existing, deliberately non-runnable input for configuration-only tests.
pub(crate) struct ScriptFixture(tempfile::NamedTempFile);

impl ScriptFixture {
    pub(crate) fn new() -> Self {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            include_str!("fixtures/config/program.php"),
        )
        .unwrap();
        Self(file)
    }

    pub(crate) fn path(&self) -> &Path {
        self.0.path()
    }
}
