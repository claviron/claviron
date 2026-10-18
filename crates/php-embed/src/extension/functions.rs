//! Own SAPI function entries together with the argument tables Zend borrows.

use ext_php_rs::args::ArgInfo;
use ext_php_rs::zend::FunctionEntry;

/// Functions contributed to the embedded SAPI before PHP startup.
///
/// Push the complete result of [`super::FunctionBuilder::build`]. Claviron
/// retains both the function entries and their argument metadata for the
/// process lifetime, including ZTS thread registration and PHP shutdown.
#[derive(Default)]
pub struct FunctionEntries {
    entries: Vec<FunctionEntry>,
    arg_info: Vec<Box<[ArgInfo]>>,
}

impl FunctionEntries {
    /// Add one function and the argument table its entry points into.
    pub fn push(
        &mut self,
        (entry, arg_info): (FunctionEntry, Box<[ArgInfo]>),
    ) {
        self.entries.push(entry);
        self.arg_info.push(arg_info);
    }

    pub(crate) fn into_raw(mut self) -> *const FunctionEntry {
        self.entries.push(FunctionEntry::end());
        // Zend retains pointers into both vectors beyond request and module
        // shutdown. Freeze the whole owner, as a C SAPI's static tables do.
        Box::leak(Box::new(self)).entries.as_ptr()
    }
}

impl Extend<(FunctionEntry, Box<[ArgInfo]>)> for FunctionEntries {
    fn extend<T: IntoIterator<Item = (FunctionEntry, Box<[ArgInfo]>)>>(
        &mut self,
        functions: T,
    ) {
        for function in functions {
            self.push(function);
        }
    }
}
