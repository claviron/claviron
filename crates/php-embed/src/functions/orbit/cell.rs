//! `Claviron\Runtime\Orbital`: a typed fleet cell as a PHP value.
//!
//! The object is a handle; the value lives in Orbit shared memory and every
//! process that opens the same id reads and updates the same cell. Operators
//! come from the class's own Zend object handlers, installed once at module
//! startup: `$n++`, `$n += 5`, `$s .= "x"` apply atomically to the cell and
//! hand back the same handle; `$n > 10`, `(int) $n`, `echo $n` read it.
//! `serialize()` carries only the id, so a cell crosses any boundary a PHP
//! value crosses — a cache, an invocation, a session — by reference.

use std::cmp::Ordering;
use std::ffi::c_int;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use claviron_orbit::cell::{CellId, Orbital, TextId};
use claviron_orbit::{Cells, CellsProvider, Text};
use ext_php_rs::boxed::ZBox;
use ext_php_rs::class::RegisteredClass;
use ext_php_rs::exception::PhpException;
use ext_php_rs::ffi::{
    ZEND_RESULT_CODE_FAILURE, ZEND_RESULT_CODE_SUCCESS, zend_object, zval,
};
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::types::{ZendClassObject, ZendHashTable, ZendObject, Zval};
use ext_php_rs::{php_class, php_impl};
use tracing::debug;

// Zend VM opcodes `do_operation` is called with; from zend_vm_opcodes.h.
const ZEND_ADD: u8 = 1;
const ZEND_SUB: u8 = 2;
const ZEND_CONCAT: u8 = 8;

// Cast targets, from zend_types.h.
const IS_LONG: c_int = 4;
const IS_DOUBLE: c_int = 5;
const IS_STRING: c_int = 6;
const IS_BOOL: c_int = 18;

static CELLS: ArcSwapOption<Cells> = ArcSwapOption::const_empty();

pub(crate) fn configure(provider: Arc<CellsProvider>) {
    let cells = provider.handle();
    let enabled = cells.is_some();
    CELLS.store(cells);
    debug!(enabled, "PHP Orbital cells configured");
}

fn cells() -> Result<Arc<Cells>, PhpException> {
    CELLS.load_full().ok_or_else(|| {
        PhpException::from_message(
            "Orbital cells are not available in this process".into(),
        )
    })
}

fn failure(error: impl std::fmt::Display) -> PhpException {
    PhpException::from_message(format!("Orbital: {error}"))
}

#[derive(Clone)]
enum Cell {
    Int(Orbital<i64>),
    Float(Orbital<f64>),
    Bool(Orbital<bool>),
    Text(Text),
}

impl Cell {
    fn id(&self) -> String {
        match self {
            Self::Int(cell) => cell.id().to_string(),
            Self::Float(cell) => cell.id().to_string(),
            Self::Bool(cell) => cell.id().to_string(),
            Self::Text(cell) => cell.id().to_string(),
        }
    }

    fn open(id: &str) -> Result<Self, PhpException> {
        let cells = cells()?;
        if let Some(rest) = id.strip_prefix("cell:") {
            // The scalar id does not say which type; the cell does.
            let cell_id: CellId =
                format!("cell:{rest}").parse().map_err(failure)?;
            return cells
                .open::<i64>(cell_id)
                .map(Self::Int)
                .or_else(|_| cells.open::<f64>(cell_id).map(Self::Float))
                .or_else(|_| cells.open::<bool>(cell_id).map(Self::Bool))
                .map_err(failure);
        }
        let text_id: TextId = id.parse().map_err(failure)?;
        cells.open_text(text_id).map(Self::Text).map_err(failure)
    }

    fn load(&self) -> Result<Loaded, PhpException> {
        Ok(match self {
            Self::Int(cell) => Loaded::Int(cell.load().map_err(failure)?),
            Self::Float(cell) => Loaded::Float(cell.load().map_err(failure)?),
            Self::Bool(cell) => Loaded::Bool(cell.load().map_err(failure)?),
            Self::Text(cell) => Loaded::Text(cell.load().map_err(failure)?),
        })
    }

    fn store(
        &self,
        value: &Zval,
    ) -> Result<(), PhpException> {
        match self {
            Self::Int(cell) => cell.store(expect_long(value)?).map_err(failure),
            Self::Float(cell) => {
                cell.store(expect_double(value)?).map_err(failure)
            }
            Self::Bool(cell) => cell
                .store(
                    value
                        .bool()
                        .ok_or_else(|| failure("a bool cell takes a bool"))?,
                )
                .map_err(failure),
            Self::Text(cell) => cell
                .store(
                    &value.string().ok_or_else(|| {
                        failure("a string cell takes a string")
                    })?,
                )
                .map_err(failure),
        }
    }

    /// `+=` / `-=` / `++` / `--`: atomic on the cell, honest about the type.
    fn add(
        &self,
        by: &Zval,
        sign: i64,
    ) -> Result<Loaded, PhpException> {
        match self {
            Self::Int(cell) => {
                let by = expect_long(by)?
                    .checked_mul(sign)
                    .ok_or_else(|| failure("overflow"))?;
                cell.fetch_add(by).map(Loaded::Int).map_err(failure)
            }
            Self::Float(cell) => cell
                .fetch_add(expect_double(by)? * sign as f64)
                .map(Loaded::Float)
                .map_err(failure),
            Self::Bool(_) => Err(failure("a bool cell has no arithmetic")),
            Self::Text(_) => {
                Err(failure("a string cell has no arithmetic; use .="))
            }
        }
    }

    fn append(
        &self,
        text: &Zval,
    ) -> Result<Loaded, PhpException> {
        let Self::Text(cell) = self else {
            return Err(failure("only a string cell can be appended to"));
        };
        let text =
            text.string().ok_or_else(|| failure("append takes a string"))?;
        cell.append(&text).map_err(failure)?;
        cell.load().map(Loaded::Text).map_err(failure)
    }

    fn version(&self) -> Result<u64, PhpException> {
        match self {
            Self::Int(cell) => cell.version().map(u64::from),
            Self::Float(cell) => cell.version().map(u64::from),
            Self::Bool(cell) => cell.version().map(u64::from),
            Self::Text(cell) => cell.version(),
        }
        .map_err(failure)
    }

    fn wait_changed(
        &self,
        since: u64,
    ) -> Result<u64, PhpException> {
        let scalar = || {
            u32::try_from(since)
                .map_err(|_| failure("wait takes a version from version()"))
        };
        match self {
            Self::Int(cell) => cell.wait_changed(scalar()?).map(u64::from),
            Self::Float(cell) => cell.wait_changed(scalar()?).map(u64::from),
            Self::Bool(cell) => cell.wait_changed(scalar()?).map(u64::from),
            Self::Text(cell) => cell.wait_changed(since),
        }
        .map_err(failure)
    }

    fn release(&self) -> Result<(), PhpException> {
        match self {
            Self::Int(cell) => cell.clone().release(),
            Self::Float(cell) => cell.clone().release(),
            Self::Bool(cell) => cell.clone().release(),
            Self::Text(cell) => cell.clone().release(),
        }
        .map_err(failure)
    }
}

enum Loaded {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

impl Loaded {
    fn write(
        &self,
        target: &mut Zval,
    ) -> Result<(), PhpException> {
        match self {
            Self::Int(value) => target.set_long(*value),
            Self::Float(value) => target.set_double(*value),
            Self::Bool(value) => target.set_bool(*value),
            Self::Text(value) => {
                target.set_string(value, false).map_err(failure)?
            }
        }
        Ok(())
    }

    fn as_string(&self) -> String {
        match self {
            Self::Int(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::Bool(value) => {
                if *value {
                    "1".to_owned()
                } else {
                    String::new()
                }
            }
            Self::Text(value) => value.clone(),
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(value) => Some(*value as f64),
            Self::Float(value) => Some(*value),
            Self::Bool(value) => Some(f64::from(u8::from(*value))),
            Self::Text(_) => None,
        }
    }

    fn compare(
        &self,
        other: &Zval,
    ) -> Option<Ordering> {
        match self {
            Self::Text(value) => {
                other.string().map(|other| value.as_str().cmp(other.as_str()))
            }
            _ => {
                let mine = self.as_f64()?;
                let theirs = other
                    .double()
                    .or_else(|| other.long().map(|value| value as f64))
                    .or_else(|| {
                        other.bool().map(|value| f64::from(u8::from(value)))
                    })?;
                mine.partial_cmp(&theirs)
            }
        }
    }
}

fn expect_long(value: &Zval) -> Result<i64, PhpException> {
    value.long().ok_or_else(|| failure("an int cell takes an int"))
}

fn expect_double(value: &Zval) -> Result<f64, PhpException> {
    value
        .double()
        .or_else(|| value.long().map(|value| value as f64))
        .ok_or_else(|| failure("a float cell takes a number"))
}

/// The PHP handle. `Final` and no dynamic properties: the value is not on
/// the object, it is in the fleet.
#[php_class]
#[php(name = "Claviron\\Runtime\\Orbital")]
#[php(flags = ClassFlags::from_bits_retain(ClassFlags::Final.bits() | ClassFlags::NoDynamicProperties.bits()))]
pub(crate) struct PhpOrbital {
    cell: Option<Cell>,
}

impl PhpOrbital {
    fn cell(&self) -> Result<&Cell, PhpException> {
        self.cell
            .as_ref()
            .ok_or_else(|| failure("this handle is not attached to a cell"))
    }

    fn attached(cell: Cell) -> ZBox<ZendClassObject<Self>> {
        ZendClassObject::new(Self { cell: Some(cell) })
    }
}

#[php_impl]
impl PhpOrbital {
    #[php(defaults(value = 0))]
    pub fn int(
        value: i64
    ) -> Result<ZBox<ZendClassObject<PhpOrbital>>, PhpException> {
        Ok(PhpOrbital::attached(Cell::Int(
            cells()?.allocate(value).map_err(failure)?,
        )))
    }

    #[php(defaults(value = 0.0))]
    pub fn float(
        value: f64
    ) -> Result<ZBox<ZendClassObject<PhpOrbital>>, PhpException> {
        Ok(PhpOrbital::attached(Cell::Float(
            cells()?.allocate(value).map_err(failure)?,
        )))
    }

    #[php(defaults(value = false))]
    pub fn bool(
        value: bool
    ) -> Result<ZBox<ZendClassObject<PhpOrbital>>, PhpException> {
        Ok(PhpOrbital::attached(Cell::Bool(
            cells()?.allocate(value).map_err(failure)?,
        )))
    }

    #[php(defaults(value = "\"\""))]
    pub fn string(
        value: String
    ) -> Result<ZBox<ZendClassObject<PhpOrbital>>, PhpException> {
        Ok(PhpOrbital::attached(Cell::Text(
            cells()?.allocate_text(&value).map_err(failure)?,
        )))
    }

    pub fn open(
        id: String
    ) -> Result<ZBox<ZendClassObject<PhpOrbital>>, PhpException> {
        Ok(PhpOrbital::attached(Cell::open(&id)?))
    }

    pub fn id(&self) -> Result<String, PhpException> {
        Ok(self.cell()?.id())
    }

    pub fn get(&self) -> Result<Zval, PhpException> {
        let mut value = Zval::new();
        self.cell()?.load()?.write(&mut value)?;
        Ok(value)
    }

    pub fn set(
        &self,
        value: &Zval,
    ) -> Result<(), PhpException> {
        self.cell()?.store(value)
    }

    pub fn add(
        &self,
        by: &Zval,
    ) -> Result<Zval, PhpException> {
        let mut value = Zval::new();
        self.cell()?.add(by, 1)?.write(&mut value)?;
        Ok(value)
    }

    pub fn append(
        &self,
        text: &Zval,
    ) -> Result<String, PhpException> {
        Ok(self.cell()?.append(text)?.as_string())
    }

    /// Opaque revision token; pass it unchanged to wait(), even if negative.
    pub fn version(&self) -> Result<i64, PhpException> {
        // PHP integers carry the complete u64 token as the same signed bits.
        Ok(self.cell()?.version()? as i64)
    }

    /// Block until the cell has been written since `$since`, by any process,
    /// and return the current opaque token. Coalescing: many writes while parked wake
    /// once; `get()` reads the latest. Throws once the cell is released.
    pub fn wait(
        &self,
        since: i64,
    ) -> Result<i64, PhpException> {
        Ok(self.cell()?.wait_changed(since as u64)? as i64)
    }

    pub fn release(&mut self) -> Result<(), PhpException> {
        self.cell()?.release()?;
        self.cell = None;
        Ok(())
    }

    pub fn __serialize(&self) -> Result<Vec<(String, String)>, PhpException> {
        Ok(vec![("id".to_owned(), self.cell()?.id())])
    }

    pub fn __unserialize(
        &mut self,
        data: &ZendHashTable,
    ) -> Result<(), PhpException> {
        let id = data
            .get("id")
            .and_then(Zval::string)
            .ok_or_else(|| failure("serialized form carries no id"))?;
        self.cell = Some(Cell::open(&id)?);
        Ok(())
    }

    pub fn __to_string(&self) -> Result<String, PhpException> {
        Ok(self.cell()?.load()?.as_string())
    }
}

/// Patch the class's own handler table, once, after registration. The table
/// keeps its identity, which is what ext-php-rs uses to recognise its
/// objects; only three entries change.
pub(crate) fn install_handlers() {
    let metadata = PhpOrbital::get_metadata();
    let handlers = metadata.handlers() as *const _
        as *mut ext_php_rs::zend::ZendObjectHandlers;
    unsafe {
        (*handlers).do_operation = Some(do_operation);
        (*handlers).compare = Some(compare);
        (*handlers).cast_object = Some(cast_object);
        // ext-php-rs denies serialization for every class with a Rust body,
        // because it cannot know what the body means. This one can:
        // `__serialize` carries the id and nothing else.
        let class =
            metadata.ce() as *const _ as *mut ext_php_rs::zend::ClassEntry;
        (*class).ce_flags &= !ext_php_rs::ffi::ZEND_ACC_NOT_SERIALIZABLE;
        // `unserialize()` builds the object without a constructor and then
        // calls `__unserialize` on it. ext-php-rs leaves such an object's
        // Rust body uninitialised until a constructor runs; this class has
        // an honest empty state, so its objects start there.
        (*class).__bindgen_anon_2.create_object = Some(create_object);
    }
}

/// A detached handle; `__unserialize` or a factory attaches it.
unsafe extern "C" fn create_object(
    _class: *mut ext_php_rs::zend::ClassEntry
) -> *mut zend_object {
    let object = ZendClassObject::new(PhpOrbital { cell: None });
    let raw = ZBox::into_raw(object);
    // SAFETY: into_raw transfers a live ZendClassObject with its embedded header.
    unsafe { &raw mut (*raw).std }
}

unsafe fn orbital_of<'a>(value: *mut zval) -> Option<&'a PhpOrbital> {
    let object: &ZendObject = unsafe { (*value).object()? };
    let raw: &zend_object = object;
    ZendClassObject::<PhpOrbital>::from_zend_obj(raw).map(|wrapped| &**wrapped)
}

/// `$n + 1`, `$n += 1`, `$n++`, `$s . "x"`, `$s .= "x"`: applied to the cell,
/// result is the same handle. Anything else is not an operation a cell has,
/// and PHP reports the unsupported operand types itself.
unsafe extern "C" fn do_operation(
    opcode: u8,
    result: *mut zval,
    op1: *mut zval,
    op2: *mut zval,
) -> c_int {
    let Some(orbital) = (unsafe { orbital_of(op1) }) else {
        return ZEND_RESULT_CODE_FAILURE;
    };
    let Ok(cell) = orbital.cell() else {
        return ZEND_RESULT_CODE_FAILURE;
    };
    let operand: &Zval = unsafe { &*op2 };
    let applied = match opcode {
        ZEND_ADD => cell.add(operand, 1),
        ZEND_SUB => cell.add(operand, -1),
        ZEND_CONCAT => cell.append(operand),
        _ => return ZEND_RESULT_CODE_FAILURE,
    };
    if let Err(exception) = applied {
        exception.throw();
        return ZEND_RESULT_CODE_FAILURE;
    }
    if result != op1 {
        unsafe { std::ptr::write(result, (*op1).shallow_clone()) };
    }
    ZEND_RESULT_CODE_SUCCESS
}

unsafe extern "C" fn compare(
    op1: *mut zval,
    op2: *mut zval,
) -> c_int {
    let ordering = if let Some(orbital) = unsafe { orbital_of(op1) } {
        orbital.cell().ok().and_then(|cell| cell.load().ok()).and_then(
            |value| match unsafe { orbital_of(op2) } {
                Some(other) => other
                    .cell()
                    .ok()
                    .and_then(|cell| cell.load().ok())
                    .and_then(|theirs| {
                        let mut scratch = Zval::new();
                        theirs.write(&mut scratch).ok()?;
                        value.compare(&scratch)
                    }),
                None => value.compare(unsafe { &*op2 }),
            },
        )
    } else if let Some(orbital) = unsafe { orbital_of(op2) } {
        orbital
            .cell()
            .ok()
            .and_then(|cell| cell.load().ok())
            .and_then(|value| value.compare(unsafe { &*op1 }))
            .map(Ordering::reverse)
    } else {
        None
    };
    match ordering {
        Some(Ordering::Less) => -1,
        Some(Ordering::Equal) => 0,
        Some(Ordering::Greater) => 1,
        // Incomparable: PHP's own convention for objects that do not compare.
        None => 1,
    }
}

unsafe extern "C" fn cast_object(
    object: *mut zend_object,
    retval: *mut zval,
    kind: c_int,
) -> c_int {
    let raw: &zend_object = unsafe { &*object };
    let Some(orbital) = ZendClassObject::<PhpOrbital>::from_zend_obj(raw)
        .map(|wrapped| &**wrapped)
    else {
        return ZEND_RESULT_CODE_FAILURE;
    };
    let Ok(value) = orbital.cell().and_then(|cell| cell.load()) else {
        return ZEND_RESULT_CODE_FAILURE;
    };
    // PHP hands over an uninitialised stack zval. ext-php-rs setters release
    // whatever the zval held before, so it must hold nothing first.
    unsafe { retval.write(Zval::new()) };
    let target: &mut Zval = unsafe { &mut *retval };
    match kind {
        IS_LONG => match value {
            Loaded::Int(value) => target.set_long(value),
            Loaded::Float(value) => target.set_long(value as i64),
            Loaded::Bool(value) => target.set_long(i64::from(value)),
            Loaded::Text(_) => return ZEND_RESULT_CODE_FAILURE,
        },
        IS_DOUBLE => match value.as_f64() {
            Some(value) => target.set_double(value),
            None => return ZEND_RESULT_CODE_FAILURE,
        },
        IS_STRING => {
            if target.set_string(&value.as_string(), false).is_err() {
                return ZEND_RESULT_CODE_FAILURE;
            }
        }
        IS_BOOL => target.set_bool(match value {
            Loaded::Int(value) => value != 0,
            Loaded::Float(value) => value != 0.0,
            Loaded::Bool(value) => value,
            Loaded::Text(value) => !value.is_empty() && value != "0",
        }),
        _ => return ZEND_RESULT_CODE_FAILURE,
    }
    ZEND_RESULT_CODE_SUCCESS
}

#[cfg(test)]
#[path = "../../../tests/functions/orbit/cell.rs"]
mod tests;
