use super::*;

#[php_class]
#[php(name = "Example\\Runtime\\Currency")]
struct ExampleCurrency {}

struct Types;

impl PhpEmbedExtension for Types {
    fn name(&self) -> &'static str {
        "example-types"
    }

    fn register_classes<'a>(
        &self,
        module: ModuleBuilder<'a>,
    ) -> Result<ModuleBuilder<'a>, String> {
        Ok(module.class::<ExampleCurrency>())
    }
}

#[test]
fn downstream_class_declarations_use_the_object_safe_extension_hook() {
    let extension: &dyn PhpEmbedExtension = &Types;
    // Building declarations does not start PHP, invoke MINIT or boot a runtime.
    assert!(
        extension
            .register_classes(ModuleBuilder::new("example", "0.1"))
            .is_ok()
    );
    assert_eq!(ExampleCurrency::CLASS_NAME, "Example\\Runtime\\Currency");
}
