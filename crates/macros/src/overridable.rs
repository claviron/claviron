//! `#[derive(Overridable)]` — one definition for a configuration setting.
//!
//! Before this, every transport setting was written three times: once in
//! `XConfig`, again in `XOverride`, and again in `XOverride::apply`. Adding a
//! field and forgetting one of the three did not fail to compile — the setting
//! simply never overrode, silently. Eight pairs and twenty-three hand-written
//! `resolve` lines is a lot of surface for a mistake that says nothing.
//!
//! The generated code is what was written by hand, not something cleverer: a
//! sparse `XOverride` of `Override<T>` fields and an `apply` that resolves each
//! against a base. The point is that it can no longer drift from the type it
//! describes.
//!
//! Field shape is taken from the field's type, because that is what decided it
//! by hand too:
//!
//! * `Duration` and `Option<Duration>` get the duration deserialisers, since
//!   YAML spells a duration as `30s` rather than as a number.
//! * A type whose name ends in `Config` is a nested block: its own `Override`
//!   is embedded and applied rather than resolved.
//! * Everything else is `Override<T>`, resolved.
//!
//! Two escape hatches, because inference that cannot be overruled is worse than
//! no inference:
//!
//! * `#[overridable(nested)]` forces the nested shape for a type the naming
//!   convention does not catch.
//! * `#[overridable(deserialize_with = "path")]` keeps the inferred shape and
//!   only replaces the deserialiser, for a value YAML spells differently from
//!   the type it becomes — a header name written as a string, say.
//! * `#[overridable(sparse = "Ty", resolve = "path::to::fn")]` takes over
//!   entirely, for a field whose override is not simply its own type — the one
//!   in this workspace is an H1 header timeout, where an explicit value has to
//!   be distinguished from an inherited one after resolution.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{
    Attribute, Data, DeriveInput, Error, Fields, LitStr, Type,
    parse_macro_input,
};

/// Where the `Override` type and the duration deserialisers live in the crate
/// being derived in. Overridable with `#[overridable(helpers = "...")]`.
const DEFAULT_HELPERS: &str = "crate::config::overrides";

pub(crate) fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match expand(&input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let Data::Struct(data) = &input.data else {
        return Err(Error::new(
            input.span(),
            "Overridable describes a struct of settings",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new(input.span(), "Overridable needs named fields"));
    };

    let container = Container::parse(&input.attrs)?;
    let helpers: syn::Path = syn::parse_str(&container.helpers)?;
    let base = &input.ident;
    // `TcpConfig` → `TcpOverride`, the convention already in use — and the one
    // `nested_override` below relies on when it embeds another block's override.
    let sparse = container.name.map_or_else(
        || {
            let name = base.to_string();
            let stem = name.strip_suffix("Config").unwrap_or(&name);

            format_ident!("{stem}Override")
        },
        |name| format_ident!("{name}"),
    );

    let mut declarations = Vec::new();
    let mut resolutions = Vec::new();

    for field in &fields.named {
        let name = field.ident.as_ref().expect("named field");
        let shape = Shape::of(field)?;

        match shape {
            Shape::Custom { sparse_ty, resolve } => {
                let sparse_ty: Type = syn::parse_str(&sparse_ty)?;
                let resolve: syn::Path = syn::parse_str(&resolve)?;
                // The sparse type still decides how YAML spells it: taking over
                // the resolution is not a reason to lose the deserialiser and
                // start reading `250ms` as a struct.
                let attribute = duration_attribute(&sparse_ty, &container.helpers, field.span())
                    .map_or_else(
                        || quote! { #[serde(default)] },
                        |deserializer| {
                            quote! { #[serde(default, deserialize_with = #deserializer)] }
                        }
                    );
                declarations.push(quote! {
                    #attribute
                    pub #name: #helpers::Override<#sparse_ty>
                });
                resolutions.push(quote! {
                    #name: match &self.#name {
                        #helpers::Override::Inherit => base.#name.clone(),
                        #helpers::Override::Value(value) => #resolve(value.clone())
                    }
                });
            }
            Shape::Nested => {
                let ty = &field.ty;
                let nested = nested_override(ty)?;
                declarations.push(quote! {
                    #[serde(default)]
                    pub #name: #nested
                });
                resolutions
                    .push(quote! { #name: self.#name.apply(&base.#name) });
            }
            Shape::Duration { optional } => {
                let deserializer = LitStr::new(
                    &format!(
                        "{}::{}",
                        container.helpers,
                        if optional { "optional_duration" } else { "duration" }
                    ),
                    field.span(),
                );
                let ty = &field.ty;
                declarations.push(quote! {
                    #[serde(default, deserialize_with = #deserializer)]
                    pub #name: #helpers::Override<#ty>
                });
                resolutions
                    .push(quote! { #name: self.#name.resolve(&base.#name) });
            }
            Shape::Plain { deserialize_with } => {
                let ty = &field.ty;
                match deserialize_with {
                    Some(path) => {
                        let path = LitStr::new(&path, field.span());
                        declarations.push(quote! {
                            #[serde(default, deserialize_with = #path)]
                            pub #name: #helpers::Override<#ty>
                        });
                    }
                    None => declarations.push(quote! {
                        #[serde(default)]
                        pub #name: #helpers::Override<#ty>
                    }),
                }
                resolutions
                    .push(quote! { #name: self.#name.resolve(&base.#name) });
            }
        }
    }

    let doc = format!(
        "Sparse overrides for [`{base}`]. Missing inherits; present replaces."
    );

    Ok(quote! {
        #[doc = #doc]
        #[derive(Clone, Debug, Default, PartialEq, ::serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct #sparse {
            #(#declarations),*
        }

        impl #sparse {
            /// Resolves these overrides against an inherited configuration.
            pub fn apply(&self, base: &#base) -> #base {
                #base {
                    #(#resolutions),*
                }
            }
        }
    })
}

struct Container {
    helpers: String,
    name: Option<String>,
}

impl Container {
    fn parse(attrs: &[Attribute]) -> syn::Result<Self> {
        let mut container =
            Self { helpers: DEFAULT_HELPERS.to_owned(), name: None };

        for attribute in
            attrs.iter().filter(|a| a.path().is_ident("overridable"))
        {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("helpers") {
                    container.helpers =
                        meta.value()?.parse::<LitStr>()?.value();
                } else if meta.path.is_ident("name") {
                    container.name =
                        Some(meta.value()?.parse::<LitStr>()?.value());
                } else {
                    return Err(meta.error("expected `helpers` or `name`"));
                }

                Ok(())
            })?;
        }

        Ok(container)
    }
}

enum Shape {
    Plain { deserialize_with: Option<String> },
    Duration { optional: bool },
    Nested,
    Custom { sparse_ty: String, resolve: String },
}

impl Shape {
    fn of(field: &syn::Field) -> syn::Result<Self> {
        let mut forced_nested = false;
        let mut sparse_ty = None;
        let mut resolve = None;
        let mut deserialize_with = None;

        for attribute in
            field.attrs.iter().filter(|a| a.path().is_ident("overridable"))
        {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("nested") {
                    forced_nested = true;
                } else if meta.path.is_ident("sparse") {
                    sparse_ty = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("resolve") {
                    resolve = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("deserialize_with") {
                    deserialize_with = Some(meta.value()?.parse::<LitStr>()?.value());
                } else {
                    return Err(
                        meta.error("expected `nested`, `deserialize_with`, `sparse` or `resolve`")
                    );
                }

                Ok(())
            })?;
        }

        match (sparse_ty, resolve) {
            (Some(sparse_ty), Some(resolve)) => {
                return Ok(Self::Custom { sparse_ty, resolve });
            }
            (None, None) => {}
            _ => {
                return Err(Error::new(
                    field.span(),
                    "`sparse` and `resolve` describe one override together; give both",
                ));
            }
        }

        if forced_nested {
            return Ok(Self::Nested);
        }
        if deserialize_with.is_some() {
            return Ok(Self::Plain { deserialize_with });
        }

        let name = type_name(&field.ty);

        Ok(match name.as_deref() {
            Some("Duration") => Self::Duration { optional: false },
            Some("Option") if inner_is(&field.ty, "Duration") => {
                Self::Duration { optional: true }
            }
            Some(name) if name.ends_with("Config") => Self::Nested,
            _ => Self::Plain { deserialize_with: None },
        })
    }
}

/// `AcceptRetryConfig` → `AcceptRetryOverride`, matching the naming the derive
/// generates for the nested type itself.
fn nested_override(ty: &Type) -> syn::Result<syn::Ident> {
    let name = type_name(ty).ok_or_else(|| {
        Error::new(ty.span(), "a nested override needs a named type")
    })?;
    let stem = name.strip_suffix("Config").ok_or_else(|| {
        Error::new(ty.span(), "a nested type should be named `…Config`")
    })?;

    Ok(format_ident!("{stem}Override"))
}

/// The deserialiser a duration-shaped type needs, or `None` for anything else.
fn duration_attribute(
    ty: &Type,
    helpers: &str,
    span: proc_macro2::Span,
) -> Option<LitStr> {
    let name = match type_name(ty).as_deref() {
        Some("Duration") => "duration",
        Some("Option") if inner_is(ty, "Duration") => "optional_duration",
        _ => return None,
    };

    Some(LitStr::new(&format!("{helpers}::{name}"), span))
}

fn type_name(ty: &Type) -> Option<String> {
    let Type::Path(path) = ty else {
        return None;
    };

    path.path.segments.last().map(|segment| segment.ident.to_string())
}

fn inner_is(
    ty: &Type,
    expected: &str,
) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    let Some(segment) = path.path.segments.last() else {
        return false;
    };
    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments
    else {
        return false;
    };

    arguments.args.iter().any(|argument| {
        matches!(argument, syn::GenericArgument::Type(inner)
            if type_name(inner).as_deref() == Some(expected))
    })
}
