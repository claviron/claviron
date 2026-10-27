mod overridable;

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{ItemFn, LitStr, Token, parse_macro_input};

/// Arguments for the task attribute macro
struct TaskArgs {
    name: String,
    scope: Option<String>,
    dispatch: Option<String>,
    feature: Option<String>,
    crate_path: Option<String>,
    config: Option<String>,
}

impl Parse for TaskArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;
        let mut scope = None;
        let mut dispatch = None;
        let mut feature = None;
        let mut crate_path = None;
        let mut config: Option<String> = None;

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            let destination = match ident.to_string().as_str() {
                "name" => &mut name,
                "scope" => &mut scope,
                "dispatch" => &mut dispatch,
                "feature" => &mut feature,
                "crate_path" => &mut crate_path,
                "config" => &mut config,
                _ => {
                    return Err(syn::Error::new_spanned(
                        &ident,
                        "unknown task argument",
                    ));
                }
            };
            if destination.is_some() {
                return Err(syn::Error::new_spanned(
                    &ident,
                    "duplicate task argument",
                ));
            }
            let value: LitStr = input.parse()?;
            *destination = Some(value.value());
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(TaskArgs {
            name: name.ok_or_else(|| {
                input.error("task attribute requires 'name' parameter")
            })?,
            scope,
            dispatch,
            feature,
            crate_path,
            config,
        })
    }
}

/// Attribute macro for registering background tasks
///
/// Usage:
/// ```ignore
/// #[task(name = "my_task")]
/// async fn my_task(state: AppState, token: CancellationToken) -> TaskResult<()> {
///     // implementation (default scope: worker, default dispatch: each)
/// }
///
/// // Opt into per-attempt runtime identity (including pool slot number):
/// #[task(name = "my_daemon", scope = "workers", dispatch = "pool")]
/// async fn my_daemon(
///     state: AppState,
///     token: CancellationToken,
///     runtime: TaskRuntime,
/// ) -> TaskResult<()> {
///     tracing::debug!(slot = runtime.slot(), "daemon tick");
/// }
///
/// // Specify candidate scope and per-occurrence dispatch:
/// #[task(name = "my_task", scope = "master")]
/// #[task(name = "my_task", scope = "workers")]
/// #[task(name = "my_task", scope = "fleet", dispatch = "one")]
/// #[task(name = "my_daemon", scope = "fleet", dispatch = "pool")]
///
/// // With feature gate:
/// #[task(name = "my_task", scope = "fleet", feature = "my-feature")]
///
/// // A builtin that carries its own `tasks.yaml` entry, so it exists without
/// // a line in the file; a line under its name replaces the entry whole:
/// #[task(
///     name = "cache_gc",
///     scope = "workers",
///     config = "timing: { kind: interval, every: 1h, skip_on_boot: true }\nattempts: 1\nsilent: true\n"
/// )]
/// async fn my_task(state: AppState, token: CancellationToken) -> TaskResult<()> {
///     // implementation
/// }
/// ```
#[proc_macro_attribute]
pub fn task(
    args: TokenStream,
    input: TokenStream,
) -> TokenStream {
    let args = parse_macro_input!(args as TaskArgs);
    let input_fn = parse_macro_input!(input as ItemFn);

    let task_name = args.name;

    let task_config = match &args.config {
        Some(yaml) => quote! { Some(#yaml) },

        None => quote! { None },
    };
    let tasks_root = if let Some(path_str) = args.crate_path.as_deref() {
        match syn::parse_str::<syn::Path>(path_str) {
            Ok(path) => quote! { #path::tasks },
            Err(_) => {
                return syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!(
                        "Invalid crate_path '{}'. Example: crate_path = \"claviron\"",
                        path_str
                    )
                )
                .to_compile_error()
                .into();
            }
        }
    } else {
        // Default: route everything through the claviron-tasks crate as it
        // appears in the consumer's dependency graph. (Old monolithic default
        // was `crate::tasks`, which assumed a sibling module — wrong now that
        // tasks is its own crate.)
        quote! { ::claviron_tasks }
    };

    let task_scope = match args.scope.as_deref() {
        Some("master") => quote! { #tasks_root::TaskScope::Master },
        Some("workers") | Some("worker") | None => {
            quote! { #tasks_root::TaskScope::Worker }
        }
        Some("fleet") | Some("any") | Some("both") => {
            quote! { #tasks_root::TaskScope::Any }
        }
        Some(other) => {
            return syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("Invalid task scope '{}'. Must be 'master', 'worker', or 'any' (legacy aliases: 'workers', 'fleet', 'both')", other)
            )
            .to_compile_error()
            .into();
        }
    };
    let task_dispatch = match args.dispatch.as_deref() {
        Some("each") | None => quote! { #tasks_root::TaskDispatch::Each },
        Some("one") => quote! { #tasks_root::TaskDispatch::One },
        Some("pool") => quote! { #tasks_root::TaskDispatch::Pool },
        Some(other) => {
            return syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("Invalid task dispatch '{}'. Must be 'each', 'one', or 'pool'", other)
            )
            .to_compile_error()
            .into();
        }
    };

    let fn_name = &input_fn.sig.ident;
    let runner_constructor = match input_fn.sig.inputs.len() {
        2 => quote! { TaskRunner::from(#fn_name) },
        3 => quote! { TaskRunner::from_with_runtime(#fn_name) },
        count => {
            return syn::Error::new_spanned(
                &input_fn.sig,
                format!(
                    "task function must take 2 arguments (AppState, CancellationToken) or 3 arguments (AppState, CancellationToken, TaskRuntime); found {count}"
                )
            )
            .to_compile_error()
            .into();
        }
    };
    let fn_vis = &input_fn.vis;
    let fn_attrs = &input_fn.attrs;
    let fn_sig = &input_fn.sig;
    let fn_block = &input_fn.block;

    // Generate a unique identifier for the registration function
    let registration_fn_name = syn::Ident::new(
        &format!("__task_registration_{}", fn_name),
        fn_name.span(),
    );

    // Generate the registration code
    let registration = if let Some(feat) = args.feature {
        quote! {
            #[cfg(feature = #feat)]
            fn #registration_fn_name() -> #tasks_root::TaskRunner {
                use #tasks_root::TaskRunner;
                #runner_constructor
            }

            #[cfg(feature = #feat)]
            #tasks_root::_macros::inventory::submit! {
                #tasks_root::TaskRegistration {
                    name: #task_name,
                    scope: #task_scope,
                    dispatch: #task_dispatch,
                    create_runner: #registration_fn_name,
                    config: #task_config,
                }
            }
        }
    } else {
        quote! {
            fn #registration_fn_name() -> #tasks_root::TaskRunner {
                use #tasks_root::TaskRunner;
                #runner_constructor
            }

            #tasks_root::_macros::inventory::submit! {
                #tasks_root::TaskRegistration {
                    name: #task_name,
                    scope: #task_scope,
                    dispatch: #task_dispatch,
                    create_runner: #registration_fn_name,
                    config: #task_config,
                }
            }
        }
    };

    // Generate the output: original function + registration
    let output = quote! {
        #(#fn_attrs)*
        #fn_vis #fn_sig {
            #fn_block
        }

        #registration
    };

    output.into()
}

/// Derives the sparse override type and its `apply` for a configuration
/// struct, so a setting is declared once rather than three times.
///
/// See [`overridable`] for the shape it generates and the two escape hatches.
#[proc_macro_derive(Overridable, attributes(overridable))]
pub fn overridable(input: TokenStream) -> TokenStream {
    overridable::derive(input)
}

#[cfg(test)]
#[path = "../tests/task.rs"]
mod task_tests;
