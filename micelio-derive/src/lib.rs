mod from_rdf;
mod namespaced;
mod ml_catalog;

use oxiri::Iri;
use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};

#[proc_macro_derive(FromRdf, attributes(prefix, rdftype, subject, predicate, predicates))]
pub fn derive_from_rdf(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: syn::DeriveInput = syn::parse_macro_input!(input);
    from_rdf::expand(&ast)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Namespaced, attributes(prefix, base, prefixmap))]
pub fn derive_namespaced(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: syn::DeriveInput = syn::parse_macro_input!(input);
    namespaced::expand(&ast)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(MlCatalog, attributes(implements, loads))]
pub fn derive_ml_catalog(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: syn::DeriveInput = syn::parse_macro_input!(input);
    ml_catalog::expand(&ast)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

struct PrefixAttr {
    name: String,
    iri: Iri<String>,
}

impl Parse for PrefixAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let (_, name) = parse_str_or_ident(&input)?;
        input.parse::<syn::Token![:]>()?;
        let lit = input.parse::<syn::LitStr>()?;
        let iri =
            Iri::parse(lit.value()).map_err(|e| syn::Error::new(lit.span(), e.to_string()))?;
        Ok(PrefixAttr { name, iri })
    }
}

fn parse_str_or_ident(input: &ParseStream) -> syn::Result<(Span, String)> {
    if input.peek(syn::Ident) {
        let name: syn::Ident = input.parse()?;
        Ok((name.span(), name.to_string()))
    } else if input.peek(syn::LitStr) {
        let lit: syn::LitStr = input.parse()?;
        Ok((lit.span(), lit.value()))
    } else {
        Err(syn::Error::new(
            input.span(),
            "expected a string literal or identifier",
        ))
    }
}
