use super::PrefixAttr;
use oxiri::Iri;
use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;

pub fn expand(ast: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = match &ast.data {
        syn::Data::Struct(syn::DataStruct { fields, .. }) => Ok(fields),
        _ => Err(syn::Error::new(ast.span(), "expected struct")),
    }?;
    let (i, field) = match fields {
        syn::Fields::Unnamed(fields) => fields
            .unnamed
            .iter()
            .enumerate()
            .filter(|(_, f)| f.attrs.iter().any(|attr| attr.path().is_ident("prefixmap")))
            .next()
            .ok_or_else(|| {
                syn::Error::new(fields.span(), "a field must be marked with #[prefixmap]")
            }),
        syn::Fields::Named(fields) => fields
            .named
            .iter()
            .enumerate()
            .filter(|(_, f)| f.attrs.iter().any(|attr| attr.path().is_ident("prefixmap")))
            .next()
            .ok_or_else(|| {
                syn::Error::new(fields.span(), "a field must be marked with #[prefixmap]")
            }),
        _ => Err(syn::Error::new(
            fields.span(),
            "expected struct with fields",
        )),
    }?;
    let field_accessor = field
        .ident
        .as_ref()
        .map(|i| quote!(self.#i))
        .unwrap_or_else(|| quote!(self.#i));

    let tname = &ast.ident;
    let generics = &ast.generics;
    let init_namespace = expand_init_namespace(ast)?;

    Ok(quote! {
        impl #generics ::micelio_rdf::Namespaced for #tname #generics {
            fn prefixes(&self) -> &::micelio_rdf::PrefixMap {
                &#field_accessor
            }

            fn prefixes_mut(&mut self) -> &mut ::micelio_rdf::PrefixMap {
                &mut #field_accessor
            }

            #init_namespace
        }
    })
}

fn expand_init_namespace(ast: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let mut used = false;
    let mut lines = TokenStream::new();
    for attr in ast.attrs.iter() {
        if attr.path().is_ident("prefix") {
            let pname: PrefixAttr = attr.parse_args()?;
            let prefix = pname.name;
            let iri = pname.iri.as_str();
            lines.extend(
                quote!( p.insert(#prefix.into(), ::oxiri::Iri::parse_unchecked(#iri.into())); ),
            );
            used = true;
        } else if attr.path().is_ident("base") {
            let iri_lit: syn::LitStr = attr.parse_args()?;
            let iri = Iri::parse(iri_lit.value())
                .map_err(|e| syn::Error::new(iri_lit.span(), e.to_string()))?;
            let iri_str = iri.as_str();
            lines.extend(quote!( p.set_base(::oxiri::Iri::parse_unchecked(#iri_str.into())); ));
            used = true;
        }
    }
    if used {
        Ok(quote! {
            fn init_namespace(&mut self) {
                let p = self.prefixes_mut();
                #lines
            }
        })
    } else {
        Ok(quote!())
    }
}
