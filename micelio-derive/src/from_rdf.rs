use super::{PrefixAttr, parse_str_or_ident};
use micelio_rdf::prefix::{PrefixMap, PrefixedName};
use oxiri::Iri;
use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote};
use syn::{
    Token, Variant,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
};

pub fn expand(ast: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let (pmap, rdf_type) = parse_base_attributes(ast)?;
    let tname = &ast.ident;
    let generics = &ast.generics;
    let (g, impl_params) = if let Some(lt) = generics.lifetimes().next() {
        (
            lt.to_token_stream(),
            generics
                .params
                .iter()
                .map(|t| t.to_token_stream())
                .collect(),
        )
    } else {
        (quote!('g), vec![quote!('g)])
    };

    let (use_subject, init_impl) = match &ast.data {
        syn::Data::Struct(syn::DataStruct { fields, .. }) => {
            let (use_subject, fields_impl, init_impl) = match fields {
                syn::Fields::Named(fields) => parse_named_fields(tname, fields, &pmap),
                syn::Fields::Unnamed(fields) => parse_unnamed_fields(tname, fields, &pmap),
                _ => Err(syn::Error::new(ast.span(), "expected struct with fields")),
            }?;
            Ok((
                use_subject,
                quote! {
                    #fields_impl
                    #init_impl
                },
            ))
        }
        syn::Data::Enum(syn::DataEnum { variants, .. }) => parse_enum_variants(variants, &pmap),
        _ => Err(syn::Error::new(
            ast.span(),
            "expected struct or enum with fields",
        )),
    }?;

    let subject_impl = use_subject.then(|| quote! {
        let subject = match term {
            ::oxrdf::TermRef::BlankNode(node) => Ok(::oxrdf::NamedOrBlankNodeRef::BlankNode(node)),
            ::oxrdf::TermRef::NamedNode(node) => Ok(::oxrdf::NamedOrBlankNodeRef::NamedNode(node)),
            _ => Err(::micelio_rdf::error::FromRdfError::NotNode(term)),
        }.map_err(::micelio_rdf::error::DeriveError::generic)?;
    });

    let from_rdf_impl = quote! {
        impl <#(#impl_params),*> ::micelio_rdf::FromRdf<#g> for #tname #generics {
            type Err = ::micelio_rdf::error::DeriveError<#g>;

            fn from_rdf_term(graph: &#g ::oxrdf::Graph, term: impl Into<::oxrdf::TermRef<#g>>) -> Result<Self, Self::Err> {
                let term = term.into();
                #subject_impl
                #init_impl
            }
        }
    };

    let rdf_type_impl = rdf_type.map(|t| {
        let iri = t.as_str();
        quote! {
            impl #generics ::micelio_rdf::RdfType for #tname #generics {
                fn rdf_type() -> ::oxiri::Iri<&'static str> {
                    ::oxiri::Iri::parse_unchecked(#iri)
                }
            }
        }
    });

    Ok(quote! {
        #from_rdf_impl
        #rdf_type_impl
    })
}

fn parse_base_attributes(ast: &syn::DeriveInput) -> syn::Result<(PrefixMap, Option<Iri<String>>)> {
    let mut pmap = PrefixMap::new();
    let mut rdf_type = None;
    for attr in ast.attrs.iter() {
        match attr.path().get_ident() {
            Some(ident) if ident == "prefix" => {
                let PrefixAttr { name, iri } = attr.parse_args()?;
                pmap.insert(name, iri);
            }
            Some(ident) if ident == "rdftype" => {
                rdf_type = Some(parse_type_attr(&pmap, attr)?);
            }
            _ => {}
        }
    }
    Ok((pmap, rdf_type))
}

fn parse_type_attr(pmap: &PrefixMap, attr: &syn::Attribute) -> syn::Result<Iri<String>> {
    let attr: NamedTermAttr = attr.parse_args()?;
    attr.iri(pmap)
}

enum NamedTermAttr {
    #[allow(unused)]
    Iri(Span, Iri<String>),
    PName(Span, PrefixedName),
}

impl NamedTermAttr {
    fn iri(self, pmap: &PrefixMap) -> syn::Result<Iri<String>> {
        match self {
            NamedTermAttr::Iri(_, iri) => Ok(iri),
            NamedTermAttr::PName(span, pname) => pmap
                .resolve_prefixed(&pname)
                .ok_or_else(|| syn::Error::new(span, "unknown prefix")),
        }
    }
}

impl Parse for NamedTermAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let init_span = input.span();
        let (span, prefix_or_iri) = parse_str_or_ident(&input)?;
        if input.peek(syn::Token![:]) {
            input.parse::<syn::Token![:]>()?;
            let (_, name) = parse_str_or_ident(&input)?;
            Ok(NamedTermAttr::PName(
                span,
                PrefixedName::new(prefix_or_iri, name),
            ))
        } else {
            let iri =
                Iri::parse(prefix_or_iri).map_err(|e| syn::Error::new(init_span, e.to_string()))?;
            Ok(NamedTermAttr::Iri(span, iri))
        }
    }
}

fn parse_named_fields(
    tname: &syn::Ident,
    fields: &syn::FieldsNamed,
    pmap: &PrefixMap,
) -> syn::Result<(bool, TokenStream, TokenStream)> {
    let mut use_subject = false;
    let mut bindings = TokenStream::new();
    let mut inits = Vec::new();
    for (i, field) in fields.named.iter().enumerate() {
        let fname = field.ident.as_ref().unwrap();
        let field_binding = format_ident!("field{i}");
        let (s, fexpr) = parse_field_attr(FieldId::Named(fname), field, pmap)?;
        use_subject |= s;
        bindings.extend(quote! {let #field_binding = #fexpr;});
        inits.push(quote! {#fname: #field_binding});
    }
    Ok((use_subject, bindings, quote!( Ok(#tname { #(#inits),* }) )))
}

fn parse_unnamed_fields(
    tname: &syn::Ident,
    fields: &syn::FieldsUnnamed,
    pmap: &PrefixMap,
) -> syn::Result<(bool, TokenStream, TokenStream)> {
    let mut use_subject = false;
    let mut bindings = TokenStream::new();
    let mut inits = Vec::new();
    for (i, field) in fields.unnamed.iter().enumerate() {
        let field_binding = format_ident!("field{i}");
        let (s, fexpr) = parse_field_attr(FieldId::Unnamed(i), field, pmap)?;
        use_subject |= s;
        bindings.extend(quote! {let #field_binding = #fexpr;});
        inits.push(field_binding);
    }
    Ok((use_subject, bindings, quote!( Ok(#tname ( #(#inits),* )) )))
}

fn parse_enum_variants(
    variants: &Punctuated<Variant, Token![,]>,
    pmap: &PrefixMap,
) -> syn::Result<(bool, TokenStream)> {
    let arms = variants
        .iter()
        .map(|var| {
            if var.fields.is_empty() {
                match var
                    .attrs
                    .iter()
                    .filter_map(|attr| {
                        attr.path()
                            .get_ident()
                            .and_then(get_field_kind)
                            .map(|k| (attr, k))
                    })
                    .next()
                {
                    Some((attr, FieldKind::Subject)) => {
                        let prop: NamedTermAttr = attr.parse_args()?;
                        let iri = prop.iri(pmap)?;
                        let iri = iri.as_str();
                        let var_name = &var.ident;
                        Ok(quote!(#iri => Ok(Self::#var_name),))
                    }
                    None => Ok(quote!(Default::default())),
                    _ => Err(syn::Error::new(
                        var.span(),
                        "enum variants must contain a `subject` attribute",
                    )),
                }
            } else {
                Err(syn::Error::new(
                    var.fields.span(),
                    "currently only unit variants are supported",
                ))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        false,
        quote! {
            let node = match term {
                ::oxrdf::TermRef::NamedNode(node) => Ok(node),
                _ => Err(::micelio_rdf::error::FromRdfError::NotNode(term))
            }?;
            let v = match node.as_str() {
                #(#arms)*
                _ => Err(::micelio_rdf::error::FromRdfError::NotVariant(node)),
            }?;
            Ok(v)
        },
    ))
}

enum FieldId<'a> {
    Named(&'a syn::Ident),
    Unnamed(usize),
}

impl<'a> FieldId<'a> {
    fn derive_map_err(&self) -> TokenStream {
        match self {
            Self::Named(name) => {
                quote!(|e| ::micelio_rdf::error::DeriveError::Named(stringify!(#name), Box::new(e)))
            }
            Self::Unnamed(i) => {
                quote!(|e| ::micelio_rdf::error::DeriveError::Unnamed(#i, Box::new(e)))
            }
        }
    }
}

fn parse_field_attr(
    field_id: FieldId,
    field: &syn::Field,
    pmap: &PrefixMap,
) -> syn::Result<(bool, TokenStream)> {
    match field
        .attrs
        .iter()
        .filter_map(|attr| {
            attr.path()
                .get_ident()
                .and_then(get_field_kind)
                .map(|k| (attr, k))
        })
        .next()
    {
        Some((_, FieldKind::Subject)) => {
            subject_field_attr_expr(field, field_id).map(|e| (false, e))
        }
        Some((attr, FieldKind::Predicate)) => {
            predicate_field_attr_expr(field, field_id, attr, pmap).map(|e| (true, e))
        }
        Some((attr, FieldKind::Predicates)) => {
            predicates_field_attr_expr(field, field_id, attr, pmap).map(|e| (true, e))
        }
        None => Ok((false, quote!(Default::default()))),
    }
}

fn subject_field_attr_expr(field: &syn::Field, field_id: FieldId) -> syn::Result<TokenStream> {
    let ty = &field.ty;
    let maperr = field_id.derive_map_err();
    Ok(quote!(
        <#ty as ::micelio_rdf::decode::FromRdf>
            ::from_rdf_term(graph, term)
            .map_err(#maperr)?
    ))
}

fn predicate_field_attr_expr(
    field: &syn::Field,
    field_id: FieldId,
    attr: &syn::Attribute,
    pmap: &PrefixMap,
) -> syn::Result<TokenStream> {
    let ty = &field.ty;
    let maperr = field_id.derive_map_err();
    let PredicateAttrArgs(prop, default) = attr.parse_args()?;
    let iri = prop.iri(pmap)?;
    let iri = iri.as_str();
    let handle_missing_impl = match default {
        Some(d) => quote!( Ok(#d) ),
        None => quote! {
            Err(::micelio_rdf::error::FromRdfError::NoMatchingObject {
                subject: subject.into(),
                predicate: predicate.into(),
            })
            .map_err(#maperr)
        },
    };
    Ok(quote!({
        let predicate = ::oxrdf::NamedNodeRef::new_unchecked(#iri);
        match graph.object_for_subject_predicate(subject, predicate) {
            Some(object) => {
                <#ty as ::micelio_rdf::FromRdf>::from_rdf_term(graph, object)
                    .map_err(#maperr)
            },
            None => #handle_missing_impl
        }?
    }))
}

struct PredicateAttrArgs(NamedTermAttr, Option<DefaultArg>);

impl Parse for PredicateAttrArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let named_term = input.parse::<NamedTermAttr>()?;
        let default = if input.peek(syn::Token![,]) {
            input.parse::<syn::Token![,]>()?;
            Some(input.parse::<DefaultArg>()?)
        } else {
            None
        };
        Ok(Self(named_term, default))
    }
}

enum DefaultArg {
    Trait,
    Custom(syn::Expr),
}

impl Parse for DefaultArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ident = input.parse::<syn::Ident>()?;
        if ident == "default" {
            if input.peek(syn::Token![=]) {
                input.parse::<syn::Token![=]>()?;
                Ok(Self::Custom(input.parse::<syn::Expr>()?))
            } else {
                Ok(Self::Trait)
            }
        } else {
            Err(syn::Error::new(
                ident.span(),
                "expected `default` or `default = <expr>`",
            ))
        }
    }
}

impl ToTokens for DefaultArg {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            Self::Trait => tokens.extend(quote! { ::core::default::Default::default() }),
            Self::Custom(expr) => expr.to_tokens(tokens),
        }
    }
}

fn predicates_field_attr_expr(
    field: &syn::Field,
    field_id: FieldId,
    attr: &syn::Attribute,
    pmap: &PrefixMap,
) -> syn::Result<TokenStream> {
    let ty = &field.ty;
    let maperr = field_id.derive_map_err();
    let prop: NamedTermAttr = attr.parse_args()?;
    let iri = prop.iri(pmap)?;
    let iri = iri.as_str();
    Ok(quote!({
        let predicate = ::oxrdf::NamedNodeRef::new_unchecked(#iri);
        let objects = graph.objects_for_subject_predicate(subject, predicate);
        <#ty as ::micelio_rdf::FromRdfMulti>::from_rdf_terms(graph, objects)
            .map_err(#maperr)?
    }))
}

fn get_field_kind(ident: &syn::Ident) -> Option<FieldKind> {
    if ident == "subject" {
        Some(FieldKind::Subject)
    } else if ident == "predicate" {
        Some(FieldKind::Predicate)
    } else if ident == "predicates" {
        Some(FieldKind::Predicates)
    } else {
        None
    }
}

enum FieldKind {
    Subject,
    Predicate,
    Predicates,
}
