use crate::from_rdf::{
    FieldId, FieldKind, PredicateAttrArgs, get_field_kind, parse_base_attributes,
};
use micelio_rdf::prefix::PrefixMap;
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::spanned::Spanned;

pub fn expand(ast: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let (pmap, rdf_type) = parse_base_attributes(ast)?;
    let tname = &ast.ident;
    let generics = &ast.generics;
    let impl_params = generics
        .params
        .iter()
        .map(|t| t.to_token_stream())
        .collect::<Vec<_>>();

    let (subject, triples, has_rdf_type) = match &ast.data {
        syn::Data::Struct(syn::DataStruct { fields, .. }) => {
            match fields {
                syn::Fields::Named(fields) => parse_named_fields(fields, &pmap),
                // syn::Fields::Unnamed(fields) => parse_unnamed_fields(tname, fields, &pmap),
                _ => Err(syn::Error::new(ast.span(), "expected struct with fields")),
            }
        }
        _ => Err(syn::Error::new(ast.span(), "expected struct")),
    }?;

    let rdf_type_triple = (rdf_type.is_some() || has_rdf_type).then(|| {
        let p = quote!( ::oxrdf::vocab::rdf::TYPE );
        let o = quote!( <Self as ::micelio_rdf::RdfTypeRef>::rdf_type_ref(self)) ;
        quote!( graph.insert(::oxrdf::TripleRef::new(subject, #p, ::oxrdf::NamedNodeRef::from(#o))); )
    });

    Ok(quote! {
        impl <#(#impl_params),*> ::micelio_rdf::ToRdf for #tname #generics {
            fn into_rdf_triples<'g>(
                &'g self,
                graph: &'g mut ::oxrdf::Graph,
                subject: ::oxrdf::NamedOrBlankNodeRef<'g>
            ) -> ::oxrdf::NamedOrBlankNodeRef<'g> {
                #subject
                #rdf_type_triple
                #triples
                subject
            }
        }
    })
}

fn parse_named_fields(
    fields: &syn::FieldsNamed,
    pmap: &PrefixMap,
) -> syn::Result<(Option<TokenStream>, TokenStream, bool)> {
    let mut subject: Option<TokenStream> = None;
    let mut triples = TokenStream::new();
    let mut has_rdf_type = false;
    for field in fields.named.iter() {
        let fname = field.ident.as_ref().unwrap();
        let (s, t, rdftype) = parse_field_attr(FieldId::Named(fname), field, pmap)?;
        if s.is_some() {
            subject = s;
        }
        has_rdf_type |= rdftype;
        triples.extend(t);
    }
    Ok((subject, triples, has_rdf_type))
}

fn parse_field_attr(
    field_id: FieldId,
    field: &syn::Field,
    pmap: &PrefixMap,
) -> syn::Result<(Option<TokenStream>, TokenStream, bool)> {
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
            subject_field_attr_expr(field_id).map(|(s, t)| (Some(s), t, false))
        }
        Some((_, FieldKind::Type)) => {
            Ok((None, quote!(), true))
        }
        Some((attr, FieldKind::Predicate)) => {
            predicate_field_attr_expr(field_id, attr, pmap).map(|t| (None, t, false))
        }
        Some((attr, FieldKind::Predicates)) => {
            predicates_field_attr_expr(field_id, attr, pmap).map(|t| (None, t, false))
        }
        None => Err(syn::Error::new(field.span(), "expected predicate")),
    }
}

fn subject_field_attr_expr(field_id: FieldId) -> syn::Result<(TokenStream, TokenStream)> {
    let att = match field_id {
        FieldId::Named(fname) => quote!(self.#fname),
        FieldId::Unnamed(i) => quote!(self.#i),
    };
    let s = quote!( let subject: ::oxrdf::NamedOrBlankNodeRef = ::oxrdf::NamedNodeRef::from(#att.as_ref()).into(); );
    Ok((s, quote!()))
}

fn predicate_field_attr_expr(
    field_id: FieldId,
    attr: &syn::Attribute,
    pmap: &PrefixMap,
) -> syn::Result<TokenStream> {
    let att = match field_id {
        FieldId::Named(fname) => quote!(self.#fname),
        FieldId::Unnamed(i) => quote!(self.#i),
    };
    let PredicateAttrArgs(prop, _) = attr.parse_args()?;
    let iri = prop.iri(pmap)?;
    let iri_str = iri.as_str();
    let p = quote!(::oxrdf::NamedNodeRef::new_unchecked(#iri_str));
    let o = quote!( &::micelio_rdf::TermAdapter::from(&#att) );
    Ok(quote!( graph.insert(::oxrdf::TripleRef::new(subject, #p, #o)); ))
}

fn predicates_field_attr_expr(
    field_id: FieldId,
    attr: &syn::Attribute,
    pmap: &PrefixMap,
) -> syn::Result<TokenStream> {
    let att = match field_id {
        FieldId::Named(fname) => quote!(self.#fname),
        FieldId::Unnamed(i) => quote!(self.#i),
    };
    let PredicateAttrArgs(prop, _) = attr.parse_args()?;
    let iri = prop.iri(pmap)?;
    let iri_str = iri.as_str();
    let p = quote!(::oxrdf::NamedNodeRef::new_unchecked(#iri_str));
    Ok(quote! {
        for t in #att.iter() {
            graph.insert(::oxrdf::TripleRef::new(subject, #p, &::micelio_rdf::TermAdapter::from(t)));
        }
    })
}
