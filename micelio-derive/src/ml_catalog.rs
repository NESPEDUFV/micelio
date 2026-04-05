use proc_macro2::TokenStream;
use quote::quote;

pub fn expand(ast: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let ty_name = &ast.ident;
    let algorithms = get_algorithms(ast)?;
    let models = get_models(ast)?;
    let algorithm_iris = impl_algorithm_iris(&algorithms)?;
    let start_algorithm = impl_start_algorithm(&algorithms)?;
    let load_model = impl_load_model(&models)?;
    Ok(quote! {
        impl ::micelio::fl::MlCatalog for #ty_name {
            #algorithm_iris
            #start_algorithm
            #load_model
        }
    })
}

fn get_algorithms(ast: &syn::DeriveInput) -> syn::Result<Vec<syn::TypePath>> {
    ast.attrs
        .iter()
        .filter(|attr| attr.path().is_ident("implements"))
        .map(|attr| attr.parse_args::<syn::TypePath>())
        .collect::<Result<Vec<_>, _>>()
}

fn get_models(ast: &syn::DeriveInput) -> syn::Result<Vec<syn::TypePath>> {
    ast.attrs
        .iter()
        .filter(|attr| attr.path().is_ident("loads"))
        .map(|attr| attr.parse_args::<syn::TypePath>())
        .collect::<Result<Vec<_>, _>>()
}

fn impl_algorithm_iris(types: &Vec<syn::TypePath>) -> syn::Result<TokenStream> {
    let mut inner = TokenStream::new();
    for ty in types.iter() {
        let ty_name = &ty.path;
        inner.extend(quote!(#ty_name::algorithm_iri(),));
    }
    Ok(quote! {
        fn algorithm_iris(&self) -> Vec<::oxiri::Iri<&'static str>> {
            vec![#inner]
        }
    })
}

fn impl_start_algorithm(types: &Vec<syn::TypePath>) -> syn::Result<TokenStream> {
    let mut inner = TokenStream::new();
    for ty in types.iter() {
        let ty_name = &ty.path;
        inner.extend(quote!{
            if iri == #ty_name::algorithm_iri() {
                Some(
                    <#ty_name as ::micelio::fl::MlAlgorithm>::start(params)
                        .map(|a| Box::new(a) as Box<dyn ::micelio::fl::MlAlgorithm>)
                )
            } else
        });
    }
    inner.extend(quote!({None}));
    Ok(quote! {
        fn start_algorithm(
            &self,
            iri: ::oxiri::Iri<&str>,
            params: ::micelio::dto::Config,
        ) -> Option<Result<Box<dyn ::micelio::fl::MlAlgorithm>, Box<dyn ::std::error::Error>>> {
            #inner
        }
    })
}

fn impl_load_model(types: &Vec<syn::TypePath>) -> syn::Result<TokenStream> {
    let mut inner = TokenStream::new();
    for ty in types.iter() {
        let ty_name = &ty.path;
        inner.extend(quote!{
            if iri == #ty_name::algorithm_iri() {
                Some(
                    <#ty_name as ::micelio::fl::MlModel>::load(&dir)
                        .map(|a| Box::new(a) as Box<dyn ::micelio::fl::MlModel>)
                )
            } else
        });
    }
    inner.extend(quote!({None}));
    Ok(quote! {
        fn load_model<'a>(
            &self,
            iri: ::oxiri::Iri<&str>,
            dir: ::micelio::fl::MlDirectory<'a>,
        ) -> Option<::std::io::Result<Box<dyn ::micelio::fl::MlModel>>> {
            let dir = dir.to_path()?;
            #inner
        }
    })
}
