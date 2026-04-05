use micelio::{
    dto::{CategorizedImage, EntityImage},
    kdb::ContextBuffer,
};
use micelio_derive::FromRdf;
use micelio_rdf::{
    FromRdf, RdfType,
    error::{DeriveError, FromRdfError},
};
use oxiri::Iri;
use oxrdf::{Graph, NamedNodeRef, NamedOrBlankNodeRef, TermRef, vocab::rdf};
use std::io;

#[derive(Debug, Clone)]
pub struct InputData {
    pub by_node: usize,
    inner: InputDataInner,
}

#[derive(Debug, Clone)]
pub enum InputDataInner {
    Trash(CategorizedTrashImage),
}

impl RdfType for InputData {
    fn rdf_type() -> Iri<&'static str> {
        Iri::parse_unchecked("http://nesped1.caf.ufv.br/micelio/simulation#CollectedContext")
    }
}

impl<'g> FromRdf<'g> for InputData {
    type Err = DeriveError<'g>;

    fn from_rdf_term(graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term = term.into();
        let node = match term {
            TermRef::NamedNode(node) => Ok(NamedOrBlankNodeRef::NamedNode(node)),
            TermRef::BlankNode(node) => Ok(NamedOrBlankNodeRef::BlankNode(node)),
            _ => Err(FromRdfError::NotNode(term)),
        }?;
        let predicate =
            NamedNodeRef::new_unchecked("http://nesped1.caf.ufv.br/micelio/simulation#byNode");
        let by_node = graph
            .object_for_subject_predicate(node, predicate)
            .ok_or_else(|| FromRdfError::NoMatchingObject {
                subject: node.into(),
                predicate: predicate.into(),
            })
            .and_then(|o| usize::from_rdf_term(graph, o))?;
        graph
            .objects_for_subject_predicate(node, rdf::TYPE)
            .filter_map(|object| match object {
                TermRef::NamedNode(node) => Some(node),
                _ => None,
            })
            .filter_map(|object| {
                if object.as_str() == CategorizedTrashImage::rdf_type().as_str() {
                    let ctx = CategorizedTrashImage::from_rdf_term(graph, term).ok()?;
                    let inner = InputDataInner::Trash(ctx);
                    Some(Self { by_node, inner })
                } else {
                    None
                }
            })
            .next()
            .ok_or_else(|| {
                FromRdfError::NoMatchingObject {
                    subject: node.into(),
                    predicate: rdf::TYPE.into(),
                }
                .into()
            })
    }
}

impl InputData {
    pub async fn acquire_to_client(
        self,
        ctx_buffer: &mut ContextBuffer,
        for_training: bool,
    ) -> io::Result<()> {
        match self.inner {
            InputDataInner::Trash(ctx) => {
                let path = std::path::Path::new(ctx.image.as_str()).with_extension("");
                let name = path
                    .file_name()
                    .expect("path should have file name")
                    .to_string_lossy();
                let trash_piece = Iri::parse_unchecked(format!(
                    "http://nesped1.caf.ufv.br/micelio/simulation/trash#{name}",
                ));
                ctx_buffer
                    .acquire(&EntityImage {
                        represents: trash_piece.clone(),
                        file_path: ctx.image,
                    })
                    .await?;
                if for_training {
                    ctx_buffer
                        .acquire(&CategorizedImage {
                            represents: trash_piece,
                            category: ctx.category,
                            predict_prob: None,
                        })
                        .await?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, FromRdf, Clone)]
#[prefix(trash:"http://nesped1.caf.ufv.br/micelio/simulation/trash#")]
#[rdftype(trash:CategorizedTrashImage)]
pub struct CategorizedTrashImage {
    #[predicate(trash:image)]
    pub image: String,
    #[predicate(trash:category)]
    pub category: Iri<String>,
}
