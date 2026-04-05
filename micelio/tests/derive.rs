use micelio_derive::FromRdf;
use micelio_rdf::GraphDecode;
use oxiri::Iri;
use oxrdf::Graph;
use oxttl::TurtleParser;

#[derive(FromRdf, Debug)]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:Person)]
struct Person<'g> {
    #[subject]
    iri: Iri<String>,
    #[predicate(mcl:hasName)]
    name: &'g str,
    #[predicate(mcl:hasAge)]
    age: u8,
    #[predicates(mcl:likes)]
    liked_things: Vec<String>,
}

#[test]
fn test_decode_person() {
    let ttl_doc = r#"
PREFIX mcl: <http://nesped1.caf.ufv.br/micelio/ontology#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

mcl:Alice a mcl:Person;
    mcl:hasName "Alice";
    mcl:hasAge 24;
    mcl:likes "rain";
    .
    
mcl:Bob a mcl:Person;
    mcl:hasName "Bob";
    mcl:hasAge 37;
    mcl:likes "cats", "dogs", "birds";
    .

mcl:Clark a mcl:Person;
    .
"#;
    let graph = Graph::from_iter(TurtleParser::new().for_slice(ttl_doc).map(|t| t.unwrap()));
    for person in graph.decode_instances::<Person>() {
        match person {
            Ok(person) => println!(
                "({}) Hi, my name is {}, I'm {}, I like {}.",
                person.iri,
                person.name,
                person.age,
                person.liked_things.join(", ")
            ),
            Err(e) => eprintln!("{e}"),
        }
    }
}
