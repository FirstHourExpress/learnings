use std::collections::HashMap;

#[derive(Debug)]
struct Requirement {
    name: String,
    version: String,
}

impl Requirement {
    fn new(name: &str, version: &str) -> Requirement {
        Requirement {
            name: name.to_string(),
            version: version.to_string(),
        }
    }
}

fn main() {
    let mut software_requirements = HashMap::new();
    software_requirements.insert("Language", Requirement::new("Rust", "1.56.0"));
    software_requirements.insert("Database", Requirement::new("PostgreSQL", "13.4"));
    software_requirements.insert("Web Framework", Requirement::new("Rocket", "0.5.0"));
    software_requirements.insert("Template Engine", Requirement::new("Handlebars", "4.5.3"));

    // Filter for requirements with a version number greater than or equal to "1.0"
    let filtered_requirements: HashMap<_, _> = software_requirements
        .into_iter()
        .filter(|(_, requirement)| requirement.version >= "1.0".to_string())
        .collect();

    println!("{:?}", filtered_requirements);
}
