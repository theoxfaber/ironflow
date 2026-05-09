use super::definition::{DagDefinition, TaskDefinition};
use anyhow::{Context, Result};
use std::path::Path;

pub struct DagParser;

impl DagParser {
    /// Parse a TOML file into a DagDefinition
    pub fn parse_file<P: AsRef<Path>>(path: P) -> Result<DagDefinition> {
        let content = std::fs::read_to_string(&path)
            .context("Failed to read DAG file")?;
        Self::parse_string(&content)
    }

    /// Parse a TOML string into a DagDefinition
    pub fn parse_string(content: &str) -> Result<DagDefinition> {
        #[derive(serde::Deserialize)]
        struct RawDag {
            dag: RawDagConfig,
        }

        #[derive(serde::Deserialize)]
        struct RawDagConfig {
            id: String,
            description: Option<String>,
            schedule: Option<String>,
            max_active_runs: Option<u32>,
            catchup: Option<bool>,
            tasks: Option<Vec<serde_json::Value>>,
        }

        let raw: RawDag = toml::from_str(content)
            .context("Failed to parse TOML")?;

        let dag = raw.dag;
        let tasks = if let Some(tasks) = dag.tasks {
            tasks
                .into_iter()
                .map(|task_value| {
                    serde_json::from_value::<TaskDefinition>(task_value)
                        .context("Failed to parse task definition")
                })
                .collect::<Result<Vec<_>>>()
        } else {
            Ok(Vec::new())
        }?;

        Ok(DagDefinition {
            id: dag.id,
            description: dag.description,
            schedule: dag.schedule,
            max_active_runs: dag.max_active_runs,
            catchup: dag.catchup,
            tasks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_dag() {
        let toml_content = r#"
[dag]
id = "test_dag"
description = "A test DAG"
schedule = "0 2 * * *"

[[dag.tasks]]
id = "extract"
operator = "bash"
command = "echo 'extracting'"

[[dag.tasks]]
id = "transform"
operator = "bash"
command = "echo 'transforming'"
depends_on = ["extract"]
"#;

        let dag = DagParser::parse_string(toml_content).unwrap();
        assert_eq!(dag.id, "test_dag");
        assert_eq!(dag.tasks.len(), 2);
        assert_eq!(dag.tasks[0].id, "extract");
        assert_eq!(dag.tasks[1].id, "transform");
        assert_eq!(dag.tasks[1].dependencies(), vec!["extract"]);
    }

    #[test]
    fn test_parse_complex_dag() {
        let toml_content = r#"
[dag]
id = "etl_pipeline"
description = "Daily ETL pipeline"
schedule = "0 2 * * *"
max_active_runs = 1
catchup = false

[[dag.tasks]]
id = "extract"
operator = "bash"
command = "python scripts/extract.py"
retries = 3
retry_delay_secs = 300
timeout_secs = 3600

[[dag.tasks]]
id = "transform"
operator = "bash"
command = "python scripts/transform.py"
depends_on = ["extract"]
retries = 2

[[dag.tasks]]
id = "validate"
operator = "http"
url = "https://api.internal/validate"
method = "POST"
depends_on = ["transform"]

[[dag.tasks]]
id = "load"
operator = "bash"
command = "python scripts/load.py"
depends_on = ["validate"]
"#;

        let dag = DagParser::parse_string(toml_content).unwrap();
        assert_eq!(dag.id, "etl_pipeline");
        assert_eq!(dag.tasks.len(), 4);
        assert!(dag.schedule.is_some());
        assert_eq!(dag.max_active_runs, Some(1));
    }
}
