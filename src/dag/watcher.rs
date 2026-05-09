use anyhow::Result;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use crate::dag::{DagDefinition, DagParser, DagValidator};

#[derive(Clone)]
pub struct DagWatcher {
    dags: Arc<Mutex<HashMap<String, DagDefinition>>>,
}

impl DagWatcher {
    pub fn new() -> Self {
        DagWatcher {
            dags: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Load all DAG files from a directory
    pub fn load_dags_from_directory<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().map(|e| e == "toml").unwrap_or(false) {
                match DagParser::parse_file(&path) {
                    Ok(dag) => {
                        if let Err(e) = DagValidator::validate(&dag) {
                            eprintln!("Failed to validate DAG from {}: {}", path.display(), e);
                            continue;
                        }
                        
                        let mut dags = self.dags.lock().unwrap();
                        dags.insert(dag.id.clone(), dag);
                        println!("Loaded DAG from {}", path.display());
                    }
                    Err(e) => {
                        eprintln!("Failed to parse DAG from {}: {}", path.display(), e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Get all loaded DAGs
    pub fn get_all_dags(&self) -> Result<Vec<DagDefinition>> {
        Ok(self
            .dags
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect())
    }

    /// Get a specific DAG by ID
    pub fn get_dag(&self, dag_id: &str) -> Result<Option<DagDefinition>> {
        Ok(self.dags.lock().unwrap().get(dag_id).cloned())
    }

    /// Watch a directory for changes and reload DAGs
    pub async fn watch_directory<P: AsRef<Path>>(
        &self, 
        path: P,
        store: Arc<crate::store::Store>,
        scheduler: Arc<crate::scheduler::DagScheduler>,
    ) -> Result<()> {
        use notify::{RecommendedWatcher, RecursiveMode, Watcher, Config, EventKind};
        use notify::event::ModifyKind;
        use tokio::sync::mpsc;
        
        let path = path.as_ref().to_path_buf();
        
        // Channel for receiving file events
        let (tx, mut rx) = mpsc::unbounded_channel();
        
        // Create the watcher
        let mut watcher = RecommendedWatcher::new(
            move |res| {
                if let Ok(event) = res {
                    let _ = tx.send(event);
                }
            },
            Config::default(),
        )?;
        
        // Watch the directory
        watcher.watch(&path, RecursiveMode::Recursive)?;
        
        tracing::info!("Started watching directory for DAG changes: {}", path.display());
        
        // Process events
        while let Some(event) = rx.recv().await {
            match event.kind {
                EventKind::Create(_) | EventKind::Modify(ModifyKind::Data(_)) => {
                    for path in event.paths {
                        if path.extension().map(|e| e == "toml").unwrap_or(false) {
                            tracing::info!("Detected change in DAG file: {}", path.display());
                            
                            // Parse and validate the new DAG
                            match DagParser::parse_file(&path) {
                                Ok(dag) => {
                                    if let Err(e) = DagValidator::validate(&dag) {
                                        tracing::error!("Failed to validate modified DAG from {}: {}", path.display(), e);
                                        continue;
                                    }
                                    
                                    // Update our internal cache
                                    {
                                        let mut dags = self.dags.lock().unwrap();
                                        dags.insert(dag.id.clone(), dag.clone());
                                    }
                                    
                                    // Save to DB
                                    if let Err(e) = store.save_dag(&dag).await {
                                        tracing::error!("Failed to save modified DAG to DB: {}", e);
                                    }
                                    
                                    // Schedule the DAG
                                    if let Err(e) = scheduler.schedule_dag(&dag).await {
                                        tracing::error!("Failed to schedule modified DAG: {}", e);
                                    }
                                    
                                    tracing::info!("Successfully hot-reloaded DAG: {}", dag.id);
                                }
                                Err(e) => {
                                    tracing::error!("Failed to parse modified DAG from {}: {}", path.display(), e);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        
        Ok(())
    }
}

impl Default for DagWatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_load_dags_from_directory() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let dag_path = temp_dir.path().join("test_dag.toml");

        let toml_content = r#"
[dag]
id = "test_dag"
description = "Test DAG"

[[dag.tasks]]
id = "task_a"
operator = "bash"
command = "echo 'test'"
"#;

        std::fs::write(&dag_path, toml_content)?;

        let watcher = DagWatcher::new();
        watcher.load_dags_from_directory(temp_dir.path())?;

        let dags = watcher.get_all_dags()?;
        assert_eq!(dags.len(), 1);
        assert_eq!(dags[0].id, "test_dag");

        Ok(())
    }

    #[test]
    fn test_get_dag_by_id() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let dag_path = temp_dir.path().join("test_dag.toml");

        let toml_content = r#"
[dag]
id = "my_dag"

[[dag.tasks]]
id = "task1"
operator = "bash"
command = "echo 'test'"
"#;

        std::fs::write(&dag_path, toml_content)?;

        let watcher = DagWatcher::new();
        watcher.load_dags_from_directory(temp_dir.path())?;

        let dag = watcher.get_dag("my_dag")?;
        assert!(dag.is_some());
        assert_eq!(dag.unwrap().id, "my_dag");

        Ok(())
    }
}
