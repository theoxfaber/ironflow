use clap::{Parser, Subcommand};
use anyhow::Result;
use std::sync::Arc;
use crate::store::Store;
use crate::scheduler::DagScheduler;
use crate::dag::DagWatcher;
use crate::api::routes::create_router;

#[derive(Parser)]
#[command(name = "ironflow")]
#[command(about = "A lightning-fast data pipeline orchestrator", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the IronFlow scheduler and executor
    Start {
        /// Path to the dags directory
        #[arg(long, default_value = "./dags")]
        dags_dir: String,

        /// Path to the database file
        #[arg(long, default_value = "./ironflow.db")]
        db_path: String,

        /// Start with API server
        #[arg(long, default_value = "false")]
        with_api: bool,

        /// Port for API server
        #[arg(long, default_value = "8080")]
        port: u16,
    },

    /// Manually trigger a DAG run
    Trigger {
        /// DAG ID to trigger
        dag_id: String,

        /// Path to the database file
        #[arg(long, default_value = "./ironflow.db")]
        db_path: String,
    },

    /// Get the status of a DAG
    Status {
        /// DAG ID to query
        dag_id: String,

        /// Number of runs to display
        #[arg(long, default_value = "10")]
        limit: i64,

        /// Path to the database file
        #[arg(long, default_value = "./ironflow.db")]
        db_path: String,
    },

    /// Pause a DAG (prevents scheduled execution)
    Pause {
        /// DAG ID to pause
        dag_id: String,

        /// Path to the database file
        #[arg(long, default_value = "./ironflow.db")]
        db_path: String,
    },

    /// Unpause a DAG
    Unpause {
        /// DAG ID to unpause
        dag_id: String,

        /// Path to the database file
        #[arg(long, default_value = "./ironflow.db")]
        db_path: String,
    },

    /// List all DAGs
    List {
        /// Path to the database file
        #[arg(long, default_value = "./ironflow.db")]
        db_path: String,
    },

    /// Start the API server
    Serve {
        /// Port to listen on
        #[arg(long, default_value = "8080")]
        port: u16,

        /// Path to the database file
        #[arg(long, default_value = "./ironflow.db")]
        db_path: String,
    },
}

pub async fn execute_cli() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start { dags_dir, db_path, with_api, port } => {
            execute_start(&dags_dir, &db_path, with_api, port).await?
        }
        Commands::Trigger { dag_id, db_path } => execute_trigger(&dag_id, &db_path).await?,
        Commands::Status { dag_id, limit, db_path } => execute_status(&dag_id, limit, &db_path).await?,
        Commands::Pause { dag_id, db_path } => execute_pause(&dag_id, &db_path).await?,
        Commands::Unpause { dag_id, db_path } => execute_unpause(&dag_id, &db_path).await?,
        Commands::List { db_path } => execute_list(&db_path).await?,
        Commands::Serve { port, db_path } => execute_serve(port, &db_path).await?,
    }

    Ok(())
}

async fn execute_start(dags_dir: &str, db_path: &str, with_api: bool, port: u16) -> Result<()> {
    println!("Starting IronFlow...");
    println!("DAGs directory: {}", dags_dir);
    println!("Database: {}", db_path);

    // Initialize database
    let db_url = if db_path.starts_with("./") || !db_path.contains("://") {
        format!("sqlite://{}", db_path)
    } else {
        db_path.to_string()
    };
    let store = Arc::new(Store::new(&db_url).await?);

    // Recover from any previous crashes before starting the scheduler
    println!("Recovering from any previous crashes...");
    store.recover_orphaned_runs().await?;

    // Load DAGs from directory
    let watcher = DagWatcher::new();
    watcher.load_dags_from_directory(dags_dir)?;

    // Create scheduler
    let scheduler = Arc::new(DagScheduler::new(Arc::clone(&store)).await?);

    // Schedule all loaded DAGs
    for dag in watcher.get_all_dags()? {
        store.save_dag(&dag).await?;
        if let Err(e) = scheduler.schedule_dag(&dag).await {
            eprintln!("Warning: Could not schedule DAG {}: {}", dag.id, e);
        }
    }

    // Start scheduler
    scheduler.start().await?;

    // Start watching directory for changes
    if with_api {
        let watcher_clone = watcher.clone();
        let store_clone = Arc::clone(&store);
        let scheduler_clone = Arc::clone(&scheduler);
        let dags_dir_clone = dags_dir.to_string();
        
        tokio::spawn(async move {
            if let Err(e) = watcher_clone.watch_directory(dags_dir_clone, store_clone, scheduler_clone).await {
                eprintln!("Error watching directory: {}", e);
            }
        });
    }

    if with_api {
        println!("Starting API server on port {}", port);
        let api_store = Arc::clone(&store);
        let api_scheduler = Arc::clone(&scheduler);
        
        let app = create_router(api_store, api_scheduler);
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        println!("API server running at http://0.0.0.0:{}", port);
        
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service()).await.unwrap();
        });
    }

    // Keep running
    println!("IronFlow running. Press Ctrl+C to stop.");
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}

async fn execute_trigger(dag_id: &str, db_path: &str) -> Result<()> {
    let db_url = format!("sqlite://{}", db_path);
    let store = Arc::new(Store::new(&db_url).await?);
    let scheduler = DagScheduler::new(store).await?;

    println!("Triggering DAG: {}", dag_id);
    scheduler.trigger_dag(dag_id).await?;
    println!("DAG triggered successfully");

    Ok(())
}

async fn execute_status(dag_id: &str, limit: i64, db_path: &str) -> Result<()> {
    let db_url = format!("sqlite://{}", db_path);
    let store = Arc::new(Store::new(&db_url).await?);

    println!("Status for DAG: {}", dag_id);
    println!();

    let runs = store.get_dag_runs(dag_id, limit).await?;

    if runs.is_empty() {
        println!("No runs found for this DAG");
        return Ok(());
    }

    println!("{:<40} {:<15} {:<20}", "Run ID", "Status", "Started At");
    println!("{}", "=".repeat(75));

    for run in runs {
        println!(
            "{:<40} {:<15} {:<20}",
            run.id,
            run.status.to_string(),
            run.started_at.format("%Y-%m-%d %H:%M:%S")
        );
    }

    Ok(())
}

async fn execute_pause(dag_id: &str, db_path: &str) -> Result<()> {
    let db_url = format!("sqlite://{}", db_path);
    let store = Arc::new(Store::new(&db_url).await?);

    store.pause_dag(dag_id).await?;
    println!("DAG paused: {}", dag_id);

    Ok(())
}

async fn execute_unpause(dag_id: &str, db_path: &str) -> Result<()> {
    let db_url = format!("sqlite://{}", db_path);
    let store = Arc::new(Store::new(&db_url).await?);

    store.unpause_dag(dag_id).await?;
    println!("DAG unpaused: {}", dag_id);

    Ok(())
}

async fn execute_list(db_path: &str) -> Result<()> {
    let db_url = format!("sqlite://{}", db_path);
    let store = Arc::new(Store::new(&db_url).await?);

    let dags = store.get_all_dags().await?;

    if dags.is_empty() {
        println!("No DAGs found");
        return Ok(());
    }

    println!("{:<30} {:<50}", "DAG ID", "Description");
    println!("{}", "=".repeat(80));

    for dag in dags {
        let description = dag.description.as_deref().unwrap_or("N/A");
        println!("{:<30} {:<50}", dag.id, description);
    }

    Ok(())
}

async fn execute_serve(port: u16, db_path: &str) -> Result<()> {
    println!("Starting IronFlow API server on port {}", port);

    let db_url = format!("sqlite://{}", db_path);
    let store = Arc::new(Store::new(&db_url).await?);
    let scheduler = Arc::new(DagScheduler::new(Arc::clone(&store)).await?);

    let app = create_router(store, scheduler);
    
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!("API server running at http://0.0.0.0:{}", port);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}
