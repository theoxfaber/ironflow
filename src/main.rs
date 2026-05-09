use ironflow::cli::execute_cli;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("ironflow=info".parse().unwrap()),
        )
        .init();

    // Execute CLI
    execute_cli().await?;

    Ok(())
}
