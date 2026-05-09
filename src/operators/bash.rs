use super::Operator;
use async_trait::async_trait;
use serde_json::Value;
use anyhow::Result;
use std::process::Stdio;
use tokio::process::Command;

pub struct BashOperator;

#[async_trait]
impl Operator for BashOperator {
    async fn execute(&self, config: &Value) -> Result<String> {
        let command = config
            .get("command")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing 'command' field in bash operator config"))?;

        let output = Command::new("bash")
            .arg("-c")
            .arg(command)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "Bash command failed with exit code: {:?}\nStderr: {}",
                output.status.code(),
                stderr
            ));
        }

        Ok(stdout)
    }

    fn name(&self) -> &'static str {
        "bash"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bash_operator_success() {
        let config = serde_json::json!({
            "command": "echo 'hello world'"
        });

        let operator = BashOperator;
        let result = operator.execute(&config).await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("hello world"));
    }

    #[tokio::test]
    async fn test_bash_operator_failure() {
        let config = serde_json::json!({
            "command": "exit 1"
        });

        let operator = BashOperator;
        let result = operator.execute(&config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bash_operator_missing_command() {
        let config = serde_json::json!({});

        let operator = BashOperator;
        let result = operator.execute(&config).await;
        assert!(result.is_err());
    }
}
