use super::Operator;
use async_trait::async_trait;
use serde_json::Value;
use anyhow::Result;

pub struct HttpOperator;

#[async_trait]
impl Operator for HttpOperator {
    async fn execute(&self, config: &Value) -> Result<String> {
        let url = config
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing 'url' field in HTTP operator config"))?;

        let method = config
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("GET");

        let client = reqwest::Client::new();
        
        let response = match method.to_uppercase().as_str() {
            "GET" => client.get(url).send().await?,
            "POST" => {
                let mut req = client.post(url);
                if let Some(body) = config.get("body") {
                    req = req.json(body);
                }
                req.send().await?
            }
            "PUT" => {
                let mut req = client.put(url);
                if let Some(body) = config.get("body") {
                    req = req.json(body);
                }
                req.send().await?
            }
            "DELETE" => client.delete(url).send().await?,
            _ => return Err(anyhow::anyhow!("Unsupported HTTP method: {}", method)),
        };

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "HTTP request failed with status: {}",
                response.status()
            ));
        }

        let body = response.text().await?;
        Ok(body)
    }

    fn name(&self) -> &'static str {
        "http"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_http_operator_missing_url() {
        let config = serde_json::json!({});

        let operator = HttpOperator;
        let result = operator.execute(&config).await;
        assert!(result.is_err());
    }
}
