use crate::store::Store;
use crate::scheduler::DagScheduler;
use crate::ui;
use axum::{
    extract::{Path, State},
    http::{StatusCode, HeaderMap, header},
    response::{Json, IntoResponse},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize)]
pub struct DagRunResponse {
    pub id: String,
    pub dag_id: String,
    pub status: String,
    pub started_at: String,
    pub triggered_by: String,
}

#[derive(Serialize)]
pub struct TaskRunResponse {
    pub id: String,
    pub dag_run_id: String,
    pub task_id: String,
    pub status: String,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub attempt_number: u32,
    pub log: String,
}

#[derive(Serialize)]
pub struct DashboardDagResponse {
    pub id: String,
    pub description: Option<String>,
    pub schedule: Option<String>,
    pub latest_run: Option<DagRunResponse>,
    pub runs: Vec<DagRunResponse>,
    pub tasks: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct TriggerRequest {
    pub dag_id: String,
}

#[derive(Serialize)]
pub struct StatusResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

pub fn create_router(store: Arc<Store>, scheduler: Arc<DagScheduler>) -> Router {
    Router::new()
        // API endpoints
        .route("/api/dags", get(list_dags))
        .route("/api/dags/:dag_id", get(get_dag))
        .route("/api/dags/:dag_id/runs", get(get_dag_runs))
        .route("/api/dags/:dag_id/trigger", post(trigger_dag))
        .route("/api/runs/:run_id", get(get_run))
        // Frontend routes
        .route("/", get(serve_frontend))
        .route("/index.html", get(serve_frontend))
        .fallback(serve_static_assets)
        .with_state(AppState { store, scheduler })
}

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub scheduler: Arc<DagScheduler>,
}

// Serve the frontend dashboard
async fn serve_frontend() -> impl IntoResponse {
    match ui::get_asset("index.html") {
        Some((content, mime_type)) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                format!("{}; charset=utf-8", mime_type)
                    .parse()
                    .unwrap_or_else(|_| "text/html; charset=utf-8".parse().unwrap()),
            );
            (StatusCode::OK, headers, content)
        }
        None => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                "text/plain".parse().unwrap(),
            );
            (
                StatusCode::NOT_FOUND,
                headers,
                "Frontend not available".as_bytes().to_vec(),
            )
        }
    }
}

// Serve static assets (CSS, JS, etc.)
async fn serve_static_assets(Path(path): Path<String>) -> impl IntoResponse {
    match ui::get_asset(&path) {
        Some((content, mime_type)) => {
            let mut headers = HeaderMap::new();
            if let Ok(mime) = mime_type.parse() {
                headers.insert(header::CONTENT_TYPE, mime);
            }
            (StatusCode::OK, headers, content)
        }
        None => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
            (
                StatusCode::NOT_FOUND,
                headers,
                format!("Asset not found: {}", path).into_bytes(),
            )
        }
    }
}

async fn list_dags(
    State(state): State<AppState>,
) -> Result<Json<StatusResponse<Vec<crate::dag::DagDefinition>>>, (StatusCode, String)> {
    let dags = state
        .store
        .get_all_dags()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(StatusResponse {
        success: true,
        data: Some(dags),
        error: None,
    }))
}

async fn get_dag(
    Path(dag_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<StatusResponse<crate::dag::DagDefinition>>, (StatusCode, String)> {
    let dag = state
        .store
        .get_dag(&dag_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, format!("DAG {} not found", dag_id)))?;

    Ok(Json(StatusResponse {
        success: true,
        data: Some(dag),
        error: None,
    }))
}

async fn get_dag_runs(
    Path(dag_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<StatusResponse<Vec<DagRunResponse>>>, (StatusCode, String)> {
    let runs = state
        .store
        .get_dag_runs(&dag_id, 100)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let response: Vec<DagRunResponse> = runs
        .into_iter()
        .map(|r| DagRunResponse {
            id: r.id,
            dag_id: r.dag_id,
            status: r.status.to_string(),
            started_at: r.started_at.to_rfc3339(),
            triggered_by: r.triggered_by.to_string(),
        })
        .collect();

    Ok(Json(StatusResponse {
        success: true,
        data: Some(response),
        error: None,
    }))
}

async fn trigger_dag(
    Path(dag_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<StatusResponse<serde_json::Value>>, (StatusCode, String)> {
    let run_id = state
        .scheduler
        .trigger_dag(&dag_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(StatusResponse {
        success: true,
        data: Some(serde_json::json!({
            "message": format!("DAG {} triggered successfully", dag_id),
            "run_id": run_id
        })),
        error: None,
    }))
}

async fn get_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<StatusResponse<serde_json::Value>>, (StatusCode, String)> {
    let run = state
        .store
        .get_dag_run(&run_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, format!("Run {} not found", run_id)))?;

    let task_runs = state
        .store
        .get_task_runs_for_dag_run(&run_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let response = serde_json::json!({
        "run": {
            "id": run.id,
            "dag_id": run.dag_id,
            "status": run.status.to_string(),
            "started_at": run.started_at.to_rfc3339(),
            "ended_at": run.ended_at.map(|t| t.to_rfc3339()),
            "triggered_by": run.triggered_by.to_string(),
        },
        "tasks": task_runs.iter().map(|t| {
            serde_json::json!({
                "id": t.id,
                "task_id": t.task_id,
                "status": t.status.to_string(),
                "started_at": t.started_at.map(|t| t.to_rfc3339()),
                "ended_at": t.ended_at.map(|t| t.to_rfc3339()),
                "attempt_number": t.attempt_number,
                "log": t.log,
            })
        }).collect::<Vec<_>>()
    });

    Ok(Json(StatusResponse {
        success: true,
        data: Some(response),
        error: None,
    }))
}