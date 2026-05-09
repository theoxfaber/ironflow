//! UI frontend embedding module
//! This module embeds the frontend assets using rust-embed

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "frontend/dist"]
pub struct Assets;

pub fn get_asset(path: &str) -> Option<(Vec<u8>, String)> {
    let clean_path = if path.is_empty() || path == "/" {
        "index.html"
    } else {
        path.trim_start_matches('/')
    };

    Assets::get(clean_path).map(|data| {
        let mime_type = mime_guess::from_path(clean_path)
            .first_raw()
            .unwrap_or("application/octet-stream")
            .to_string();
        (data.data.to_vec(), mime_type)
    })
}

pub fn is_frontend_available() -> bool {
    Assets::get("index.html").is_some()
}
