use async_trait::async_trait;
use std::time::Duration;

use serde_json::{Value as JsonValue, json};

use crate::chat_types::{RichHandlerResult, ToolImagePart};
use crate::external_tool_handler::ExternalToolHandler;
use crate::external_tools::ExternalToolExecutor;
use crate::remi_uri::{RemiUri, RemiUriLocation};
pub struct ResolveUriHandler;

#[async_trait]
impl ExternalToolHandler for ResolveUriHandler {
    async fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let uri = extract_uri(payload)?;
        fetch_metadata_json(uri).await
    }

    async fn handle_rich(
        &self,
        _interrupt_id: &str,
        payload: &JsonValue,
    ) -> Result<RichHandlerResult, String> {
        let uri = extract_uri(payload)?;
        fetch_rich(uri).await
    }
}

pub fn register_url_external_tools(executor: &mut ExternalToolExecutor) {
    executor.register("resolve_uri", ResolveUriHandler);
}

fn extract_uri(payload: &JsonValue) -> Result<&str, String> {
    payload
        .get("uri")
        .and_then(|value| value.as_str())
        .or_else(|| {
            payload
                .get("arguments")
                .and_then(|value| value.get("uri"))
                .and_then(|value| value.as_str())
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "resolve_uri requires a non-empty uri".to_string())
}

fn build_http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (compatible; RemiBot/1.0)")
        .redirect(reqwest::redirect::Policy::limited(5))
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(false)
        .build()
        .map_err(|e| e.to_string())
}

async fn fetch_rich(url: &str) -> Result<RichHandlerResult, String> {
    // Handle remi:// URIs by resolving to image bytes directly
    if url.starts_with("remi://") {
        return fetch_rich_remi_uri(url).await;
    }

    fetch_http_rich(url).await
}

async fn fetch_http_rich(url: &str) -> Result<RichHandlerResult, String> {
    let client = build_http_client()?;

    let response = client
        .get(url)
        .header("Accept", "text/html,application/xhtml+xml,*/*;q=0.8")
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();

    if content_type.starts_with("image/") {
        const MAX_IMAGE_BYTES: usize = 5 * 1024 * 1024;
        let bytes = response.bytes().await.map_err(|e| e.to_string())?;
        if bytes.len() > MAX_IMAGE_BYTES {
            return Ok(RichHandlerResult::Json(json!({
                "url": url,
                "type": "image",
                "content_type": content_type,
            })));
        }
        let media_type = content_type
            .split(';')
            .next()
            .unwrap_or(&content_type)
            .trim()
            .to_string();
        return Ok(RichHandlerResult::Image(ToolImagePart {
            media_type,
            data: bytes.to_vec(),
        }));
    }

    // For HTML and other types, fall back to metadata extraction
    fetch_metadata_json(url).await.map(RichHandlerResult::Json)
}

async fn fetch_rich_remi_uri(uri: &str) -> Result<RichHandlerResult, String> {
    let parsed = RemiUri::parse(uri).map_err(|e| e.to_string())?;
    match parsed.location {
        RemiUriLocation::Remote => {
            // Remote is an HTTPS URL — delegate back to HTTP fetch
            fetch_http_rich(&parsed.path).await
        }
        RemiUriLocation::File => {
            // Absolute local path
            let path = if parsed.path.len() > 2 && parsed.path.chars().nth(1) == Some(':') {
                parsed.path.clone()
            } else {
                format!("/{}", parsed.path)
            };
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|e| format!("Cannot read {path}: {e}"))?;
            let media_type = parsed.mime_type.clone();
            Ok(RichHandlerResult::Image(ToolImagePart {
                media_type,
                data: bytes,
            }))
        }
        RemiUriLocation::Local => Err(format!(
            "remi://local URIs require app data dir context and cannot be resolved by the desktop SDK handler: {uri}"
        )),
        RemiUriLocation::Inline => {
            // Inline is already base64 data — wrap as text answer
            Ok(RichHandlerResult::Json(
                json!({ "url": uri, "type": "inline" }),
            ))
        }
    }
}

async fn fetch_metadata_json(url: &str) -> Result<JsonValue, String> {
    fetch_metadata(url).await
}

async fn fetch_metadata(url: &str) -> Result<JsonValue, String> {
    let client = build_http_client()?;

    let response = client
        .get(url)
        .header("Accept", "text/html,application/xhtml+xml,*/*;q=0.8")
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();

    if content_type.starts_with("image/") {
        return Ok(json!({
            "url": url,
            "type": "image",
            "content_type": content_type,
        }));
    }

    if !content_type.contains("html") {
        return Ok(json!({
            "url": url,
            "content_type": content_type,
        }));
    }

    let body = response.text().await.map_err(|e| e.to_string())?;
    let meta = extract_meta(&body);

    let title = meta
        .get("og:title")
        .or_else(|| meta.get("twitter:title"))
        .or_else(|| meta.get("html_title"))
        .cloned()
        .unwrap_or_default();
    let description = meta
        .get("og:description")
        .or_else(|| meta.get("twitter:description"))
        .or_else(|| meta.get("description"))
        .cloned()
        .unwrap_or_default();

    let mut result = json!({ "url": url });
    let object = result.as_object_mut().expect("json object");
    if !title.is_empty() {
        object.insert("title".into(), json!(title));
    }
    if !description.is_empty() {
        object.insert("description".into(), json!(description));
    }
    if let Some(value) = meta.get("og:site_name") {
        object.insert("site_name".into(), json!(value));
    }
    if let Some(value) = meta.get("og:image").or_else(|| meta.get("twitter:image")) {
        object.insert("image".into(), json!(value));
    }
    if let Some(value) = meta.get("og:type") {
        object.insert("type".into(), json!(value));
    }

    Ok(result)
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    let needle_bytes = needle.as_bytes();
    if needle_bytes.is_empty() {
        return Some(0);
    }

    haystack
        .as_bytes()
        .windows(needle_bytes.len())
        .position(|window| window.eq_ignore_ascii_case(needle_bytes))
}

fn extract_meta(html: &str) -> std::collections::HashMap<String, String> {
    use std::collections::HashMap;

    let mut result = HashMap::new();

    if let Some(start) = find_ascii_case_insensitive(html, "<title") {
        if let Some(end_tag) = html[start..].find('>') {
            let after_open = start + end_tag + 1;
            if let Some(close) = find_ascii_case_insensitive(&html[after_open..], "</title>") {
                let title_text = &html[after_open..after_open + close];
                result.insert("html_title".into(), html_decode(title_text.trim()));
            }
        }
    }

    let mut pos = 0;
    while let Some(offset) = find_ascii_case_insensitive(&html[pos..], "<meta") {
        let start = pos + offset;
        let end = match html[start..].find('>') {
            Some(end) => start + end + 1,
            None => break,
        };
        let tag = &html[start..end];
        let (prop, content) = parse_meta_tag(tag);
        let relevant = matches!(
            prop.as_str(),
            "og:title"
                | "og:description"
                | "og:image"
                | "og:site_name"
                | "og:url"
                | "og:type"
                | "twitter:title"
                | "twitter:description"
                | "twitter:image"
                | "description"
        );
        if relevant && !content.is_empty() {
            result.entry(prop).or_insert(content);
        }
        pos = end;
    }

    result
}

fn parse_meta_tag(tag: &str) -> (String, String) {
    let mut prop = String::new();
    let mut content = String::new();
    let mut remaining = tag;

    while let Some(eq_pos) = remaining.find('=') {
        let key = remaining[..eq_pos]
            .trim()
            .split_whitespace()
            .last()
            .unwrap_or("")
            .to_lowercase();
        remaining = &remaining[eq_pos + 1..];
        let (value, rest) = parse_attr_value(remaining);
        remaining = rest;

        let key_lc = key.trim_start_matches('/');
        if matches!(key_lc, "property" | "name") {
            prop = value.to_lowercase();
        } else if key_lc == "content" {
            content = html_decode(&value);
        }
    }

    (prop, content)
}

fn parse_attr_value(source: &str) -> (String, &str) {
    let source = source.trim_start();
    if let Some(inner) = source.strip_prefix('"') {
        if let Some(end) = inner.find('"') {
            return (inner[..end].into(), &inner[end + 1..]);
        }
    } else if let Some(inner) = source.strip_prefix('\'') {
        if let Some(end) = inner.find('\'') {
            return (inner[..end].into(), &inner[end + 1..]);
        }
    } else {
        let end = source
            .find(|c: char| c.is_whitespace() || c == '>')
            .unwrap_or(source.len());
        return (source[..end].into(), &source[end..]);
    }

    (String::new(), source)
}

fn html_decode(source: &str) -> String {
    source
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_uri_supports_direct_shape() {
        let payload = json!({ "uri": "https://example.com" });
        let url = extract_uri(&payload).expect("direct uri");
        assert_eq!(url, "https://example.com");
    }

    #[test]
    fn extract_uri_supports_nested_arguments_shape() {
        let payload = json!({
            "type": "external_tool_call",
            "tool_name": "resolve_uri",
            "arguments": { "uri": "https://example.com/page" }
        });
        let url = extract_uri(&payload).expect("nested uri");
        assert_eq!(url, "https://example.com/page");
    }

    #[tokio::test]
    async fn resolve_uri_handler_requires_uri() {
        let handler = ResolveUriHandler;
        let error = handler
            .handle("call-1", &json!({ "type": "resolve_uri" }))
            .await
            .expect_err("missing uri must fail");
        assert!(error.contains("non-empty uri"));
    }

    #[test]
    fn extract_meta_handles_multibyte_text_before_title_tag() {
        let html = "İ你好<title>示例标题</title>";
        let meta = extract_meta(html);

        assert_eq!(meta.get("html_title").map(String::as_str), Some("示例标题"));
    }

    #[test]
    fn extract_meta_handles_multibyte_text_before_meta_tag() {
        let html = "İ你好<meta property=\"og:title\" content=\"世界\">";
        let meta = extract_meta(html);

        assert_eq!(meta.get("og:title").map(String::as_str), Some("世界"));
    }
}
