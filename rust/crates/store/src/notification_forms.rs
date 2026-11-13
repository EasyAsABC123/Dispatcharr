//! Bounded notification HTML form inputs, anchored to Source12 and pinned DRF.
//! Original Dispatcharr, Django and DRF contributors retain source credit.
use crate::StoreError;
use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, FromRequest, Multipart, Request},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Map, Value};

#[derive(Debug)]
pub(super) struct WriteInput {
    pub fields: Map<String, Value>,
    pub html_json_null: bool,
    pub invalid_html_json: bool,
}
impl WriteInput {
    pub fn json(fields: Map<String, Value>) -> Self {
        Self {
            fields,
            html_json_null: false,
            invalid_html_json: false,
        }
    }
    pub fn finish(&self, fields: &mut Map<String, Value>, errors: &mut Map<String, Value>) {
        // JSONField.get_value marks HTML strings before json.loads. Its decoded
        // null is not the raw-null validate_empty_values path used for JSON.
        if self.html_json_null {
            errors.remove("action_data");
            fields.insert("action_data".into(), Value::Null);
        }
        if self.invalid_html_json {
            errors.insert("action_data".into(), json!(["Value must be valid JSON."]));
        }
    }
}
fn invalid_multipart() -> StoreError {
    StoreError::InvalidInput(json!({"detail":"Multipart form parse error"}))
}
fn writable(name: &str) -> bool {
    matches!(
        name,
        "notification_key"
            | "notification_type"
            | "priority"
            | "source"
            | "title"
            | "message"
            | "action_data"
            | "is_active"
            | "admin_only"
            | "expires_at"
    )
}
fn html(mut fields: Map<String, Value>, partial: bool) -> WriteInput {
    // Field.get_value treats missing HTML checkboxes differently from omitted
    // JSON fields. PATCH skips missing fields; PUT/create use false.
    for name in ["is_active", "admin_only"] {
        if fields.get(name) == Some(&json!("")) {
            fields.remove(name);
        } else if !partial && !fields.contains_key(name) {
            fields.insert(name.into(), Value::Bool(false));
        }
    }
    for name in ["notification_type", "priority", "source"] {
        if fields.get(name) == Some(&json!("")) {
            fields.remove(name);
        }
    }
    if fields.get("expires_at") == Some(&json!("")) {
        fields.insert("expires_at".into(), Value::Null);
    }
    let mut result = WriteInput::json(fields);
    if let Some(Value::String(raw)) = result.fields.get("action_data") {
        match serde_json::from_str::<Value>(raw) {
            Ok(value) => {
                result.html_json_null = value.is_null();
                result.fields.insert("action_data".into(), value);
            }
            Err(_) => result.invalid_html_json = true,
        }
    }
    result
}
pub(super) async fn parse(
    headers: &HeaderMap,
    body: &Bytes,
    partial: bool,
) -> Result<Option<WriteInput>, StoreError> {
    let media = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if !matches!(
        media,
        "application/x-www-form-urlencoded" | "multipart/form-data"
    ) {
        return Ok(None);
    }
    if body.len() > 2 * 1024 * 1024 {
        return Err(StoreError::StatusBody(
            413,
            json!({"detail":"Form body too large"}),
        ));
    }
    let mut fields = Map::new();
    if media == "application/x-www-form-urlencoded" {
        for (index, (key, value)) in url::form_urlencoded::parse(body).enumerate() {
            if index >= 1000 || key.len() > 1024 || value.len() > 65536 {
                return Err(invalid_multipart());
            }
            fields.insert(key.into_owned(), Value::String(value.into_owned()));
        }
    } else {
        let mut request = Request::new(Body::from(body.clone()));
        *request.headers_mut() = headers.clone();
        let mut multipart = Multipart::from_request(request, &())
            .await
            .map_err(|_| invalid_multipart())?;
        let mut count = 0;
        while let Some(field) = multipart
            .next_field()
            .await
            .map_err(|_| invalid_multipart())?
        {
            count += 1;
            if count > 1000 {
                return Err(invalid_multipart());
            }
            let Some(name) = field.name().map(str::to_owned) else {
                continue;
            };
            if name.len() > 1024 {
                return Err(invalid_multipart());
            }
            if field.file_name().is_some() {
                // Source12's unknown upload is parsed but never stored by the
                // serializer. Other file-valued write fields remain explicit.
                if writable(&name) {
                    return Err(StoreError::Unsupported(
                        "File-valued notification write fields are not migrated",
                    ));
                }
                continue;
            }
            let text = field.text().await.map_err(|_| invalid_multipart())?;
            if text.len() > 65536 {
                return Err(invalid_multipart());
            }
            fields.insert(name, Value::String(text));
        }
    }
    Ok(Some(html(fields, partial)))
}
pub(super) async fn create_input(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<WriteInput, StoreError> {
    if let Some(input) = parse(headers, body, false).await? {
        return Ok(input);
    }
    // Keep the preexisting create JSON extractor/content-type/error contract.
    let mut request = Request::new(Body::from(body.clone()));
    *request.headers_mut() = headers.clone();
    DefaultBodyLimit::disable().apply(&mut request); // outer captured Bytes already enforced its configured limit
    let parsed = Json::<Value>::from_request(request, &()).await;
    crate::admin_writes::body_object(parsed).map(WriteInput::json)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn html_put_missing_booleans_and_patch_retention() {
        let put = html(Map::new(), false);
        assert_eq!(
            put.fields,
            json!({"is_active":false,"admin_only":false})
                .as_object()
                .unwrap()
                .clone()
        );
        assert!(html(Map::new(), true).fields.is_empty());
    }
    #[test]
    fn html_json_string_is_decoded_with_typed_values() {
        let value = html(
            json!({"action_data":"{\"flag\":true,\"count\":1,\"nested\":[false,0]}"})
                .as_object()
                .unwrap()
                .clone(),
            true,
        );
        assert_eq!(
            value.fields["action_data"],
            json!({"flag":true,"count":1,"nested":[false,0]})
        );
        assert!(!value.invalid_html_json);
    }
    #[test]
    fn invalid_json_and_decoded_null_use_distinct_validation_paths() {
        let bad = html(
            json!({"action_data":"{"}).as_object().unwrap().clone(),
            true,
        );
        let mut fields = Map::new();
        let mut errors = Map::new();
        bad.finish(&mut fields, &mut errors);
        assert_eq!(errors["action_data"], json!(["Value must be valid JSON."]));
        let null = html(
            json!({"action_data":"null"}).as_object().unwrap().clone(),
            true,
        );
        null.finish(&mut fields, &mut errors);
        assert!(!errors.contains_key("action_data"));
        assert_eq!(fields["action_data"], Value::Null);
    }
    #[tokio::test]
    async fn frozen_source_multipart_unknown_file_is_not_a_write_field() {
        use sha2::{Digest, Sha256};
        const BODY:&[u8]=b"--DispatcharrFormsSource12Boundary7f34\r\nContent-Disposition: form-data; name=\"notification_key\"\r\n\r\nowned_forms_b\r\n--DispatcharrFormsSource12Boundary7f34\r\nContent-Disposition: form-data; name=\"title\"\r\n\r\nForm B\r\n--DispatcharrFormsSource12Boundary7f34\r\nContent-Disposition: form-data; name=\"message\"\r\n\r\nMultipart notification\r\n--DispatcharrFormsSource12Boundary7f34\r\nContent-Disposition: form-data; name=\"is_active\"\r\n\r\ntrue\r\n--DispatcharrFormsSource12Boundary7f34\r\nContent-Disposition: form-data; name=\"unknown_upload\" ; filename=\"owned-small.txt\"\r\nContent-Type: text/plain\r\n\r\nowned unknown upload\n\r\n--DispatcharrFormsSource12Boundary7f34--\r\n";
        assert_eq!(
            format!("{:x}", Sha256::digest(BODY)),
            "838ed4c301bcd8702c3325418ec371fc58ac7ff2f02b3005eba92c752f62373a"
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            "multipart/form-data; boundary=DispatcharrFormsSource12Boundary7f34"
                .parse()
                .unwrap(),
        );
        let input = parse(&headers, &Bytes::from_static(BODY), false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(input.fields["title"], "Form B");
        assert_eq!(input.fields["notification_key"], "owned_forms_b");
        assert!(!input.fields.contains_key("unknown_upload"));
    }
    #[tokio::test]
    async fn urlencoded_duplicate_values_and_json_validation_match_frozen_surface() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let input = parse(
            &headers,
            &Bytes::from_static(b"title=first&title=second&action_data=%7Bbad"),
            true,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(input.fields["title"], "second");
        assert!(input.invalid_html_json);
        assert!(!input.fields.contains_key("is_active"));
    }
    #[tokio::test]
    async fn original_create_json_content_type_and_rejections_are_retained() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            "application/vnd.fixture+json".parse().unwrap(),
        );
        let input = create_input(&headers, &Bytes::from_static(b"{\"title\":\"JSON\"}"))
            .await
            .unwrap();
        assert_eq!(input.fields["title"], "JSON");
        headers.insert("content-type", "text/plain".parse().unwrap());
        let error = create_input(&headers, &Bytes::from_static(b"{\"title\":\"JSON\"}"))
            .await
            .unwrap_err();
        assert!(
            matches!(error,StoreError::InvalidInput(v)if v==json!({"detail":"JSON parse error"}))
        );
    }
}
