//! Owned synthetic extraction differential driver; original authors retain credit.
use base64::Engine;
use dispatcharr_ingest_core::{extract_xmltv_owned, UploadLimits};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{self, BufRead, Read},
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, atomic::AtomicU64, atomic::Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct OwnedDirectory(PathBuf);
impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn digest(path: &Path) -> io::Result<Value> {
    if !path.exists() {
        return Ok(Value::Null);
    }
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut count = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let size = file.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        hash.update(&buffer[..size]);
        count += size as u64;
    }
    Ok(json!({"bytes":count,"sha256":format!("{:x}",hash.finalize())}))
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    for line in io::stdin().lock().lines() {
        let input: Value = serde_json::from_str(&line?)?;
        let name = input["name"].as_str().ok_or("missing name")?;
        if Path::new(name).file_name().and_then(|part| part.to_str()) != Some(name)
            || name.contains('\0')
        {
            return Err("unowned fixture filename".into());
        }
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let owned = OwnedDirectory(std::env::temp_dir().join(format!(
            "dispatcharr-xmltv-extract-{}-{time}-{id}",
            std::process::id()
        )));
        fs::create_dir(&owned.0)?;
        let source = owned.0.join(name);
        let output = owned.0.join("output.xml");
        fs::write(
            &source,
            base64::engine::general_purpose::STANDARD
                .decode(input["source_base64"].as_str().ok_or("missing bytes")?)?,
        )?;
        if input["existing_output"] == true {
            fs::write(&output, b"previous-owned-output")?;
        }
        let mut limits = UploadLimits::default();
        if let Some(max) = input["max_decoded_bytes"].as_u64() {
            limits.decoded_bytes = max;
        }
        let cancel = AtomicBool::new(input["cancelled"] == true);
        let result = extract_xmltv_owned(
            &source,
            &output,
            input["delete_original"] == true,
            limits,
            &cancel,
        );
        println!(
            "{}",
            json!({"success":result.is_ok(),"source_exists":source.exists(),"output":digest(&output)?})
        );
    }
    Ok(())
}
