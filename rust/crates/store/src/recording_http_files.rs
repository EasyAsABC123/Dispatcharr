//! Source Recording.destroy file/HLS removal and empty-parent pruning.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Stable owned namespace required; no concurrent external rename guarantee.
use serde_json::Value;
use std::{
    fs, io,
    path::{Path, PathBuf},
};
fn resolved(path: &str, root: &Path) -> io::Result<Option<PathBuf>> {
    if !path.starts_with("/data")
        || Path::new(path)
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Ok(None);
    }
    // Source Path.resolve resolves symlinks. Missing outputs are normal no-op;
    // the real existing prefix is still resolved before confining deletion.
    let path = Path::new(path);
    let mut missing = Vec::new();
    let mut prefix = path;
    loop {
        match fs::canonicalize(prefix) {
            Ok(mut resolved) => {
                for part in missing.iter().rev() {
                    resolved.push(part);
                }
                if resolved == root {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "recording artifact cannot be library root",
                    ));
                }
                return Ok(resolved.starts_with(root).then_some(resolved));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let Some(name) = prefix.file_name() else {
                    return Ok(None);
                };
                missing.push(name.to_os_string());
                let Some(parent) = prefix.parent() else {
                    return Ok(None);
                };
                prefix = parent;
            }
            Err(e) => return Err(e),
        }
    }
}
fn prune(path: &Path, root: &Path) -> io::Result<()> {
    let mut parent = path.parent();
    while let Some(current) = parent {
        if current == root || !current.starts_with(root) {
            break;
        }
        match fs::read_dir(current) {
            Ok(mut entries) => {
                if entries.next().is_some() {
                    break;
                }
                match fs::remove_dir(current) {
                    Ok(()) => {}
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::DirectoryNotEmpty | io::ErrorKind::NotFound
                        ) =>
                    {
                        break
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        parent = current.parent();
    }
    Ok(())
}
pub(crate) fn remove_recording(properties: &Value) -> io::Result<()> {
    let root = match fs::canonicalize("/data/recordings") {
        Ok(root) => root,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let file = properties
        .get("file_path")
        .and_then(Value::as_str)
        .map(|p| resolved(p, &root))
        .transpose()?
        .flatten();
    let hls = properties
        .get("_hls_dir")
        .and_then(Value::as_str)
        .map(|p| resolved(p, &root))
        .transpose()?
        .flatten();
    let mut failure = None;
    for (path, directory) in [(&file, false), (&hls, true)] {
        let Some(path) = path else {
            continue;
        };
        let operation = if directory {
            fs::remove_dir_all(path)
        } else {
            fs::remove_file(path)
        };
        if let Err(e) = operation {
            if e.kind() != io::ErrorKind::NotFound && failure.is_none() {
                failure = Some(e);
            }
        }
        if let Err(e) = prune(path, &root) {
            if failure.is_none() {
                failure = Some(e);
            }
        }
    }
    match failure {
        None => Ok(()),
        Some(error) => Err(error),
    }
}
