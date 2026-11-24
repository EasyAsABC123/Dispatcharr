//! Real bounded filesystem/archive regression; no Python or Source runtime parity claim.
use super::*;
use zip::{write::SimpleFileOptions, ZipWriter};
struct Owned(PathBuf);
impl Drop for Owned {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn workspace() -> Owned {
    let root = std::env::temp_dir().join(format!(
        "dispatcharr-plugin-{}",
        uuid::Uuid::new_v4().simple()
    ));
    fs::create_dir(&root).unwrap();
    Owned(root)
}
fn archive(root: &Path, entries: &[(&str, &[u8])]) -> PathBuf {
    let file = root.join(format!("{}.zip", uuid::Uuid::new_v4().simple()));
    let mut zip = ZipWriter::new(File::create(&file).unwrap());
    for (name, body) in entries {
        zip.start_file(*name, SimpleFileOptions::default()).unwrap();
        zip.write_all(body).unwrap();
    }
    zip.finish().unwrap();
    file
}
#[test]
fn package_selection_prefers_pluginpy_and_copies_shallow_external_logo() {
    let owned = workspace();
    let root = owned.0.join("plugins");
    let zip = archive(
        &owned.0,
        &[
            ("bundle/__init__.py", b""),
            (
                "bundle/deeper/plugin.py",
                b"raise RuntimeError('must never execute')",
            ),
            ("LOGO.PNG", b"owned-logo"),
        ],
    );
    assert_eq!(
        install(&zip, &root, "input.zip", false).unwrap(),
        json!({"success":true,"plugin_key":"deeper"})
    );
    assert_eq!(
        fs::read(root.join("deeper/logo.png")).unwrap(),
        b"owned-logo"
    );
    assert!(root.join("deeper/plugin.py").is_file());
    let registry = discover(&root).unwrap();
    assert_eq!(registry.len(), 1);
    assert!(registry[0].legacy);
    assert_eq!(registry[0].key, "deeper");
}
#[test]
fn flat_hint_duplicate_and_atomic_replacement_keep_source_layout() {
    let owned = workspace();
    let root = owned.0.join("plugins");
    let first = archive(
        &owned.0,
        &[
            ("plugin.py", b"old"),
            (
                "plugin.json",
                br#"{"name":"Owned","version":"1","fields":[]}"#,
            ),
        ],
    );
    assert_eq!(
        install(&first, &root, "Example-Plugin.zip", false).unwrap()["plugin_key"],
        "example_plugin"
    );
    let second = archive(&owned.0, &[("plugin.py", b"new")]);
    assert_eq!(
        install(&second, &root, "Example-Plugin.zip", false).unwrap()["error"],
        "Plugin 'example_plugin' already exists"
    );
    assert_eq!(
        fs::read(root.join("example_plugin/plugin.py")).unwrap(),
        b"old"
    );
    assert_eq!(
        install(&second, &root, "Example-Plugin.zip", true).unwrap()["success"],
        true
    );
    assert_eq!(
        fs::read(root.join("example_plugin/plugin.py")).unwrap(),
        b"new"
    );
    assert!(!root.join("example_plugin.__backup__").exists());
    let lock = MutationLock::acquire(&root).unwrap();
    delete_locked(&root, "example_plugin", None, &lock).unwrap();
    delete_locked(&root, "example_plugin", None, &lock).unwrap();
    assert!(!root.join("example_plugin").exists());
}
#[test]
fn unsafe_and_missing_entrypoints_never_create_final_plugin() {
    let owned = workspace();
    let root = owned.0.join("plugins");
    let escaped = archive(&owned.0, &[("../outside.py", b"bad")]);
    assert_eq!(
        install(&escaped, &root, "bad.zip", false).unwrap()["error"],
        "Unsafe path in archive"
    );
    assert!(!owned.0.join("outside.py").exists());
    let no_code = archive(&owned.0, &[("readme.txt", b"text")]);
    assert_eq!(
        install(&no_code, &root, "text.zip", false).unwrap()["error"],
        "Invalid plugin: missing plugin.py or package __init__.py"
    );
    let empty = archive(&owned.0, &[]);
    assert_eq!(
        install(&empty, &root, "empty.zip", false).unwrap()["error"],
        "Archive is empty"
    );
    assert!(discover(&root).unwrap().is_empty());
}
#[test]
fn disabled_discovery_never_executes_python_and_invalid_manifest_is_legacy() {
    let owned = workspace();
    let root = owned.0.join("plugins");
    let path = root.join("Some Folder");
    fs::create_dir_all(&path).unwrap();
    let marker = owned.0.join("would-execute");
    fs::write(
        path.join("plugin.py"),
        format!(
            "open({:?}, 'w').write('executed')",
            marker.to_str().unwrap()
        ),
    )
    .unwrap();
    fs::write(path.join("plugin.json"), b"not JSON").unwrap();
    let registry = discover(&root).unwrap();
    assert_eq!(registry[0].key, "some_folder");
    assert!(registry[0].legacy);
    assert!(!marker.exists());
    fs::write(
        path.join("plugin.json"),
        br#"{"name":"Metadata","actions":[]}"#,
    )
    .unwrap();
    let registry = discover(&root).unwrap();
    assert!(!registry[0].legacy);
    assert_eq!(registry[0].metadata["name"], "Metadata");
    assert!(!marker.exists());
}
