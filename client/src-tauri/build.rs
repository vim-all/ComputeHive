use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    repair_tauri_permission_paths();
    tauri_build::build()
}

fn repair_tauri_permission_paths() {
    let manifest_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("missing CARGO_MANIFEST_DIR in build script"),
    );

    for (key, value) in env::vars_os() {
        let key = key.to_string_lossy();

        if !key.starts_with("DEP_TAURI") || !key.ends_with("PERMISSION_FILES_PATH") {
            continue;
        }

        let manifest_path = PathBuf::from(value);
        if !manifest_path.exists() {
            continue;
        }

        rewrite_permission_manifest(&manifest_path, &manifest_dir);
    }
}

fn rewrite_permission_manifest(manifest_path: &Path, manifest_dir: &Path) {
    let Ok(contents) = fs::read_to_string(manifest_path) else {
        return;
    };

    let Ok(permission_paths) = serde_json::from_str::<Vec<PathBuf>>(&contents) else {
        return;
    };

    let mut changed = false;
    let rewritten = permission_paths
        .into_iter()
        .map(|path| {
            if path.exists() {
                return path;
            }

            let Some(suffix) = suffix_after_target_dir(&path) else {
                return path;
            };

            let corrected_path = manifest_dir.join("target").join(suffix);
            if corrected_path.exists() {
                changed = true;
                corrected_path
            } else {
                path
            }
        })
        .collect::<Vec<_>>();

    if changed {
        let json = serde_json::to_string(&rewritten)
            .expect("failed to serialize rewritten permission manifest");
        fs::write(manifest_path, json).expect("failed to persist rewritten permission manifest");
    }
}

fn suffix_after_target_dir(path: &Path) -> Option<PathBuf> {
    let marker = Path::new("src-tauri").join("target");
    let marker_components = marker.components().collect::<Vec<_>>();
    let components = path.components().collect::<Vec<_>>();

    let marker_start = components
        .windows(marker_components.len())
        .position(|window| window == marker_components.as_slice())?;

    let suffix = components
        .iter()
        .skip(marker_start + marker_components.len())
        .map(|component| component.as_os_str())
        .collect::<PathBuf>();

    Some(suffix)
}
