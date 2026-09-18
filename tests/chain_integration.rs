use std::fs;
use std::process::Command;

const SORTER: &str = r##"
import sys

buf = []
consumed = released = dropped = 0
flushes = 0
index = 0


def release():
    global released, flushes
    buf.sort()
    origins = [i for _, i, _ in buf]
    for _, _, line in buf:
        sys.stdout.write(line + "\n")
    sys.stdout.flush()
    released += len(buf)
    flushes += 1
    buf.clear()
    return origins


def ctl(origins=None):
    line = f"#mfctl consumed={consumed} released={released} dropped={dropped}"
    if origins:
        line += " origins=" + ",".join(map(str, origins))
    print(line, file=sys.stderr, flush=True)


for raw in sys.stdin:
    line = raw.rstrip("\n")
    index += 1
    consumed += 1
    origins = None
    if line.startswith("RELEASE"):
        dropped += 1
        origins = release()
    else:
        buf.append((int(line.split()[0]), index, line))
    ctl(origins)

origins = release() if buf else None
ctl(origins)
print('#mfstats {"flushes": %d}' % flushes, file=sys.stderr, flush=True)
"##;

const TOOL: &str = "#!/bin/sh
while IFS= read -r line; do
  echo \"OUT $line\"
  echo \"event count\"
done
";

fn setup(name: &str, data: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("oed-{}-{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("data"), data).unwrap();
    fs::write(dir.join("sorter.py"), SORTER).unwrap();
    fs::write(dir.join("tool.sh"), TOOL).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir.join("tool.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    }
    dir
}

fn base_args(dir: &std::path::Path) -> Vec<String> {
    vec![
        "--mode".into(), "accelerated".into(),
        "--format".into(), "csv".into(),
        "--data-source-type".into(), "file".into(),
        "--data-source".into(), dir.join("data").display().to_string(),
        "--binary-location".into(), dir.display().to_string(),
        "--binary-name".into(), "tool.sh".into(),
        "--response-mode".into(), "event-count".into(),
        "--output-collection-mode".into(), "before-delimiter".into(),
        "--maximum-latency".into(), "10000".into(),
    ]
}

#[test]
fn chain_sorter_end_to_end() {
    let dir = setup("chain", "3 a\n1 b\n2 c\nRELEASE\n5 d\n");
    let mut args = base_args(&dir);
    args.push("--processor".into());
    args.push(format!("python3 -u {}", dir.join("sorter.py").display()));

    let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
        .args(&args)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "driver failed:\n{}\n{}", stdout, stderr);

    assert!(stdout.contains("[Driver Protocol] 2"), "{}", stdout);
    // the three data rounds before the release close via ack, held grows
    assert!(stdout.contains("[Held] 1"), "{}", stdout);
    assert!(stdout.contains("[Held] 2"), "{}", stdout);
    assert!(stdout.contains("[Held] 3"), "{}", stdout);
    // release round: permutation reported, burst collected in one block
    assert!(stdout.contains("[Origins 0] 2,3,1"), "{}", stdout);
    assert!(stdout.contains("OUT 1 b\nOUT 2 c\nOUT 3 a"), "{}", stdout);
    // the swallowed RELEASE line must not count as held
    let release_round = stdout.split("[Input  ] RELEASE").nth(1).expect("release round missing");
    let round_block = release_round.split("\n\n").next().unwrap();
    assert!(round_block.contains("[Delivered] 3"), "{}", round_block);
    assert!(!round_block.contains("[Held]"), "swallowed signal counted as held:\n{}", round_block);
    // EOF flush drains the last buffered line and the stats arrive
    assert!(stdout.contains("OUT 5 d"), "{}", stdout);
    assert!(stdout.contains("[Total Delivered] 4"), "{}", stdout);
    assert!(stdout.contains("[Stage 0 Stats]"), "{}", stdout);
    assert!(stdout.contains("flushes"), "{}", stdout);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn legacy_path_unchanged_without_processors() {
    let dir = setup("legacy", "3 a\n1 b\n2 c\n");
    let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
        .args(&base_args(&dir))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "driver failed:\n{}\n{}", stdout, String::from_utf8_lossy(&out.stderr));

    assert!(stdout.contains("[Driver Protocol] 2"), "{}", stdout);
    assert!(stdout.contains("OUT 3 a"), "{}", stdout);
    assert!(stdout.contains("[Total Count] 3"), "{}", stdout);
    assert!(!stdout.contains("[Delivered]"), "legacy path must not print chain lines:\n{}", stdout);
    assert!(!stdout.contains("[Held]"), "{}", stdout);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn chain_rejects_latency_marker_and_warmup() {
    let dir = setup("reject", "1 a\n");
    for extra in [["--latency-marker", "MARK"], ["--warm-up-input", "warm"]] {
        let mut args = base_args(&dir);
        args.push("--processor".into());
        args.push(format!("python3 -u {}", dir.join("sorter.py").display()));
        args.extend(extra.iter().map(|s| s.to_string()));
        let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
            .args(&args)
            .output()
            .unwrap();
        assert!(!out.status.success(), "{:?} must be rejected with --processor", extra);
    }
    fs::remove_dir_all(&dir).ok();
}

const SILENT_TOOL: &str = "#!/bin/sh
while IFS= read -r line; do
  case \"$line\" in
    *hit*) echo \"VERDICT $line\" ;;
  esac
done
";

#[test]
fn chain_accounting_with_a_silent_tool() {
    let dir = setup("chainacct", "3 a hit\n1 b\n2 c hit\nRELEASE\n5 d\n");
    fs::write(dir.join("tool.sh"), SILENT_TOOL).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir.join("tool.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    }
    let mut args = base_args(&dir);
    args.push("--processor".into());
    args.push(format!("python3 -u {}", dir.join("sorter.py").display()));
    args.push("--response-accounting".into());
    args.push("chain".into());

    let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
        .args(&args)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "driver failed:\n{}\n{}", stdout, String::from_utf8_lossy(&out.stderr));

    // rounds close on chain counters although the tool never acknowledges
    assert!(stdout.contains("[Held] 1"), "{}", stdout);
    assert!(stdout.contains("[Total Delivered] 4"), "{}", stdout);
    // the tool's selective verdicts are harvested (possibly in later rounds)
    assert!(stdout.contains("VERDICT 2 c hit"), "{}", stdout);
    assert!(stdout.contains("VERDICT 3 a hit"), "{}", stdout);
    assert!(!stdout.contains("VERDICT 1 b"), "{}", stdout);

    fs::remove_dir_all(&dir).ok();
}
