use std::fs;
use std::process::Command;

/// Answers nothing per line (a lockstep driver would wait forever): echoes
/// every line and reports its progress after the end of the input.
const TOOL: &str = "#!/bin/sh
n=0
while IFS= read -r line; do
  n=$((n+1))
  echo \"OUT $line\"
done
echo \"#progress $n\"
";

fn setup(name: &str, data: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("oed-open-{}-{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("data"), data).unwrap();
    fs::write(dir.join("tool.sh"), TOOL).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir.join("tool.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    }
    dir
}

fn args(dir: &std::path::Path, extra: &[&str]) -> Vec<String> {
    let mut a: Vec<String> = vec![
        "--open-loop".into(),
        "--data-source-type".into(), "file".into(),
        "--data-source".into(), dir.join("data").display().to_string(),
        "--binary-location".into(), dir.display().to_string(),
        "--binary-name".into(), "tool.sh".into(),
        "--maximum-latency".into(), "10000".into(),
    ];
    a.extend(extra.iter().map(|s| s.to_string()));
    a
}

/// `[Out] <ns> <line>` entries in log order.
fn outs(stdout: &str) -> Vec<(u128, String)> {
    stdout
        .lines()
        .filter_map(|l| l.strip_prefix("[Out] "))
        .map(|l| {
            let (ns, line) = l.split_once(' ').unwrap();
            (ns.parse().unwrap(), line.to_string())
        })
        .collect()
}

#[test]
fn open_loop_writes_without_waiting_and_stamps_the_output() {
    let dir = setup("plain", "3 a\n1 b\n2 c\n");
    let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
        .args(args(&dir, &["--mode", "accelerated", "--format", "csv"]))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "driver failed:\n{}\n{}", stdout, String::from_utf8_lossy(&out.stderr));
    assert!(stdout.contains("[Open Loop] 1"), "{}", stdout);
    let got = outs(&stdout);
    let lines: Vec<&str> = got.iter().map(|(_, l)| l.as_str()).collect();
    assert_eq!(lines, vec!["OUT 3 a", "OUT 1 b", "OUT 2 c", "#progress 3"], "{}", stdout);
    assert!(got.windows(2).all(|w| w[0].0 <= w[1].0), "stamps not monotone:\n{}", stdout);
    assert!(stdout.contains("[Total Count] 3"), "{}", stdout);
    assert!(stdout.contains("[Sent] 3 "), "{}", stdout);
    assert!(stdout.contains("[Tool Exit] exit status: 0"), "{}", stdout);
    // the lockstep log lines never appear
    assert!(!stdout.contains("[Input  ]"), "{}", stdout);
    assert!(!stdout.contains("[Elapsed]"), "{}", stdout);
    // nothing is paced in accelerated mode, so there is no anchor
    assert!(!stdout.contains("[Anchor]"), "{}", stdout);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn open_loop_paces_each_line_to_its_due_time() {
    // Dues 0, 200 and 400 ms, stripped before sending: the tool echoes at once,
    // so each echo is stamped at about its due time.
    let dir = setup("paced", "0\ta\n200\tb\n400\tc\n");
    let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
        .args(args(&dir, &["--mode", "real-time", "--format", "prefixed", "--timestamp-units", "milliseconds"]))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "driver failed:\n{}\n{}", stdout, String::from_utf8_lossy(&out.stderr));
    let got = outs(&stdout);
    let at = |line: &str| got.iter().find(|(_, l)| l == line).unwrap_or_else(|| panic!("{line} missing:\n{stdout}")).0;
    let ms = |ns: u128| ns / 1_000_000;
    assert!(ms(at("OUT b")) >= 200 && ms(at("OUT b")) < 350, "{}", stdout);
    assert!(ms(at("OUT c")) >= 400 && ms(at("OUT c")) < 550, "{}", stdout);
    assert!(stdout.contains("[Max Send Lateness] "), "{}", stdout);
    assert!(stdout.contains("[Anchor] 0 "), "{}", stdout);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn open_loop_refuses_response_options() {
    let dir = setup("refuse", "1 a\n");
    let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
        .args(args(&dir, &["--mode", "accelerated", "--format", "csv", "--response-mode", "event-count"]))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--open-loop does not wait for responses"));
    fs::remove_dir_all(&dir).ok();
}

/// A buffering stage in the LiveRunner protocol: holds lines until a RELEASE
/// line (swallowed), then releases them sorted; releases the rest at the end.
const STAGE: &str = r##"
import sys

buf = []
consumed = released = dropped = 0
flushes = 0


def release():
    global released, flushes
    buf.sort()
    for line in buf:
        sys.stdout.write(line + "\n")
    sys.stdout.flush()
    released += len(buf)
    flushes += 1
    buf.clear()


for raw in sys.stdin:
    line = raw.rstrip("\n")
    consumed += 1
    if line.startswith("RELEASE"):
        dropped += 1
        release()
    else:
        buf.append(line)
    print(f"#mfctl consumed={consumed} released={released} dropped={dropped}", file=sys.stderr, flush=True)

if buf:
    release()
print(f"#mfctl consumed={consumed} released={released} dropped={dropped}", file=sys.stderr, flush=True)
print("stage diagnostics", file=sys.stderr, flush=True)
print('#mfstats {"flushes": %d}' % flushes, file=sys.stderr, flush=True)
"##;

fn with_stage(dir: &std::path::Path) -> String {
    fs::write(dir.join("stage.py"), STAGE).unwrap();
    format!("python3 -u {}", dir.join("stage.py").display())
}

#[test]
fn open_loop_runs_processors_live_between_driver_and_tool() {
    let dir = setup("stage", "3 a\n1 b\nRELEASE\n2 c\n");
    let stage = with_stage(&dir);
    let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
        .args(args(&dir, &["--mode", "accelerated", "--format", "csv", "--processor", &stage]))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "driver failed:\n{}\n{}", stdout, stderr);
    let lines: Vec<String> = outs(&stdout).into_iter().map(|(_, l)| l).collect();
    // the stage sorts what it held at RELEASE and releases the rest at the end
    assert_eq!(lines, vec!["OUT 1 b", "OUT 3 a", "OUT 2 c", "#progress 3"], "{}", stdout);
    assert!(stdout.contains("[Total Count] 4"), "{}", stdout);
    assert!(stdout.contains("[Stage 0 Stats] {\"flushes\": 2}"), "{}", stdout);
    assert!(stdout.contains("[Total Delivered] 3"), "{}", stdout);
    assert!(stdout.contains("[Tool Exit] exit status: 0"), "{}", stdout);
    // control lines are drained, the stage's other stderr is passed on
    assert!(!stdout.contains("#mfctl") && !stderr.contains("#mfctl"), "{}\n{}", stdout, stderr);
    assert!(stderr.contains("[stage 0] stage diagnostics"), "{}", stderr);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn open_loop_delivers_a_held_line_when_the_stage_releases_it() {
    // Lines due at 0 and 100 ms are held until the RELEASE due at 300 ms; the
    // line due at 400 ms is held until the end of the input.
    let dir = setup("stage-paced", "0\t3 a\n100\t1 b\n300\tRELEASE\n400\t2 c\n");
    let stage = with_stage(&dir);
    let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
        .args(args(&dir, &[
            "--mode", "real-time", "--format", "prefixed", "--timestamp-units", "milliseconds",
            "--processor", &stage,
        ]))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "driver failed:\n{}\n{}", stdout, String::from_utf8_lossy(&out.stderr));
    let got = outs(&stdout);
    let at = |line: &str| got.iter().find(|(_, l)| l == line).unwrap_or_else(|| panic!("{line} missing:\n{stdout}")).0;
    let ms = |ns: u128| ns / 1_000_000;
    for held in ["OUT 1 b", "OUT 3 a"] {
        assert!(ms(at(held)) >= 300 && ms(at(held)) < 450, "{held}:\n{}", stdout);
    }
    assert!(ms(at("OUT 2 c")) >= 400, "{}", stdout);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn open_loop_refuses_chain_accounting() {
    let dir = setup("refuse-chain", "1 a\n");
    let out = Command::new(env!("CARGO_BIN_EXE_OnlineExperimentDriver"))
        .args(args(&dir, &["--mode", "accelerated", "--format", "csv", "--response-accounting", "chain"]))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--open-loop does not wait for responses"));
    fs::remove_dir_all(&dir).ok();
}
