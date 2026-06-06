from pathlib import Path
import json
import subprocess


def _run_monitor(repo: Path, tmp_path: Path, *extra_args: str):
    return subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(repo / "scripts" / "diagnose-socket-pressure.ps1"),
            "-Once",
            "-OutputDir",
            str(tmp_path),
            *extra_args,
        ],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )


def _read_last_record(tmp_path: Path):
    files = list(tmp_path.glob("socket-pressure-*.jsonl"))
    assert len(files) == 1
    return json.loads(files[0].read_text(encoding="utf-8").strip().splitlines()[-1])


def test_socket_pressure_monitor_writes_jsonl_snapshot(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    result = _run_monitor(repo, tmp_path, "-SkipDockerInspect")

    assert result.returncode == 0, result.stdout + result.stderr
    record = _read_last_record(tmp_path)
    assert record["schemaVersion"] == 1
    assert "tcpDynamicRange" in record
    assert "tcpStateCounts" in record
    assert "processTcpTop" in record
    assert "dockerBackend" in record
    assert "easyContainers" in record
    assert "proxyPort42344" in record


def test_socket_pressure_monitor_counts_running_easy_containers_when_docker_is_available(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    docker_ps = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    if docker_ps.returncode != 0:
        return

    expected_easy_count = sum(1 for line in docker_ps.stdout.splitlines() if line.startswith("easy-"))
    if expected_easy_count == 0:
        return

    result = _run_monitor(repo, tmp_path, "-SkipDetailedTcpQuery")

    assert result.returncode == 0, result.stdout + result.stderr
    record = _read_last_record(tmp_path)
    assert record["easyContainers"]["easyRunningCount"] == expected_easy_count
    flattened_count = sum(
        container["publishedBindingCount"]
        for container in record["easyContainers"]["easyContainers"]
    )
    assert len(record["easyContainers"]["publishedHostPorts"]) == flattened_count


def test_machine_proxy_cleanup_script_is_parseable():
    repo = Path(__file__).resolve().parents[1]
    script = repo / "scripts" / "clear-machine-flclash-proxy-env-admin.ps1"
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            (
                "$errors=$null;"
                "[System.Management.Automation.Language.Parser]::ParseFile("
                f"'{script}', [ref]$null, [ref]$errors) | Out-Null;"
                "if($errors){ $errors | ForEach-Object { $_.Message }; exit 1 }"
            ),
        ],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
