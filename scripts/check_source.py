#!/usr/bin/env python3
"""Structural source checks, NOT Rust compilation, type checking or Wasm validation."""

from __future__ import annotations

import ast
import hashlib
import json
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKS = []


def source_files(pattern):
    return (
        path
        for path in ROOT.rglob(pattern)
        if not {".git", "target", ".venv", "__pycache__"}.intersection(path.relative_to(ROOT).parts)
    )


def check(condition, name):
    if not condition:
        raise AssertionError(name)
    CHECKS.append(name)


def main():
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text())
    members = workspace["workspace"]["members"]
    packages = {}
    graph = {}
    check(
        workspace["workspace"]["package"]["repository"] == "https://github.com/doout/wayframe",
        "repository metadata is doout/wayframe",
    )
    check(
        set(workspace["workspace"]["default-members"])
        == {"crates/wayframe", "crates/wayframe-core", "crates/wayframe-runtime"},
        "default workspace is library-first",
    )
    check(not (ROOT / "src/main.rs").exists(), "no framework gateway executable")
    for member in members:
        path = ROOT / member / "Cargo.toml"
        check(path.is_file(), f"member manifest exists: {member}")
        config = tomllib.loads(path.read_text())
        name = config["package"]["name"]
        packages[name] = config
        check(config["package"].get("publish") is False, f"not prematurely published: {name}")
        check(
            (path.parent / "src/lib.rs").is_file() or (path.parent / "src/main.rs").is_file(),
            f"Rust entry point: {name}",
        )
        graph[name] = []
        for table in ("dependencies", "dev-dependencies", "build-dependencies"):
            for dependency, options in config.get(table, {}).items():
                if not isinstance(options, dict):
                    continue
                if "path" in options:
                    target = (path.parent / options["path"] / "Cargo.toml").resolve()
                    check(target.is_file(), f"path dependency exists: {name} -> {dependency}")
                    resolved = tomllib.loads(target.read_text())["package"]["name"]
                    check(
                        resolved == dependency, f"path package name matches: {name} -> {dependency}"
                    )
                    if table == "dependencies":
                        graph[name].append(dependency)
                if options.get("workspace"):
                    check(
                        dependency in workspace["workspace"]["dependencies"],
                        f"workspace dependency declared: {name} -> {dependency}",
                    )
    check(
        packages["wayframe-core"].get("dependencies", {}) == {},
        "protocol-independent core has no external dependencies",
    )
    facade = packages["wayframe"]
    check(facade["features"]["default"] == [], "no default optional features")
    for feature in ("http", "controls", "transport", "mcp", "wasm"):
        check(feature in facade["features"], f"opt-in facade feature: {feature}")
    visiting = set()
    seen = set()

    def walk(name):
        if name in visiting:
            raise AssertionError(f"cyclic package dependency: {name}")
        if name in seen:
            return
        visiting.add(name)
        for child in graph.get(name, []):
            walk(child)
        visiting.remove(name)
        seen.add(name)

    for name in graph:
        walk(name)
    check(True, "local package dependency graph is acyclic")
    for path in source_files("*.toml"):
        tomllib.loads(path.read_text())
    check(True, "all TOML files parse")
    manifest = tomllib.loads((ROOT / "plugins/path-control/plugin.toml").read_text())
    artifact = ROOT / "plugins/path-control" / manifest["component"]
    check(artifact.is_file(), "sample plugin artifact exists")
    check(
        hashlib.sha256(artifact.read_bytes()).hexdigest() == manifest["sha256"].lower(),
        "sample plugin digest matches exact bytes",
    )
    check(manifest["abi"] == "wayframe:gateway/policy@0.1.0", "sample ABI is namespaced/versioned")
    check(
        set(manifest["capabilities"]) == {"request.path", "operation.target"},
        "sample grants only path and operation target",
    )
    wit = (ROOT / "wit/policy.wit").read_text()
    check("package wayframe:gateway@0.1.0;" in wit, "WIT package identity matches")
    check(
        len(re.findall(r"\b(?:method|path|subject|tenant|operation|target):\s*string", wit)) == 6,
        "WIT declares six projected string fields",
    )
    wat = artifact.read_text()
    check(
        len(re.findall(r'\(param "\w+" string\)', wat)) == 6,
        "WAT component signature has six strings",
    )
    for file in source_files("*.rs"):
        text = file.read_text()
        check(
            not re.search(r"\b(?:todo!|unimplemented!)\s*\(", text),
            f"no placeholder panic: {file.relative_to(ROOT)}",
        )
        for rel in re.findall(r'include_(?:str|bytes)!\("([^"]+)"\)', text):
            check((file.parent / rel).is_file(), f"compile-time fixture exists: {rel}")
    for file in source_files("*.py"):
        ast.parse(file.read_text(), filename=str(file))
    check(True, "all Python sources parse")
    for file in source_files("*.md"):
        for target in re.findall(r"\[[^\]]*\]\(([^)]+)\)", file.read_text()):
            if "://" in target or target.startswith(("#", "mailto:")):
                continue
            target = target.split("#", 1)[0]
            if target:
                check(
                    (file.parent / target).exists(),
                    f"local documentation link: {file.relative_to(ROOT)} -> {target}",
                )
    check(
        (ROOT / ".github/workflows/ci.yml").is_file(),
        "CI workflow included (not executed by this check)",
    )
    rust_tests = sum(
        len(re.findall(r"#\[(?:tokio::)?test(?:\([^\]]*\))?\]", f.read_text()))
        for f in source_files("*.rs")
    )
    report = {
        "kind": "structural-source-checks",
        "passed": len(CHECKS),
        "checks": CHECKS,
        "workspace_packages": len(members),
        "rust_test_functions_present": rust_tests,
        "rust_compilation_executed": False,
        "wasm_execution_executed": False,
    }
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
