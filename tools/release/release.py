#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = [
#   "questionary>=2.1,<3",
#   "rich>=14,<15",
# ]
# ///

"""Release Muxe from the commit at the local main bookmark."""

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
import tomllib
from pathlib import Path
from typing import Any, Final, NoReturn

import questionary
from questionary import Choice
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


CONSOLE: Final = Console()
ROOT: Final = Path(__file__).resolve().parents[2]
ALLOWED_RELEASE_FILES: Final = {"Cargo.lock", "Cargo.toml", "CHANGELOG.md"}
WORKFLOW_DISCOVERY_TIMEOUT_SECONDS: Final = 120.0
WORKFLOW_DISCOVERY_INTERVAL_SECONDS: Final = 2.0
SEMVER_PATTERN: Final = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")

PROMPT_STYLE: Final = questionary.Style(
    [
        ("qmark", "fg:#d787ff bold"),
        ("question", "fg:#87d7ff bold"),
        ("answer", "fg:#5fffd7 bold"),
        ("pointer", "fg:#ffaf5f bold"),
        ("highlighted", "fg:#0b0f14 bg:#5fffd7 bold"),
        ("selected", "fg:#5fffd7"),
        ("instruction", "fg:#808080 italic"),
        ("text", "fg:#f0f0f0"),
    ]
)


class ReleaseError(RuntimeError):
    """A release precondition or command failed."""


def fail(message: str) -> NoReturn:
    raise ReleaseError(message)


def command(
    arguments: list[str], *, capture: bool = False, announce: bool = True
) -> str:
    if announce:
        CONSOLE.print(Text(f"$ {shlex.join(arguments)}", style="dim cyan"))
    try:
        result = subprocess.run(
            arguments,
            cwd=ROOT,
            check=True,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
    except FileNotFoundError:
        fail(f"Required command is unavailable: {arguments[0]}")
    except subprocess.CalledProcessError as error:
        if capture:
            if error.stdout:
                CONSOLE.print(Text(error.stdout.rstrip()))
            if error.stderr:
                CONSOLE.print(Text(error.stderr.rstrip(), style="red"))
        fail(
            f"Command failed with exit code {error.returncode}: {shlex.join(arguments)}"
        )
    return result.stdout if capture else ""


def workspace_version() -> str:
    with (ROOT / "Cargo.toml").open("rb") as manifest:
        data = tomllib.load(manifest)
    try:
        version = data["workspace"]["package"]["version"]
    except (KeyError, TypeError):
        fail("Cargo.toml does not define workspace.package.version")
    if not isinstance(version, str) or SEMVER_PATTERN.fullmatch(version) is None:
        fail(
            f"Expected a stable MAJOR.MINOR.PATCH workspace version, found {version!r}"
        )
    return version


def bumped_version(version: str, level: str) -> str:
    match = SEMVER_PATTERN.fullmatch(version)
    if match is None:
        fail(f"Cannot bump non-stable version {version!r}")
    major, minor, patch = (int(component) for component in match.groups())
    match level:
        case "major":
            return f"{major + 1}.0.0"
        case "minor":
            return f"{major}.{minor + 1}.0"
        case "patch":
            return f"{major}.{minor}.{patch + 1}"
        case _:
            fail(f"Unsupported bump level: {level}")


def choose_bump(version: str) -> str:
    versions = {
        level: bumped_version(version, level) for level in ("major", "minor", "patch")
    }

    table = Table(
        show_header=True, header_style="bold bright_magenta", border_style="bright_blue"
    )
    table.add_column("Hotkey", justify="center", style="bold yellow")
    table.add_column("Bump", style="bold")
    table.add_column("Result", style="bright_cyan")
    table.add_row("m", "Major", versions["major"])
    table.add_row("n", "Minor", versions["minor"])
    table.add_row("p", "Patch", versions["patch"])
    CONSOLE.print(
        Panel.fit(
            table,
            title="[bold bright_magenta]Muxe release[/]",
            subtitle=f"[dim]current {version} · arrows or hotkeys[/]",
            border_style="bright_blue",
            padding=(1, 2),
        )
    )

    if not sys.stdin.isatty():
        fail(
            "Choose a bump explicitly with --major, --minor, or --patch when stdin is not a TTY"
        )

    prompt = questionary.select(
        "Which version should be released?",
        choices=[
            Choice(f"[m]ajor  →  {versions['major']}", value="major"),
            Choice(f"mi[n]or  →  {versions['minor']}", value="minor"),
            Choice(f"[p]atch  →  {versions['patch']}", value="patch"),
        ],
        default="patch",
        use_arrow_keys=True,
        instruction="(Use arrow keys + Enter, or m/n/p)",
        style=PROMPT_STYLE,
    )
    for hotkey, level in (("m", "major"), ("n", "minor"), ("p", "patch")):

        def accept_hotkey(event: Any, selected_level: str = level) -> None:
            event.app.exit(result=selected_level)

        prompt.application.key_bindings.add(hotkey, eager=True)(accept_hotkey)
    selected = prompt.ask()
    if selected is None:
        fail("Release cancelled")
    return selected


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    bump = parser.add_mutually_exclusive_group()
    bump.add_argument(
        "--major",
        dest="level",
        action="store_const",
        const="major",
        help="bump to the next major version",
    )
    bump.add_argument(
        "--minor",
        dest="level",
        action="store_const",
        const="minor",
        help="bump to the next minor version",
    )
    bump.add_argument(
        "--patch",
        dest="level",
        action="store_const",
        const="patch",
        help="bump to the next patch version",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="verify prerequisites and preview cargo-release without changing or pushing anything",
    )
    return parser.parse_args()


def verify_clean_worktree() -> None:
    diff = command(["jj", "diff", "-r", "trunk()..@"], capture=True)
    if diff.strip():
        CONSOLE.print(
            Panel(
                Text(diff.rstrip()),
                title=Text("trunk()..@ is not empty", style="bold red"),
                border_style="red",
            )
        )
        fail(
            "Release must start from trunk() with no working-copy changes; empty commits are allowed"
        )
    CONSOLE.print("[green]✓[/] Working copy has no changes relative to trunk()")


def trunk_commit() -> str:
    commit = command(
        ["jj", "log", "-r", "trunk()", "--no-graph", "-T", 'commit_id ++ "\\n"'],
        capture=True,
    ).strip()
    if re.fullmatch(r"[0-9a-f]{40,64}", commit) is None:
        fail(f"Could not resolve trunk() to one commit: {commit!r}")
    return commit


def github_repository() -> str:
    repository = command(
        ["gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"],
        capture=True,
    ).strip()
    if re.fullmatch(r"[^/\s]+/[^/\s]+", repository) is None:
        fail(f"Could not determine the GitHub repository: {repository!r}")
    return repository


def github_action_checks(repository: str, commit: str) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    page = 1
    total = None
    while total is None or len(checks) < total:
        raw = command(
            [
                "gh",
                "api",
                f"repos/{repository}/commits/{commit}/check-runs?per_page=100&page={page}",
            ],
            capture=True,
            announce=page == 1,
        )
        try:
            payload = json.loads(raw)
            page_checks = payload["check_runs"]
            total = payload["total_count"]
        except (json.JSONDecodeError, KeyError, TypeError):
            fail("GitHub returned an invalid check-runs response")
        if not isinstance(page_checks, list) or not isinstance(total, int):
            fail("GitHub returned an invalid check-runs response")
        checks.extend(
            check
            for check in page_checks
            if check.get("app", {}).get("slug") == "github-actions"
        )
        if len(page_checks) < 100:
            break
        page += 1
    return checks


def verify_github_actions(repository: str, commit: str) -> None:
    checks = github_action_checks(repository, commit)
    if not checks:
        fail(f"No GitHub Actions checks exist for trunk commit {commit[:12]}")

    table = Table(title=f"GitHub Actions · {commit[:12]}", border_style="bright_blue")
    table.add_column("Check", style="cyan")
    table.add_column("Status")
    table.add_column("Conclusion")
    failing: list[dict[str, Any]] = []
    for check in sorted(checks, key=lambda item: str(item.get("name", ""))):
        status = str(check.get("status") or "unknown")
        conclusion = str(check.get("conclusion") or "pending")
        accepted = status == "completed" and conclusion in {"success", "skipped"}
        color = "green" if accepted else "red"
        table.add_row(
            str(check.get("name") or "unnamed"), status, f"[{color}]{conclusion}[/]"
        )
        if not accepted:
            failing.append(check)
    CONSOLE.print(table)
    if failing:
        names = ", ".join(str(check.get("name") or "unnamed") for check in failing)
        fail(f"GitHub Actions checks are not all green or skipped: {names}")
    CONSOLE.print(
        "[green]✓[/] Every GitHub Actions check on trunk() is green or skipped"
    )


def cargo_release_version(level: str, *, execute: bool) -> None:
    arguments = ["cargo", "release", "version", level, "--workspace", "--no-confirm"]
    if execute:
        arguments.append("--execute")
    command(arguments)


def verify_release_changes(expected_version: str) -> None:
    actual_version = workspace_version()
    if actual_version != expected_version:
        fail(
            f"cargo-release produced version {actual_version}, expected {expected_version}"
        )

    changed = set(command(["jj", "diff", "--name-only"], capture=True).splitlines())
    unexpected = changed - ALLOWED_RELEASE_FILES
    missing = ALLOWED_RELEASE_FILES - changed
    if unexpected:
        fail(
            f"Release preparation changed unexpected files: {', '.join(sorted(unexpected))}"
        )
    if missing:
        fail(f"Release preparation did not update: {', '.join(sorted(missing))}")


def pre_release(level: str, expected_version: str) -> tuple[str, str, str]:
    verify_clean_worktree()
    trunk = trunk_commit()
    repository = github_repository()
    verify_github_actions(repository, trunk)

    cargo_release_version(level, execute=True)
    tag = f"v{expected_version}"
    command(["git-cliff", "--tag", tag, "--output", "CHANGELOG.md"])
    verify_release_changes(expected_version)

    command(["jj", "commit", "-m", f"release: {tag}"])
    bump_commit = command(
        ["jj", "log", "-r", "@-", "--no-graph", "-T", 'commit_id ++ "\\n"'],
        capture=True,
    ).strip()
    if re.fullmatch(r"[0-9a-f]{40,64}", bump_commit) is None:
        fail(f"Could not resolve the version bump commit: {bump_commit!r}")

    command(["jj", "bookmark", "set", "main", "-r", "@-"])
    command(["jj", "tag", "set", tag, "-r", "@-"])
    CONSOLE.print(f"[green]✓[/] Prepared {tag} at [cyan]{bump_commit[:12]}[/]")
    return repository, bump_commit, tag


def cargo_release_publish() -> None:
    command(["cargo", "release", "publish", "--workspace", "--execute", "--no-confirm"])


def wait_for_release_workflow(repository: str, commit: str) -> str:
    deadline = time.monotonic() + WORKFLOW_DISCOVERY_TIMEOUT_SECONDS
    with CONSOLE.status(
        "[bold cyan]Waiting for the release workflow to start…[/]", spinner="dots"
    ):
        while time.monotonic() < deadline:
            raw = command(
                [
                    "gh",
                    "run",
                    "list",
                    "--repo",
                    repository,
                    "--workflow",
                    "release.yml",
                    "--event",
                    "push",
                    "--commit",
                    commit,
                    "--limit",
                    "20",
                    "--json",
                    "url,headSha,event",
                ],
                capture=True,
                announce=False,
            )
            try:
                runs = json.loads(raw)
            except json.JSONDecodeError:
                fail("GitHub returned an invalid workflow-runs response")
            if not isinstance(runs, list):
                fail("GitHub returned an invalid workflow-runs response")
            for run in runs:
                if (
                    run.get("headSha") == commit
                    and run.get("event") == "push"
                    and run.get("url")
                ):
                    return str(run["url"])
            time.sleep(WORKFLOW_DISCOVERY_INTERVAL_SECONDS)
    fail(
        f"Release workflow did not appear within {int(WORKFLOW_DISCOVERY_TIMEOUT_SECONDS)} seconds"
    )


def post_release(repository: str, bump_commit: str, tag: str) -> str:
    command(
        ["jj", "git", "push", "--remote", "origin", "--bookmark", "main", "--tag", tag]
    )
    workflow_url = wait_for_release_workflow(repository, bump_commit)
    CONSOLE.print(
        Panel.fit(
            f"[bold green]Release workflow started[/]\n[link={workflow_url}]{workflow_url}[/link]",
            border_style="green",
            padding=(1, 2),
        )
    )
    return workflow_url


def release(level: str, *, dry_run: bool) -> None:
    current_version = workspace_version()
    next_version = bumped_version(current_version, level)
    CONSOLE.print(
        f"[bold]Release plan:[/] [dim]{current_version}[/] → [bold bright_cyan]{next_version}[/] ({level})"
    )

    if dry_run:
        verify_clean_worktree()
        commit = trunk_commit()
        repository = github_repository()
        verify_github_actions(repository, commit)
        cargo_release_version(level, execute=False)
        CONSOLE.print(
            "[bold green]Dry run complete.[/] No files, bookmarks, tags, or remotes were changed."
        )
        return

    repository, bump_commit, tag = pre_release(level, next_version)
    cargo_release_publish()
    post_release(repository, bump_commit, tag)


def main() -> int:
    arguments = parse_arguments()
    try:
        current_version = workspace_version()
        level = arguments.level or choose_bump(current_version)
        release(level, dry_run=arguments.dry_run)
    except KeyboardInterrupt:
        CONSOLE.print("\n[yellow]Release cancelled.[/]")
        return 130
    except ReleaseError as error:
        CONSOLE.print(f"[bold red]Release stopped:[/] {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
