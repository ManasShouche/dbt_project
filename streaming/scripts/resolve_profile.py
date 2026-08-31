\
\
\
\
\
\

import argparse
import os
import pathlib
import sys

try:
    import yaml
except ImportError:
    sys.exit(
        "PyYAML not found. Run this with the project venv:\n"
        "  ../.venv/bin/python scripts/resolve_profile.py ..."
    )

REQUIRED = ["account", "user", "role", "database", "schema", "private_key_path"]

def find_profiles(explicit: str | None) -> pathlib.Path:
    candidates = []
    if explicit:
        candidates.append(pathlib.Path(explicit).expanduser())
    else:
        candidates.append(pathlib.Path(__file__).resolve().parent.parent / "profiles.yml")
        candidates.append(pathlib.Path("~/.dbt/profiles.yml").expanduser())

    for c in candidates:
        if c.is_file():
            return c
    sys.exit("No profiles.yml found. Looked in:\n  " + "\n  ".join(str(c) for c in candidates))

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default=os.environ.get("CONNECTOR_PROFILE", "tpch_stream"))
    ap.add_argument("--target", default=os.environ.get("CONNECTOR_TARGET"))
    ap.add_argument("--profiles-path", default=os.environ.get("CONNECTOR_PROFILES_PATH"))
    args = ap.parse_args()

    path = find_profiles(args.profiles_path)
    data = yaml.safe_load(path.read_text()) or {}

    if args.profile not in data:
        available = ", ".join(k for k in data if isinstance(data[k], dict)) or "(none)"
        sys.exit(f"Profile '{args.profile}' not in {path}. Available: {available}")

    profile = data[args.profile]
    outputs = profile.get("outputs") or {}
    target = args.target or profile.get("target")

    if not target:
        sys.exit(f"Profile '{args.profile}' has no `target` and none was passed with --target.")
    if target not in outputs:
        sys.exit(
            f"Target '{target}' not in profile '{args.profile}'. "
            f"Available: {', '.join(outputs) or '(none)'}"
        )

    out = outputs[target]

    if "type" in out and out["type"] != "snowflake":
        sys.exit(f"Target '{target}' has type '{out['type']}'; the Snowflake sink needs snowflake.")

    missing = [k for k in REQUIRED if not out.get(k)]
    if missing:
        sys.exit(f"Target '{args.profile}.{target}' is missing: {', '.join(missing)}")

    key_path = pathlib.Path(str(out["private_key_path"])).expanduser()
    if not key_path.is_absolute():
        key_path = (path.parent / key_path).resolve()
    if not key_path.is_file():
        sys.exit(f"private_key_path does not exist: {key_path}")

    emit = {
        "SNOWFLAKE_ACCOUNT": out["account"],
        "SNOWFLAKE_USER": out["user"],
        "SNOWFLAKE_ROLE": out["role"],
        "SNOWFLAKE_DATABASE": out["database"],
        "SNOWFLAKE_SCHEMA": out["schema"],
        "SNOWFLAKE_PRIVATE_KEY_PATH": str(key_path),
        "RESOLVED_PROFILE": args.profile,
        "RESOLVED_TARGET": target,
        "RESOLVED_PROFILES_PATH": str(path),
    }
    for k, v in emit.items():
        print(f"{k}={v}")

if __name__ == "__main__":
    main()
