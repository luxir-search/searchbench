#!/usr/bin/env python3
"""Which installation of a reference engine a run serves.

The pinned release in engines/versions.json is the default. OPENSEARCH_BUILD
and ELASTICSEARCH_BUILD select another installation by name instead,
engines/<engine>-<build>. `snapshot` is the official build of the next release
that `engines/download.sh <engine>-snapshot` installs and points at; the
directory name of one installed build pins it. Every such installation carries
the searchbench-build.json written when it was installed, so a result names
exactly what served it.
"""

import argparse
from functools import lru_cache
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILD_ENV = {"opensearch": "OPENSEARCH_BUILD", "elasticsearch": "ELASTICSEARCH_BUILD"}
PROVENANCE_FILE = "searchbench-build.json"


def load_versions(root=ROOT):
    with open(Path(root) / "engines" / "versions.json", encoding="utf-8") as source:
        return json.load(source)


def selected_build(engine):
    """The build name selected for ENGINE, or None for the pinned release."""
    return os.environ.get(BUILD_ENV[engine]) or None


def engine_install(engine, root=ROOT, versions=None):
    """{home, version, distribution, license, build} for ENGINE's installation.

    build is None for the pinned release, else the installation's provenance.
    """
    build = selected_build(engine)
    if build is None:
        release = (versions or load_versions(root))[engine]
        return {"home": Path(root) / "engines" / f"{engine}-{release['version']}",
                "version": release["version"],
                "distribution": release["distribution"],
                "license": release["license"], "build": None}
    home = Path(root) / "engines" / f"{engine}-{build}"
    provenance_path = home / PROVENANCE_FILE
    if not provenance_path.is_file():
        raise RuntimeError(
            f"{BUILD_ENV[engine]}={build}: no {provenance_path}; "
            f"install one with engines/download.sh {engine}-snapshot")
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    return {"home": home, "version": provenance["version"],
            "distribution": provenance["distribution"],
            "license": provenance["license"], "build": provenance}


@lru_cache(maxsize=None)
def selected_build_identity(engine):
    """(build_id, commit) of the build ENGINE serves now; None for the
    release, and for engines without separate builds."""
    if engine not in BUILD_ENV:
        return None
    return build_identity(engine_install(engine)["build"])


def build_identity(build):
    return None if build is None else (build["build_id"], build["commit"])


def version_label(engine_config):
    """Short version for report tables; a build names its id and commit."""
    build = engine_config.get("build")
    if build:
        return (f"{engine_config['version']} {build['channel']} {build['build_id']} "
                f"({build['commit'][:9]})")
    return str(engine_config.get("version", "N/A"))[:12]


def check_served_version(engine, install, served):
    """Fail unless the running server is the installation the result records.

    served is the `version` object of the server's GET / response.
    """
    build = install["build"]
    expected = {"number": install["version"]}
    if build is not None:
        expected["build_hash"] = build["commit"]
    mismatched = {key: (served.get(key), value) for key, value in expected.items()
                  if served.get(key) != value}
    if mismatched:
        raise RuntimeError(
            f"{engine} serves {mismatched} (served, expected): not the installation "
            "this run selected")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", choices=("home",))
    parser.add_argument("engine", choices=tuple(BUILD_ENV))
    args = parser.parse_args()
    try:
        print(engine_install(args.engine)["home"])
    except RuntimeError as error:
        raise SystemExit(str(error)) from None


if __name__ == "__main__":
    main()
