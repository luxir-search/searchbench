#!/usr/bin/env python3
"""Install the newest official snapshot build of a reference engine.

OpenSearch publishes a nightly bundle of its next release from main, packaged
exactly like the release tarball (bundled JDK, release plugin set); Elastic
publishes snapshot builds of Elasticsearch main. The build is resolved from the
vendor's manifest and installed as engines/<engine>-<version>-<build>, beside
searchbench-build.json recording what it is, and engines/<engine>-snapshot is
pointed at it. Selecting OPENSEARCH_BUILD=snapshot or ELASTICSEARCH_BUILD=snapshot
then serves it (python/installs.py).

The server jar records the commit it was built from; installation fails unless
that matches the manifest's.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile

from installs import PROVENANCE_FILE, ROOT, load_versions


def fetch_text(url):
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read().decode("utf-8")


def download(url, out):
    if out.is_file() and out.stat().st_size:
        return
    partial = out.with_name(out.name + ".part")
    subprocess.run(["curl", "--fail", "--location", "--retry", "3", "--continue-at", "-",
                    "--output", str(partial), url], check=True)
    partial.rename(out)


def file_sha512(path):
    digest = hashlib.sha512()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def opensearch_build(spec):
    """The newest nightly: build id, commit and download URL."""
    text = fetch_text(spec["manifest_url"])
    build = re.search(r"^build:\n((?:  .*\n)+)", text, re.M)
    fields = dict(re.findall(r"^  (version|id): '?([^'\n]+)'?$", build.group(1), re.M))
    if fields.get("version") != spec["version"]:
        raise RuntimeError(f"nightly manifest is version {fields.get('version')}, "
                           f"expected {spec['version']}")
    component = re.search(r"^  - name: OpenSearch\n((?:    .*\n)+)", text, re.M)
    source = dict(re.findall(r"^    (repository|ref|commit_id): (.+)$",
                             component.group(1), re.M))
    build_id = fields["id"]
    return {"build_id": build_id, "repository": source["repository"],
            "ref": source["ref"], "commit": source["commit_id"],
            "manifest_url": spec["manifest_url"].replace("/latest/", f"/{build_id}/"),
            "url": spec["url"].format(build_id=build_id), "sha512_url": None,
            "directory": f"opensearch-{spec['version']}-{spec['channel']}-{build_id}"}


def elasticsearch_build(spec):
    """The newest snapshot: build id, commit and download URL."""
    latest = json.loads(fetch_text(spec["latest_url"]))
    if latest["version"] != spec["version"]:
        raise RuntimeError(f"latest snapshot is {latest['version']}, expected {spec['version']}")
    manifest = json.loads(fetch_text(latest["manifest_url"]))
    project = manifest["projects"]["elasticsearch"]
    package = project["packages"][spec["package"]]
    build_id = manifest["build_id"]
    return {"build_id": build_id, "repository": "https://github.com/elastic/elasticsearch.git",
            "ref": project["branch"], "commit": project["commit_hash"],
            "manifest_url": latest["manifest_url"], "url": package["url"],
            "sha512_url": package["sha_url"],
            "directory": f"elasticsearch-{spec['version']}-{build_id.rsplit('-', 1)[-1]}"}


RESOLVE = {"opensearch": opensearch_build, "elasticsearch": elasticsearch_build}


def jar_manifest(home, engine, version):
    jar = home / "lib" / f"{engine}-{version}.jar"
    with zipfile.ZipFile(jar) as archive:
        text = archive.read("META-INF/MANIFEST.MF").decode("utf-8")
    return dict(line.split(": ", 1) for line in text.splitlines() if ": " in line)


def install(engine, root=ROOT):
    engines = Path(root) / "engines"
    spec = load_versions(root)[engine]["snapshot"]
    build = RESOLVE[engine](spec)
    target = engines / build["directory"]
    if not (target / PROVENANCE_FILE).is_file():
        cache = engines / "cache"
        cache.mkdir(parents=True, exist_ok=True)
        archive = cache / f"{build['directory']}.tar.gz"
        download(build["url"], archive)
        sha512 = file_sha512(archive)
        if build["sha512_url"]:
            published = fetch_text(build["sha512_url"]).split()[0]
            if published != sha512:
                raise RuntimeError(f"{archive.name}: SHA-512 {sha512} does not match "
                                   f"the published {published}")
        staging = Path(tempfile.mkdtemp(prefix=".snapshot-", dir=engines))
        try:
            subprocess.run(["tar", "-xzf", str(archive), "-C", str(staging)], check=True)
            (unpacked,) = staging.iterdir()
            jar = jar_manifest(unpacked, engine, spec["version"])
            if jar.get("Change") != build["commit"]:
                raise RuntimeError(f"server jar was built from {jar.get('Change')}, "
                                   f"but the manifest names {build['commit']}")
            provenance = {
                "engine": engine, "channel": spec["channel"], "version": spec["version"],
                "build_id": build["build_id"], "repository": build["repository"],
                "ref": build["ref"], "commit": build["commit"],
                "built_at": jar.get("Build-Date-UTC") or jar.get("Build-Date"),
                "distribution": spec["distribution"], "license": spec["license"],
                "manifest_url": build["manifest_url"], "url": build["url"],
                "sha512": sha512, "verification": spec["verification"],
                "installed_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            (unpacked / PROVENANCE_FILE).write_text(
                json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
            unpacked.rename(target)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
    link = engines / f"{engine}-snapshot"
    staged_link = engines / f".{engine}-snapshot.new"
    staged_link.unlink(missing_ok=True)
    staged_link.symlink_to(target.name)
    os.replace(staged_link, link)
    provenance = json.loads((target / PROVENANCE_FILE).read_text(encoding="utf-8"))
    print(f"{engine} snapshot: {target.name} ({provenance['ref']} {provenance['commit'][:9]}, "
          f"built {provenance['built_at']})")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("engine", choices=tuple(RESOLVE))
    install(parser.parse_args().engine)


if __name__ == "__main__":
    main()
