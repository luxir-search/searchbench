# Running Searchbench

Read [README.md](README.md#isolated-network-runs) for the shared user/agent
workflow and [docs/full-text-benchmark.md](docs/full-text-benchmark.md) for the
benchmark definition.

- Run performance campaigns through `scripts/run-isolated.sh COMMAND ...`.
  Put engine startup, ingestion, health checks, replay, and shutdown inside
  the same invocation. Wrapping only the replay client cannot reach an engine
  started on the host's loopback.
- Complete setup, builds, and downloads before entering the namespace. For
  the three-engine standard baseline: `scripts/setup.sh --standard --references`.
- Stop existing Searchbench engines before entering. Run only one campaign in
  a checkout at a time; data directories, configuration, logs, and PID files
  are shared. The wrapper refuses existing engine PID files and overlapping
  wrapper invocations.
- Set and restore CPU policy outside the wrapper, using the host's normal
  facility. On hosts with `/usr/local/sbin/agent_do`, the README shows an outer
  cleanup trap. Network isolation itself needs no sudo or helper changes.
- Use explicit result directories for new comparison campaigns. Preserve old
  baselines, and rerun all compared engines with the same network/CPU/cache
  settings. Record intentional host-network diagnostics separately.
- The wrapper cleans up remaining namespace processes, including engines that
  `quick.sh` normally leaves running. Wrap a shell if several commands should
  share one running engine. SIGINT/SIGTERM trigger cleanup; SIGKILL cannot.
- Check result errors, count agreement, and recorded provenance before calling
  a campaign successful. Some existing campaign scripts continue after a
  failed cell, so a zero campaign exit status alone is insufficient.

Harness checks: `python3 -m unittest discover -s python -p 'test_*.py'`.
Namespace integration tests need Linux with unprivileged user/network
namespaces enabled, util-linux (`unshare`, `setpriv`), and iproute2 (`ip`).
