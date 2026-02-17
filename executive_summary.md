# RGW Multisite Sync Monitor — Executive Summary

**The problem:** In Ceph RGW multisite deployments, there is no built-in way to see which specific buckets are experiencing replication lag or by how much. Engineers must manually run multiple CLI commands across different zone nodes and mentally correlate the output — a process that doesn't scale for environments with hundreds or thousands of buckets.

**What this POC does:** It automates the collection of bucket-level sync statistics from both primary and secondary zones, computes per-bucket sync progress as a percentage with delta tracking (objects and bytes behind), and presents everything through a consolidated web dashboard with priority classification (HIGH / MEDIUM / LOW / SYNCED).

**How it works:** A lightweight agent on each secondary zone captures the real sync state — including shard-level detail and sync errors that are only available locally on the pulling side — and pushes processed data to the primary dashboard. This gives engineers a single-pane view of exactly which buckets are lagging, which shards are behind, and what errors are occurring, without SSH-ing between nodes or parsing raw CLI output.

**The impact:** Mean time to identify sync issues drops from hours of manual investigation to seconds of dashboard inspection. Prometheus integration provides the foundation for automated alerting on bucket-level sync lag.