- Feature Name: Out-of-disk handling
- Status: draft
- Start Date: 2021-06-03
- Authors: Jackson Owens
- RFC PR: (PR # after acceptance of initial draft)

# Summary

This RFC addresses issues related to disk-space exhaustion. This document
proposes automated mechanisms to prevent total disk-space exhaustion and
provide means for recovery.

This design proposes the introduction of node-level and cluster-level
out-of-disk modes that restrict database functionality when available disk
space is severely limited. These out-of-disk modes seek to prevent permanent
data loss and node crashes through limiting availability before total disk
space exhaustion makes recovery impossible.

# Motivation

A CockroachDB store may run out of disk space because the operator has
insufficient monitoring, an operator is unable to respond in time, etc.  A
CockroachDB store that exhausts available disk space crashes the node.  This
is especially problematic within CockroachCloud, where Cockroach Labs SREs are
responsible for the availability of CockroachDB but have no control over the
customer workload.

Recovering from disk space exhaustion is tricky. Deleting data within
CockroachDB requires writing tombstones to disk and rewriting new immutable
sstables before removing old ones. Adding new nodes also requires writing to
existing nodes. The current recommended solution is to reserve a limited
amount of disk space in a ballast file when initializing stores. If a store
exhausts available disk space, the ballast file may be manually removed to
provide some headroom to process deletions.

Within the CockroachCloud environment, customers cannot access nodes'
filesystems to manage ballasts, and CockroachLabs SRE alerting cannot
differentiate between unavailability due to a customer-induced disk space
exhaustion and other issues. Additionally, customer support has observed some
on-premise customers forget to setup ballasts, accidentally risking data loss.

The automated out-of-disk modes proposed within this RFC seek to prevent node
crashes from disk-space exhaustion and provide easier recovery in both
on-premise and CockroachCloud deployments.

# Technical design

This RFC proposes the introduction of an out-of-disk mode that intentionally
limits a node's availability once a store's capacity falls beneath a
configurable threshold.

## Detection

Preventing total disk space exhaustion requires detecting it in advance. This
RFC introduces a new cluster setting that allows customers to configure the
amount of disk space headroom to preserve:

```
SET CLUSTER SETTING storage.out_of_disk.headroom = '2GiB';
```

New clusters are initialized with a reasonable default headroom.  Setting the
headroom to zero disables out-of-disk protection. This proposal uses two
separate approaches to detecting out-of-disk conditions and preserving
headroom, which in concert help work around each's individual limitations.

**Disk capacity monitoring**
CockroachDB periodically computes capacity metrics that are exposed as
time-series metrics and used internally to drive decision making, such as
rebalancing. These capacity metrics may be used to trigger out-of-disk
handling when available capacity falls below the configured headroom and in
advance of actually exhausting disk space. When the out-of-disk module
observes available capacity pass the threshold, it enables a _node-local_
out-of-disk mode discussed below.

Disk space usage metrics may lag actual usage and may be ineffective alone at
reliably preventing exhaustion.

**Observing `ENOSPC`**
In addition we propose the introduction of an automated ballast file at half
the configured headroom's size. Whenever the `storage.out_of_disk.headroom`
cluster setting is modified, CockroachDB updates an AUTO_BALLAST file in every
store's auxiliary directory with size `headroom/2`. Additionally, every
store's engine is configured with a special
[virtual filesystem](https://pkg.go.dev/github.com/cockroachdb/pebble/vfs#FS).
This VFS inspects all write operation errors, calling into the out-of-disk
monitor when an `ENOSPC` error is observed. The out-of-disk monitor

* removes the automated ballast
* enables node-local out-of-disk mode
* informs the virtual filesystem that it should retry the original write
  operation

The removal of the ballast frees disk space, ensuring the retried writes
succeed, and the `ENOSPC` errors are not propagated to higher levels where
they might crash the node.

**Caveat**: An `ENOSPC` encountered during `fsync` is not retriable because
Linux and many other operating systems will mark buffers that failed to sync
as clean. A future successful `fsync` will not sync the previous buffers.  If
an `ENOSPC` is encountered while syncing the Pebble write-ahead log or file
manifest, the node must crash to ensure consistency. An `ENOSPC` error in
these codepaths is less likely here than in other, recoverable codepaths
because WAL recycling and the comparably small write volume of the manifest.
It is likely enough to occur that we shouldn't alone depend on recovery from
`ENOSPC` errors.

## Node-local out-of-disk mode




## Drawbacks

...

## Rationale and Alternatives

...

# Explain it to folk outside of your team

Audience: PMs, doc writers, end-users, CockroachDB team members in other areas of the project.

# Unresolved questions

Audience: all participants to the RFC review.
