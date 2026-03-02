# TM2 Runtime Performance: Findings and Uncertainty

**Date:** March 2, 2026  
**To:** TM2 Technical Team  
**Subject:** tm2py Runtime Benchmarking — Summary of Findings

---

## Bottom Line

A 1-day runtime on MTC M2 is most likely achievable, like 75% probability, but confirming it will require approximately one to two more week of focused testing. The evidence points strongly in the right direction: the February 2026 runs on MTC M2 revealed two important and non-obvious findings about how EMME and Java perform on large servers that, once addressed, are expected to bring total runtime within the 1-day target.

We were not able to acheive the runtime with current testing timeline.

1. **Transit performance degrades at very high processor counts.** Running transit skim and transit assignment at MAX-1 (207 cores) makes both components significantly *slower* than running them with a constrained processor count. Skim took 241–362 min per iteration at MAX-1, compared to ~69 min at 32 processors per the consultant's sweep. The current config already applies this fix (`num_processors_transit_skim = "32"`); the same setting is needed for transit assignment.
2. **JVM heap configuration matters on large-memory servers.** With the default JVM settings, CTRAMP did not scale with MTC M2's larger memory and ran at the same speed as MTC M1. Correcting this is expected to unlock substantial CTRAMP speedup on M2, consistent with the consultant's M2 results (~62/78/113 min across iterations vs. 133/180/278 min observed).
3. **The current codebase is meaningfully faster than the consultant's baseline** for transit because improvements made over the past six months. This improvement is real, and its full magnitude will be confirmed by the next clean M2 run.

With these corrections applied, a rough projection puts MTC M2 runtime consistent with the 1-day target. A clean run incorporating all three changes is the next step.

---

## Background

There are four hardware/software configurations in play, from two organizations. The consultant team ran benchmarks on their own machines using an earlier codebase (~v2.2.1.3); MTC has separately tested the current codebase on its own hardware. These are **not the same machines** and not the same codebase.

| Specification | Consultant M1 | Consultant M2 | MTC M1  MTC M2 (server) |
|---|---|---|---|---|
| CPU | AMD EPYC 7763 | AMD EPYC 9V33X | Intel Xeon Gold 6338 | Unknown |
| Cores / logical CPUs | 64 | 176 (2 × 96) | 48 vCPUs (VM: 24 virtual sockets × 2 cores) | 208 |
| Clock speed | 2.44 GHz | 2.55 GHz | 2.00 GHz | Unknown |
| RAM | 512 GB | 1.37 TB | 512 GB | 2.8 TB |
| OS | — | — | Windows Server 2019 | — |
| Storage | — | — | C: 80 GB / E: ~1 TB | — |
| Code version | ~v2.2.1.3 (Aug 2024) | ~v2.2.1.3 (Aug 2024) | develop branch (Jan 2026) | — |
| Consultant runtime | 71.7 hours | 35.8 hours | — | — |

The goal is to reduce MTC M2 runtime toward 1.0 day. **The only fully reliable data is the February 4, 2026 run log on MTC M1** (`tm2py_run_20260204_0819.log`). MTC M2 has six partial or complete run logs from February 20–26, all with known configuration problems (see below), but they still yield useful component-level observations.

A partial MTC M2 run was also conducted on February 27, 2026; however, CTRAMP was ~4× slower than expected due to misconfigured JVM settings, so that run cannot be used as a reliable Machine 2 baseline. The JVM configuration issue is documented under Uncertainties.

### Code changes since the consultant baseline

Git history identifies the following performance-relevant changes made after the consultant baseline (~v2.2.1.3, August 2024):

| Commit date | Change |
|---|---|
| 2025-05-28 | Skip unnecessary skimming steps (`transit_skim.py`) |
| 2025-06-19 | PR #201: Improve transit skim (restructured for efficiency) |
| 2025-07-07 | PR #205: `num_processors_transit_skim` config parameter added (tag v2.2.1.4) |
| 2026-01-13 | Remove TAP (Transit Access Point) infrastructure: deleted WLK\_TRN\_PNR (park-and-ride), KNR\_TRN\_WLK and WLK\_TRN\_KNR (kiss-and-ride) journey levels — ~450 lines removed from `transit_assign.py` |

---

## MTC M2 Observed Data (Feb 20–26, 2026)

Six runs were conducted on MTC M2 during the week of Feb 20–26. All used `num_processors = "MAX-1"` (207 logical CPUs) for all EMME components including transit. JVM settings were also misconfigured in all runs, meaning CTRAMP ran slower than it would on a correctly configured machine. The Feb 20 run is the only one that completed all iterations; the Feb 23–26 runs stopped at or before the end of iteration 1 due to either crashes or restarts.

Component timings from the Feb 20 complete run (all iterations using `num_processors = "MAX-1"`):

| Component | Iter 1 (15%) | Iter 2 (25%) | Iter 3 (50%) |
|---|---|---|---|
| CTRAMP | 133 min | 180 min | 278 min |
| Highway assignment | 233 min | 316 min | 318 min |
| Transit assignment | 227 min | 370 min | 606 min |
|    AM period only | 33 min | 123 min | 126 min |
|    inter-period processing gaps | ~11 min each | ~12 min each | ~58 min each |
| Transit skim | 241 min | 300 min | 362 min |

**CTRAMP** ran at the same speed as MTC M1 — 133 min for iter 1 — confirming the JVM was misconfigured from the start and CTRAMP was not benefiting from the larger machine.

**Highway assignment** ran no faster than MTC M1 (233 min vs 239 min in iter 1). The Feb 23 run, which has per-period detail before it stopped, shows EA ~26 min and AM ~35 min sequentially, suggesting total 5-period highway time of ~150–160 min under comparable conditions. The Feb 20 iter 1 highway time (233 min) may reflect additional overhead from network prep or cold-start conditions.

**Transit assignment** gets dramatically worse over iterations. In iter 3, the inter-period processing gaps — the time between completing one period's assignment and starting the next — expand from ~11 min to ~58 min each. The AM period itself takes 123–126 min in iters 2 and 3 (vs 33 min in iter 1 when the network is uncongested). The congested transit assignment is sensitive to both network loading and processor count; this pattern was also observed by the consultant team.

**Transit skim** at MAX-1 (207 processors) took 241–362 min across iterations. The consultant's controlled sweep showed that MAX-1 on their M2 (175 processors) produced 161 min, while reducing to 36 processors yielded 69 min. Our M2 results at 207 processors are consistent with this: more processors actually hurts skim performance.

---

## Key Findings

### 1. CTRAMP runtime is reliable and consistent across hardware generations

The Feb 4 MTC M1 run (132.6 min for iter 1 at 15% sample rate) matches the consultant M1 result (133.1 min) to within 1% — **despite different hardware** (Intel Xeon Gold 6338 VM with 48 vCPUs at 2.00 GHz vs AMD EPYC 64-core at 2.44 GHz). This confirms that CTRAMP has not regressed in the current codebase and that CTRAMP is not particularly sensitive to moderate differences in core count or CPU architecture at this scale. MTC M2 (208 logical CPUs, 2.8 TB RAM) has substantially more resources than either M1, so its CTRAMP times should be significantly faster; the consultant M2 (~62/78/113 min) provides a rough lower bound.

### 2. Transit assignment is substantially faster in the current codebase, with identifiable code causes

The Feb 4 MTC M1 transit assignment (iter 1: 234 min) ran approximately **half the time** of the consultant M1 runs (481–508 min). The hardware is different (Intel Xeon Gold 6338 VM with 48 vCPUs vs AMD EPYC 64-core), but that alone does not explain a 2× reduction. Git history identifies the likely cause: **the TAP infrastructure removal (January 13, 2026)** deleted three transit journey level types — walk-to-transit-with-park-and-ride and two kiss-and-ride variants — that were present in the consultant codebase. If these ran during the consultant's transit assignment, they would have substantially increased assignment time. The commit note describes them as vestigial in current runs, but their presence in the older code means the consultant's runs were doing more work per assignment call.

This is a **plausible and likely explanation**, but cannot be fully confirmed without the consultant's `model_config.toml` to verify whether those journey levels were active.

### 3. Transit skims and assignment are both harmed by too many processors

A systematic sweep by the consultant on their M2 showed that transit skimming drops from 161 min (MAX-1 = 175 processors) to **69 min at 36 processors**. Our MTC M2 observed data corroborates this: transit skim at MAX-1 (207 processors) took 241–362 min per iteration — far slower than the already-slow consultant MAX-1 result. The current config uses `num_processors_transit_skim = "32"`, which captures most of this gain. Transit assignment also appears to degrade with excessive processor counts; the consultant observed the same pattern. Both components should use a limited processor count (~32–36) on large machines.

### 4. Highway assignment shows no improvement on MTC M2 at this scale

MTC M2 highway assignment iter 1 (233 min) matched MTC M1 (239 min) despite having 4× more cores. Per-period detail from the Feb 23 run (EA: 26 min, AM: 35 min sequentially) suggests total 5-period time of ~150–160 min under less stressed conditions. Running with `num_processors = "MAX-1"` and no parallel subprocesses appears to saturate diminishing returns from SOLA parallelism. A processor sweep for highway assignment on M2 — analogous to the transit skim sweep — has not been done.

### 5. Projected MTC M2 runtime under corrected configuration is approximately 1 day, but the estimate is an inference

No clean MTC M2 run exists with correct JVM settings and optimal processor counts. The projection is built from the consultant M2 baseline (2,148 min) adjusted for: (a) current codebase transit improvements, (b) `num_processors_transit_skim = "32"` (saving ~250–300 min across 3 iterations), and (c) JVM fix enabling CTRAMP to run at the consultant's M2 speed (~62/78/113 min vs 133/180/278 min observed). The combined savings from these three factors could plausibly reduce runtime to **~1,100–1,400 min (~18–23 hours)**. The transit assignment congestion behavior in later iterations and the unresolved highway scaling are the main sources of uncertainty.

---

## Key Uncertainties

| Uncertainty | Impact | How to Resolve |
|---|---|---|
| Consultant vs current transit assignment config not directly comparable | Transit improvement may be overstated or understated | Retrieve consultant's `model_config.toml` and compare `transit.congested.stop_criteria` |
| All MTC M2 runs had misconfigured JVM settings | CTRAMP ran at MTC M1 speed (~133 min iter 1) rather than expected M2 speed (~62 min); CTRAMP savings not yet demonstrated on M2 | Re-run on M2 with correct JVM heap settings |
| Feb 27 Machine 2 run had misconfigured JVM settings | CTRAMP was ~4x slower than expected; run cannot be used as a Machine 2 baseline | Re-run on Machine 2 with correct Java settings |
| `num_processors` for transit assignment not controlled on MTC M2 | Was MAX-1 (207) in all runs; likely contributing to inter-period gap explosion in iter 3 | Test transit assignment with 32–36 processors on M2 |
| Highway assignment scaling on M2 not understood | 233 min per iter 1 — same as M1 despite 4× more cores | Run a `num_processors` sweep for highway on M2; compare with sequential M1 results |
| tm2py code changes since consultant baseline | Unknown effect on component runtimes | Identify specific commits between v2.2.1.3 and current `develop` affecting transit assignment |

---

## Recommendations

1. **Fix JVM settings for MTC M2 and run a complete iteration** to establish a reliable CTRAMP baseline. All Feb 20–26 runs had incorrect JVM config; CTRAMP gains from the larger machine have not been realized.
2. **Set `num_processors_transit_skim` and `num_processors` (transit assignment) to ~32–36** for any M2 run. Both skim and assignment are harmed by MAX-1 on this machine, as confirmed by both the consultant's sweep data and our observed MTC M2 run times.
3. **Investigate the transit assignment inter-period gap expansion** in iter 3 (from ~11 min to ~58 min per gap). This alone added ~250 min to the iter 3 assignment time and may be related to memory pressure or disk I/O under high processor count.
4. **Run a highway processor sweep on M2** to determine whether a reduced `num_processors` also improves highway assignment, as was observed for transit. Current MTC M2 highway times (233 min) match MTC M1 with no benefit from the larger machine.
5. **Document the `num_processors_transit_skim = "32"` finding** as a confirmed, reproducible optimization and include it in production configuration guidance (see issue #194).


---

## Background

There are four hardware/software configurations in play, from two organizations. The consultant team ran benchmarks on their own machines using an earlier codebase (~v2.2.1.3); MTC has separately tested the current codebase on its own hardware. These are **not the same machines** and not the same codebase.

| Specification | Consultant M1 | Consultant M2 | MTC M1 (this machine) | MTC M2 (server) |
|---|---|---|---|---|
| CPU | AMD EPYC 7763 | AMD EPYC 9V33X | Intel Xeon Gold 6338 | Unknown |
| Cores / logical CPUs | 64 | 176 (2 × 96) | 48 vCPUs (VM: 24 virtual sockets × 2 cores) | 208 |
| Clock speed | 2.44 GHz | 2.55 GHz | 2.00 GHz | Unknown |
| RAM | 512 GB | 1.37 TB | 512 GB | 2.8 TB |
| OS | — | — | Windows Server 2019 | — |
| Storage | — | — | C: 80 GB / E: ~1 TB | — |
| Code version | ~v2.2.1.3 (Aug 2024) | ~v2.2.1.3 (Aug 2024) | develop branch (Jan 2026) | — |
| Consultant runtime | 71.7 hours | 35.8 hours | — | — |

The goal is to reduce MTC M2 runtime toward 1.0 day. **The only data we are fully confident in is the February 4, 2026 run log on MTC M1** (`tm2py_run_20260204_0819.log`). All statements about MTC M2 or cross-machine comparisons require explicit inference and carry higher uncertainty.

A partial MTC M2 run was also conducted on February 27, 2026; however, CTRAMP was ~4× slower than expected due to misconfigured JVM settings, so that run cannot be used as a reliable Machine 2 baseline. The JVM configuration issue is documented under Uncertainties.

### Code changes since the consultant baseline

Git history identifies the following performance-relevant changes made after the consultant baseline (~v2.2.1.3, August 2024):

| Commit date | Change |
|---|---|
| 2025-05-28 | Skip unnecessary skimming steps (`transit_skim.py`) |
| 2025-06-19 | PR #201: Improve transit skim (restructured for efficiency) |
| 2025-07-07 | PR #205: `num_processors_transit_skim` config parameter added (tag v2.2.1.4) |
| 2026-01-13 | Remove TAP (Transit Access Point) infrastructure: deleted WLK\_TRN\_PNR (park-and-ride), KNR\_TRN\_WLK and WLK\_TRN\_KNR (kiss-and-ride) journey levels — ~450 lines removed from `transit_assign.py` |

---

## Key Findings

### 1. CTRAMP runtime is reliable and consistent across hardware generations

The Feb 4 MTC M1 run (132.6 min for iter 1 at 15% sample rate) matches the consultant M1 result (133.1 min) to within 1% — **despite different hardware** (Intel Xeon Gold 6338 VM with 48 vCPUs at 2.00 GHz vs AMD EPYC 64-core at 2.44 GHz). This confirms that CTRAMP has not regressed in the current codebase and that CTRAMP is not particularly sensitive to moderate differences in core count or CPU architecture at this scale. MTC M2 (208 logical CPUs, 2.8 TB RAM) has substantially more resources than either M1, so its CTRAMP times should be significantly faster; the consultant M2 (~62/78/113 min) provides a rough lower bound.

### 2. Transit assignment is substantially faster in the current codebase, with identifiable code causes

The Feb 4 MTC M1 transit assignment (iter 1: 234 min) ran approximately **half the time** of the consultant M1 runs (481–508 min). The hardware is different (Intel Xeon Gold 6338 VM with 48 vCPUs vs AMD EPYC 64-core), but that alone does not explain a 2× reduction. Git history identifies the likely cause: **the TAP infrastructure removal (January 13, 2026)** deleted three transit journey level types — walk-to-transit-with-park-and-ride and two kiss-and-ride variants — that were present in the consultant codebase. If these ran during the consultant's transit assignment, they would have substantially increased assignment time. The commit note describes them as vestigial in current runs, but their presence in the older code means the consultant's runs were doing more work per assignment call.

This is a **plausible and likely explanation**, but cannot be fully confirmed without the consultant's `model_config.toml` to verify whether those journey levels were active.

### 3. Transit skimming responds strongly to `num_processors`

A systematic sweep on Machine 2 showed that transit skimming time drops from 161 min (at MAX-1 = 175 processors) to **69 min at 36 processors** — a 2.3x improvement from simply reducing the processor count. The current config uses `num_processors_transit_skim = "32"`, which captures most of this gain. This finding is well-controlled and reliable.

### 4. Highway assignment does not benefit from parallelization at this scale

Running the five time-period assignments in parallel (splitting Machine 2 resources three ways) increased total highway assignment time from ~60 min to ~82 min. EMME's SOLA algorithm makes efficient use of all available cores when run sequentially; dividing cores across parallel processes negates this. Sequential assignment on Machine 2 with MAX-1 processors is the recommended approach.

### 5. Projected MTC M2 runtime is approximately 1 day, but the estimate is an inference

MTC M2 (208 logical CPUs, 2.8 TB) has more resources than consultant M2 (176 cores), but we have no current-codebase log from it. Starting from the consultant M2 baseline (2,148 min) — which is itself on different hardware — the current codebase improvements (transit assignment simplification and skim optimization) and the skim processor setting (`num_processors_transit_skim = "32"`) could plausibly reduce total runtime by 700–800 min. This puts a rough projection at **~1,300–1,400 min (~22–23 hours)**. The direction of the estimate is reliable; the specific number should be treated as an order-of-magnitude figure until a clean MTC M2 run is completed.

---

## Key Uncertainties

| Uncertainty | Impact | How to Resolve |
|---|---|---|
| Consultant vs current transit assignment config not directly comparable | Transit improvement may be overstated or understated | Retrieve consultant's `model_config.toml` and compare `transit.congested.stop_criteria` |
| Feb 27 Machine 2 run had misconfigured JVM settings | CTRAMP was ~4x slower than expected; run cannot be used as a Machine 2 baseline | Re-run on Machine 2 with correct Java settings |
| `num_processors` for transit assignment not swept on Machine 2 | Optimal setting may differ from Machine 1; further savings may be available | Run a `num_processors` sweep for transit assignment on Machine 2 analogous to the skim sweep |
| tm2py code changes since consultant baseline | Unknown effect on component runtimes | Identify specific commits between v2.2.1.3 and current `develop` affecting transit assignment |

---

## Recommendations

1. **Accept the 1-day Machine 2 target as feasible** based on current evidence, with the caveat that it requires a clean, properly configured run.
2. **Conduct a controlled comparison run on Machine 2** using default JVM settings and the current `model_config.toml` to establish a current-codebase Machine 2 baseline.
3. **Investigate the source of the transit assignment speedup** before reporting it as a confirmed improvement — it may reflect a modeling change (fewer inner iterations) rather than a software efficiency gain.
4. **Document the `num_processors_transit_skim = "32"` finding** as a confirmed, reproducible optimization for Machine 2 and include it in production configuration guidance (see issue #194).
