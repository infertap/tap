# Validation results

Validation covers event mapping, identity derivation, deployment lifecycle, isolation,
and resource behavior. The [support matrix](compatibility.md) defines the supported
configurations; test results describe the particular workloads that were run.

## 0.1 validation summary

| Area | Result |
| --- | --- |
| vLLM 0.30.0 features | Tensor parallelism 2 and 4, dynamic LoRA, hybrid groups, CPU and filesystem offload, replay, and cache reset checks passed within the documented identity limits. |
| SGLang 0.5.20 features | Prefix reuse, node splits, cache salts, evictions, a cache flush, publisher replay after a gap, and the hierarchical cache's host tier passed on one A40; the engine announces no reuse. See [SGLang feature checks](validation/0.1.md#sglang-0520-feature-checks). |
| Kubernetes | Single-GPU, two local data-parallel ranks, and two single-GPU pods passed lifecycle, isolation, and shipping checks. |
| Sustained collection | A one-hour A10G workload produced 419,880 block events with matching independent capture and identity calculations; peak tap RSS was 9.32 MiB. |
| Capacity pressure | A separate synthetic run exercised identity-cap refusal and segment retention. |
| ARM64 | Native build and transport tests passed. Real GPU integration remains outside the support scope. |
| Object-store KV offload | Experimental; stock vLLM required the documented telemetry initialization patch. |

The one-hour workload saturated a deliberately reduced KV cache, not physical GPU memory.
These measurements are workload-specific and are not throughput or memory guarantees.

The [full report](validation/0.1.md) records hardware, models, engine versions, workload
scope, and historical checks. It includes incomplete-history and engine-failure results.
Use [Release acceptance](release-acceptance.md) to validate a new packaged candidate.
