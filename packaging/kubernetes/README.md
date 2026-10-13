# Kubernetes deployment

Run one tap sidecar beside each engine pod. The tap reads the local event publisher and
writes segments to a shared `emptyDir`; a shipper sidecar uploads sealed segments to
object storage.

## Requirements

- A vLLM 0.30.0 Deployment with its model, GPU resources, and serving configuration.
- A network plugin that enforces Kubernetes NetworkPolicy.
- A 64-byte pseudonymization key, encoded as hex or base64, provisioned through a Secret.
- Object-store credentials and a destination prefix for the shipper.
- Capacity for the sidecars and local segment storage, as specified in `tap-sidecar.yaml`.

The tap does not require GPU resources or Kubernetes API access.
See [Compatibility and support](../../docs/compatibility.md) for feature limitations.

## Configure the engine

Enable prefix caching and the ZMQ event publisher:

```sh
vllm serve <model> --kv-events-config '{"enable_kv_cache_events": true, "publisher": "zmq", "endpoint": "tcp://*:5557", "replay_endpoint": "tcp://*:5657"}'
```

Apply `network-policy.yaml` before enabling the publisher. Its ports carry tokenized
prompts without authentication. The policy permits the serving port and blocks inbound
event-port access; the tap connects through the pod's loopback. Match the policy selector
to your engine labels and its serving port to your deployment.

## Configure the sidecars

Use copies of these files in your deployment configuration:

| File | Purpose |
| --- | --- |
| `tap-sidecar.yaml` | Patch that adds tap and shipper containers and shared volumes |
| `tap-config.yaml` | Tap configuration, Secret templates, and shipper script |
| `network-policy.yaml` | Event-port isolation |
| `example-vllm-deployment.yaml` | Complete example engine Deployment with sidecars |
| `kustomization.yaml` | Builds the example deployment |

Set the engine image to `vllm/vllm-openai:v0.30.0` and the tap image to your release tag
or immutable digest. Published tap images use `ghcr.io/infertap/tap:<tag>`; before a
release is available, build and publish the [container image](../../docs/install.md#container)
to your own registry.

In `tap-config.yaml`, configure:

- `tap.toml`: rank counts, retention, and other tap options.
- `cluster`: a unique cluster name when multiple clusters share an analysis domain.
- `infertap-key`: the key from your secret store. Keep it stable across restarts.
- `infertap-shipper`: the destination and credentials. `INFERTAP_DEST` must end in `/kv/`.

Provision secrets through your deployment tooling. Never commit populated Secret manifests.
For S3, grant prefix-scoped PutObject and GetObject, plus ListBucket restricted to that
prefix. The [configuration reference](../../docs/install.md#configuration-file-and-environment)
describes the tap options.

Set the engine model and the tap's model provenance to the served model name. Set
`terminationGracePeriodSeconds` to at least 96 seconds with the default shipper budgets;
the example uses 120 seconds. Preserve a longer period if the engine requires it.

## Deploy

To add sidecars to an existing Deployment, place the files alongside your engine manifest
and use this `kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - your-vllm-deployment.yaml
  - tap-config.yaml
  - network-policy.yaml
patches:
  - path: tap-sidecar.yaml
    target:
      kind: Deployment
      name: vllm
```

Change the target name to match your Deployment. Review the rendered configuration before
applying it. Apply the network policy first so the publisher is isolated from startup:

```sh
kubectl kustomize .
kubectl apply -f network-policy.yaml
kubectl apply -k .
kubectl rollout status deployment/vllm
kubectl logs deployment/vllm -c tap
kubectl logs deployment/vllm -c shipper
```

To deploy the supplied example instead, edit its model, images, configuration, and Secret
provisioning, then run the same commands from `packaging/kubernetes/`. The example includes
an engine Deployment; do not apply it alongside your existing engine unintentionally.

## Verify network isolation

Other policies can reopen the event ports: Kubernetes combines all policies selecting a
pod. Check connectivity after installation and after namespace policy changes.

From another pod, both event ports must be unreachable. This probe includes the security
settings required by restricted Pod Security:

```sh
INFERTAP_POD_IP=$(kubectl get pod -l app=vllm -o jsonpath='{.items[0].status.podIP}')
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: infertap-isolation-check
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: probe
      image: busybox:1.36
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: [ALL]
      command: [sh, -c]
      args:
        - 'for port in 5557 5657; do if nc -z -w 3 "$INFERTAP_POD_IP" "\$port"; then echo "OPEN: \$port"; exit 1; fi; done; echo closed'
EOF
kubectl logs -f pod/infertap-isolation-check --pod-running-timeout=60s
kubectl delete pod infertap-isolation-check
```

The output must be `closed`. Confirm that both ports are reachable inside the engine pod:

```sh
kubectl exec deployment/vllm -c vllm -- python3 -c \
  'import socket; [socket.create_connection(("127.0.0.1", port), timeout=3).close() for port in (5557, 5657)]'
```

Adjust the selector, container name, and ports for your deployment. Check from another
engine pod as well as an unrelated pod. Finally, send inference traffic and
[verify event collection and shipping](../../docs/onboarding.md#verify-collection).

## Data parallelism

Each rank publishes on the base event and replay ports plus its global rank. Set
`dp_local`, `dp_max`, and `dp_start_rank` in each pod's tap configuration to match the
engine. For two pods with one rank each, use `dp_local = 1`, `dp_max = 1`, and starting
ranks 0 and 1. The second pod uses ports 5558 and 5658. Keep each tap on local loopback
and verify isolation for every rank's ports.

Allow inter-pod engine coordination only on the required ports. An allow-all rule between
engine pods also exposes their event streams. See
[Data parallelism](../../docs/install.md#data-parallelism) for endpoint configuration.

The engine's deployment controller manages group recovery. For coordinated ranks, restart
the required group and verify that each rank serves requests before restoring traffic.
The tap does not restart engine ranks.

## Operation

The instance identifier is `<namespace>/<pod>`, prefixed by `<cluster>/` when configured.
A tap restart starts a new incarnation. An engine publisher restart produces a
`holder_reset`. Pod UID in provenance distinguishes replacement pods that reuse a name.

The shared `emptyDir` survives container restarts but is deleted with the pod. The shipper
attempts a final upload during graceful termination; forced deletion or a shipping outage
can lose unshipped segments. Retention limits can also discard old segments during an
outage. Monitor shipper failures and the tap's loss counters.

The tap's liveness probe checks local output freshness and restarts only the tap container.
Engine readiness does not depend on the tap. Sidecar requests equal their limits, preserving
Guaranteed QoS when all engine containers also meet that class's requirements.
