# privileged-client

Non-privileged Android SDK for reading decoded vehicle CAN signals that are otherwise reachable
only through a privileged in-car agent. It is the *consumer* half of a producer/consumer split: a
privileged agent injected into the stock app reads the vendor VHAL (`CAR_VENDOR_EXTENSION`) and
broadcasts decoded signals; any ordinary app links this library to receive them.

## What it provides

- **`PrivilegedBroadcastClient`** — registers for the agent's signal broadcasts and exposes the
  latest values plus liveness state. Broadcasts are used instead of a socket because SELinux blocks
  a `LocalSocket` across `untrusted_app -> platform_app`.
  - Batches are decoded off the main thread on a private `HandlerThread`, so UI jank can't drop a
    delivery.
  - Integrity + anti-replay: each batch carries an HMAC trailer and the producer's monotonic
    boot-clock send time. With `requireAuth` on (default), a batch is dropped unless the MAC
    verifies **and** its age is within `staleAfterMs` — a hostile app can neither forge nor replay
    values.
  - Liveness: the agent heartbeats every period, so a gap in `ticks` means the producer actually
    stopped. `stale` / `connected` / `carDown` / `noData` expose why. Per-signal freshness
    (`signalAgeMs` / `isSignalFresh`) lets a dashboard blank an individually-frozen field.
  - Back-channel keepalive announces consumer presence (so the agent can run full-rate only when
    someone is listening) and can narrow the broadcast to `requestedSignals`. Best-effort and
    fail-safe — the feed never depends on it.
- **`PrivilegedSignalService`** — a `connectedDevice` foreground service that hosts the client so
  reception survives the app being backgrounded (a runtime-registered receiver only delivers while
  its host process is alive).
- **`VendorSignals`** — generated catalog of 1153 decoded signals (name → vendor VHAL property id +
  value kind), packed as data to stay under JVM method limits.

## Requirements

- `minSdk` 29, `compileSdk` 34, Java/Kotlin 17. Android library module (`com.android.library`).
- The consumer app must declare **and hold** the receive permission the agent gates its broadcasts
  with; on Android 13+ it should also hold `POST_NOTIFICATIONS` for the service notification.
- The `connectedDevice` FGS type is satisfied via the install-time `CHANGE_NETWORK_STATE` companion
  permission (no runtime prompt).

## Usage

```kotlin
PrivilegedSignalService.start(context)
val client = PrivilegedSignalService.client   // observe client.ticks / client.stale; read values
// ...
PrivilegedSignalService.stop(context)
```

## Note

Extracted from a larger monorepo. `build.gradle.kts` references a sibling `:common` module (the
`Kind` value-kind tags on the public `VendorSignals` API); wire that dependency to build standalone.
