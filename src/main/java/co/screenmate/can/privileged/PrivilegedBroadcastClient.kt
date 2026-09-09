package co.screenmate.can.privileged

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.Build
import android.os.Handler
import android.os.HandlerThread
import android.os.SystemClock
import co.screenmate.can.ipc.BatchCodec
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import java.util.concurrent.ConcurrentHashMap

/**
 * Non-privileged consumer of the injected agent's vendor-signal broadcasts. This replaces the
 * LocalSocket client, which SELinux blocks across untrusted_app->platform_app (see the privileged
 * SDK's docs/INJECTION.md). Register once; read latest values on demand and observe [ticks].
 *
 * The consumer app MUST declare + hold [PERM_RECEIVE] (the agent gates the broadcast with it).
 *
 * Reliability + integrity, for consumers driving a live dashboard:
 *   - Batches are decoded on a private [HandlerThread], never the main thread, so UI jank cannot
 *     delay or drop a delivery.
 *   - Each v2 batch carries an HMAC trailer and the producer's monotonic send time. With
 *     [requireAuth] on (default), a batch is dropped unless its MAC verifies AND its age (from the
 *     shared device boot clock, immune to wall-clock jumps) is within [staleAfterMs] — so a hostile
 *     app can neither forge fresh values nor replay a captured batch into the dashboard.
 *   - The agent heartbeats every period, so a gap in [ticks] means the producer actually stopped.
 *     [stale] flips true when no accepted batch has arrived within [staleAfterMs] and [connected]
 *     follows it. [carDown]/[noData] expose the agent's own reason a batch carried nothing.
 *   - Per-signal freshness: [signalAgeMs]/[isSignalFresh] report how long ago each field last
 *     appeared, so a dashboard can blank an individually-frozen value even while the feed is live.
 *   - Back-channel: while started, the client periodically broadcasts a keepalive to the agent. This
 *     announces presence (so the agent can run at full rate only when a consumer is listening, and
 *     back off otherwise) and, if [requestedSignals] is set, narrows the agent's broadcast to just
 *     those props. It is best-effort and fail-safe: if the reverse direction is blocked, the agent
 *     simply keeps broadcasting its default set — the feed never depends on the back-channel.
 */
class PrivilegedBroadcastClient(
    private val context: Context,
    private val staleAfterMs: Long = 3_000,
    private val requireAuth: Boolean = true,
    /** Prop ids this consumer wants; null/empty = the agent's default set. */
    private val requestedSignals: IntArray? = null,
    private val requestIntervalMs: Long = 2_000,
    /**
     * Fast producer recovery. If the feed stays silent this long, nudge the injector to relaunch the
     * dead host process instead of waiting on its ~15-min self-heal job. null disables the nudge.
     */
    private val injectorPackage: String? = "co.screenmate.can.injector",
    private val reinjectAfterMs: Long = 5_000,
    private val reinjectIntervalMs: Long = 10_000,
) {

    companion object {
        const val ACTION_SIGNALS = "co.screenmate.can.agent.SIGNALS"
        const val ACTION_REQUEST = "co.screenmate.can.agent.REQUEST"
        const val EXTRA_BATCH = "batch"
        const val EXTRA_REQ = "req"
        const val PERM_RECEIVE = "co.screenmate.can.permission.SIGNALS"
        const val ACTION_REINJECT = "co.screenmate.can.injector.REINJECT"
        private const val FLAG_NO_DATA = 0x1
        private const val FLAG_CAR_DOWN = 0x2
        private val AUTH_KEY = "SMCAN-broadcast-hmac-v1".toByteArray(Charsets.UTF_8)
    }

    private val clientId: Long = kotlin.random.Random.nextLong()

    // Re-inject bookkeeping (elapsedRealtime): when the client started, and the last nudge sent.
    @Volatile private var startedAtMs: Long = 0
    @Volatile private var lastReinjectAtMs: Long = 0

    private val ints = ConcurrentHashMap<Int, Int>()
    private val floats = ConcurrentHashMap<Int, Float>()
    // propId -> device-boot time (elapsedRealtime) of the batch it last appeared in.
    private val seenAt = ConcurrentHashMap<Int, Long>()

    private val _ticks = MutableStateFlow(0L)
    /** Bumped on every accepted batch; observe to recompute derived state. Starts at 0. */
    val ticks: StateFlow<Long> = _ticks

    private val _stale = MutableStateFlow(true)
    /** True until the first batch, and again whenever none has arrived within [staleAfterMs]. */
    val stale: StateFlow<Boolean> = _stale

    /** True while batches are arriving on time. Inverse of [stale]; kept for read-at-a-glance. */
    @Volatile var connected: Boolean = false
        private set

    /** Last batch's monotonic sequence from the agent (v2+); -1 before any v2 batch. */
    @Volatile var lastSeq: Long = -1
        private set
    /** elapsedRealtime() of the last accepted batch; 0 before any. */
    @Volatile var lastBatchAtMs: Long = 0
        private set
    /** Agent reported it could read no signals this batch (car asleep/parked, or service down). */
    @Volatile var noData: Boolean = false
        private set
    /** Agent reported the vehicle service itself is unhealthy (it is reconnecting). */
    @Volatile var carDown: Boolean = false
        private set
    /** Count of batches dropped for a bad/absent MAC — a spoof attempt or a version/key mismatch. */
    @Volatile var rejectedAuth: Long = 0
        private set
    /** How many recovery nudges we've broadcast to the injector (feed silence -> re-inject). */
    @Volatile var reinjectRequests: Long = 0
        private set
    /** Producer's cumulative Car-service rebuilds since it started (v3+); 0 before any v3 batch. */
    @Volatile var reconnectCount: Int = 0
        private set
    /** Producer's worst per-signal read latency in the last batch, µs (v3+). */
    @Volatile var maxReadLatencyMicros: Int = 0
        private set

    private val thread = HandlerThread("smcan-bcast-client").apply { start() }
    private val handler = Handler(thread.looper)

    private val staleCheck = object : Runnable {
        override fun run() {
            val age = SystemClock.elapsedRealtime() - lastBatchAtMs
            if (lastBatchAtMs == 0L || age >= staleAfterMs) {
                markStale()
                maybeReinject()
            }
            handler.postDelayed(this, staleAfterMs / 2)
        }
    }

    private val receiver = object : BroadcastReceiver() {
        override fun onReceive(c: Context?, i: Intent?) {
            val batch = i?.getByteArrayExtra(EXTRA_BATCH) ?: return
            decode(batch)
        }
    }

    private val requestTick = object : Runnable {
        override fun run() {
            sendRequest()
            handler.postDelayed(this, requestIntervalMs)
        }
    }

    fun start() {
        startedAtMs = SystemClock.elapsedRealtime()
        registerOnHandler()
        handler.postDelayed(staleCheck, staleAfterMs / 2)
        handler.post(requestTick)
    }

    fun stop() {
        handler.removeCallbacks(staleCheck)
        handler.removeCallbacks(requestTick)
        runCatching { context.unregisterReceiver(receiver) }
        // Terminal: release the HandlerThread + its Looper. Without this, every start/stop cycle
        // (e.g. the hosting foreground service being recreated) leaked a native thread. A stopped
        // client is not restartable — hosts create a fresh instance, which they already do.
        thread.quitSafely()
    }

    /**
     * When the feed has been silent past [reinjectAfterMs], broadcast a rate-limited nudge to the
     * injector so it relaunches the dead producer host now instead of on its ~15-min job floor. The
     * broadcast is explicit (targeted at [injectorPackage]) and best-effort: if the injector is
     * absent or the send is blocked, nothing happens and the normal self-heal still applies.
     */
    private fun maybeReinject() {
        val pkg = injectorPackage ?: return
        val now = SystemClock.elapsedRealtime()
        // Silence measured from the last accepted batch, or from start if none ever arrived.
        val silentFor = now - maxOf(lastBatchAtMs, startedAtMs)
        if (silentFor < reinjectAfterMs) return
        if (now - lastReinjectAtMs < reinjectIntervalMs) return
        lastReinjectAtMs = now
        reinjectRequests++
        runCatching { context.sendBroadcast(Intent(ACTION_REINJECT).setPackage(pkg)) }
    }

    private fun sendRequest() {
        runCatching {
            val ids = requestedSignals?.toList() ?: emptyList()
            val framed = BatchCodec.frameRequest(clientId, ids, AUTH_KEY)
            // Runtime-registered agent receiver -> implicit action is fine. The agent gates senders
            // by requiring PERM_RECEIVE (which we hold), so random apps can't steer it.
            context.sendBroadcast(Intent(ACTION_REQUEST).putExtra(EXTRA_REQ, framed))
        }
    }

    fun latestInt(propId: Int): Int? = ints[propId]
    fun latestFloat(propId: Int): Float? = floats[propId]

    /** Milliseconds since [propId] last appeared in an accepted batch, or null if never seen. */
    fun signalAgeMs(propId: Int): Long? = seenAt[propId]?.let { SystemClock.elapsedRealtime() - it }

    /** True if [propId] appeared within [maxAgeMs]; a dashboard should blank the field otherwise. */
    fun isSignalFresh(propId: Int, maxAgeMs: Long = staleAfterMs): Boolean =
        signalAgeMs(propId)?.let { it <= maxAgeMs } ?: false

    private fun registerOnHandler() {
        val filter = IntentFilter(ACTION_SIGNALS)
        // Deliver on our HandlerThread, never the main thread.
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            context.registerReceiver(receiver, filter, null, handler, Context.RECEIVER_EXPORTED)
        } else {
            @Suppress("UnspecifiedRegisterReceiverFlag")
            context.registerReceiver(receiver, filter, null, handler)
        }
    }

    private fun markStale() {
        connected = false
        _stale.value = true
    }

    private fun decode(rawBatch: ByteArray) {
        // Parse + verify off in the pure codec; a null means bad MAC / unknown version / malformed.
        val d = BatchCodec.deframe(rawBatch, AUTH_KEY, requireAuth) ?: run { rejectedAuth++; return }
        val nowElapsed = SystemClock.elapsedRealtime()
        // v1 has no producer timestamp; treat receive time as the send time.
        val batchElapsed = if (d.version >= 2) d.tsElapsed else nowElapsed
        if (d.version >= 2) {
            // Reject a stale/replayed batch: its producer timestamp is outside our window.
            val age = nowElapsed - batchElapsed
            if (age < -staleAfterMs || age > staleAfterMs) { rejectedAuth++; return }
        }

        lastSeq = if (d.version >= 2) d.seq else -1
        noData = if (d.version >= 2) d.flags and FLAG_NO_DATA != 0 else d.records.isEmpty()
        carDown = d.version >= 2 && d.flags and FLAG_CAR_DOWN != 0
        reconnectCount = d.reconnectCount
        maxReadLatencyMicros = d.maxReadMicros
        d.records.forEach { r ->
            if (r.kind == BatchCodec.K_FLOAT) floats[r.id] = Float.fromBits(r.bits) else ints[r.id] = r.bits
            seenAt[r.id] = batchElapsed
        }

        lastBatchAtMs = nowElapsed
        connected = true
        _stale.value = false
        _ticks.value = _ticks.value + 1
    }
}
