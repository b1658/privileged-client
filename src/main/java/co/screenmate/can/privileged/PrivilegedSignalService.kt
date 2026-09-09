package co.screenmate.can.privileged

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Binder
import android.os.Build
import android.os.IBinder

/**
 * Foreground host for [PrivilegedBroadcastClient]. A dashboard should run the client here rather
 * than in an Activity: a runtime-registered receiver only delivers while its host is alive, so an
 * Activity-hosted client stops receiving the moment the app is backgrounded (a common cause of the
 * feed "going red" that has nothing to do with the producer). A foreground service keeps the
 * process resident and the receiver registered.
 *
 * Usage:
 * ```
 * PrivilegedSignalService.start(context)
 * val client = PrivilegedSignalService.client   // observe client.ticks / client.stale, read values
 * ...
 * PrivilegedSignalService.stop(context)
 * ```
 * The consumer app still must hold [PrivilegedBroadcastClient.PERM_RECEIVE]. On Android 13+ it
 * should also hold POST_NOTIFICATIONS for the ongoing notification to be visible.
 */
class PrivilegedSignalService : Service() {

    private var _client: PrivilegedBroadcastClient? = null
    private val binder = LocalBinder()

    inner class LocalBinder : Binder() {
        val service: PrivilegedSignalService get() = this@PrivilegedSignalService
    }

    override fun onCreate() {
        super.onCreate()
        val c = PrivilegedBroadcastClient(applicationContext)
        _client = c
        live = c
        c.start()
        startForegroundCompat()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int = START_STICKY

    override fun onBind(intent: Intent?): IBinder = binder

    override fun onDestroy() {
        _client?.stop()
        _client = null
        live = null
        super.onDestroy()
    }

    private fun startForegroundCompat() {
        val nm = getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            nm?.createNotificationChannel(
                NotificationChannel(CHANNEL, "Vehicle signals", NotificationManager.IMPORTANCE_LOW)
                    .apply { setShowBadge(false) }
            )
        }
        val notif: Notification = Notification.Builder(this, CHANNEL)
            .setContentTitle("Vehicle signals")
            .setContentText("Receiving CAN signals")
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setOngoing(true)
            .build()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            startForeground(NOTIF_ID, notif, ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE)
        } else {
            startForeground(NOTIF_ID, notif)
        }
    }

    companion object {
        private const val CHANNEL = "smcan-signals"
        private const val NOTIF_ID = 0x5C11

        /** The client owned by the running service, or null when the service is not running. */
        @Volatile
        var live: PrivilegedBroadcastClient? = null
            private set

        /** Convenience alias for [live]. */
        val client: PrivilegedBroadcastClient? get() = live

        fun start(context: Context) {
            val i = Intent(context, PrivilegedSignalService::class.java)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) context.startForegroundService(i)
            else context.startService(i)
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, PrivilegedSignalService::class.java))
        }
    }
}
