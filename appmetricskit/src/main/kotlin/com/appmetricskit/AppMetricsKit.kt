package com.appmetricskit

import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import android.util.Log
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.security.MessageDigest
import java.util.Locale
import java.util.TimeZone
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * Configuration for AppMetricsKit event ingestion and local queue behavior.
 */
public data class AppMetricsConfig @JvmOverloads constructor(
    public val ingestUrl: String,
    public val ingestKey: String,
    public val testMode: Boolean = false,
    public val batchSize: Int = 25,
    public val flushIntervalMillis: Long = 30_000L,
    public val maxQueueSize: Int = 10_000,
    public val allowedPayloadKeys: Set<String>? = null,
    public val blockedPayloadKeys: Set<String> = defaultBlockedPayloadKeys,
    public val automaticAppLaunchTracking: Boolean = true,
) {
    internal fun normalizedBatchSize(): Int = batchSize.coerceIn(1, MAX_BACKEND_BATCH_SIZE)
    internal fun normalizedMaxQueueSize(): Int = maxQueueSize.coerceAtLeast(1)
    internal fun normalizedFlushIntervalMillis(): Long = flushIntervalMillis.coerceAtLeast(0L)

    public companion object {
        public const val MAX_BACKEND_BATCH_SIZE: Int = 500

        @JvmField
        public val defaultBlockedPayloadKeys: Set<String> = setOf(
            "adid",
            "advertisingid",
            "advertisingidentifier",
            "address",
            "deviceid",
            "devicetoken",
            "email",
            "firstname",
            "idfa",
            "ip",
            "ipaddress",
            "lastname",
            "latitude",
            "location",
            "longitude",
            "name",
            "phone",
        )
    }
}

/**
 * Summary returned after a manual flush attempt.
 */
public data class AppMetricsFlushResult(
    public val attempted: Int,
    public val delivered: Int,
    public val dropped: Int,
    public val willRetry: Boolean,
    public val statusCode: Int?,
) {
    public companion object {
        @JvmField
        public val empty: AppMetricsFlushResult = AppMetricsFlushResult(
            attempted = 0,
            delivered = 0,
            dropped = 0,
            willRetry = false,
            statusCode = null,
        )
    }
}

/**
 * Common first-party event names that power AppMetricsKit dashboards.
 */
public object AppMetricsEvent {
    public const val appLaunch: String = "App.launch"
    public const val onboardingStarted: String = "Onboarding.started"
    public const val onboardingStepViewed: String = "Onboarding.stepViewed"
    public const val onboardingCompleted: String = "Onboarding.completed"
    public const val paywallViewed: String = "Paywall.viewed"
    public const val paywallCTATapped: String = "Paywall.ctaTapped"
    public const val purchaseStarted: String = "Purchase.started"
    public const val purchaseCompleted: String = "Purchase.completed"
    public const val purchaseFailed: String = "Purchase.failed"
    public const val subscriptionTrialStarted: String = "Subscription.trialStarted"
    public const val subscriptionTrialConverted: String = "Subscription.trialConverted"
    public const val subscriptionRenewed: String = "Subscription.renewed"
    public const val subscriptionCancelled: String = "Subscription.cancelled"
    public const val subscriptionRefunded: String = "Subscription.refunded"
    public const val subscriptionBillingRetry: String = "Subscription.billingRetry"
    public const val subscriptionGracePeriod: String = "Subscription.gracePeriod"
    public const val featureUsed: String = "Feature.used"
    public const val errorOccurred: String = "Error.occurred"
}

/**
 * Static facade for standard Android app integrations.
 */
public object AppMetricsKit {
    private val shared = AppMetricsClient()

    @JvmStatic
    public val pendingEventCount: Int
        get() = shared.pendingEventCount

    @JvmStatic
    public fun configure(context: Context, config: AppMetricsConfig) {
        shared.configure(context, config)
    }

    @JvmStatic
    public fun identify(userId: String?) {
        shared.identify(userId)
    }

    @JvmStatic
    public fun resetIdentity() {
        shared.resetIdentity()
    }

    @JvmStatic
    public fun setCollectionEnabled(enabled: Boolean) {
        shared.setCollectionEnabled(enabled)
    }

    @JvmOverloads
    @JvmStatic
    public fun track(
        eventName: String,
        payload: Map<String, Any?> = emptyMap(),
        floatValue: Double? = null,
    ): String? = shared.track(eventName, payload, floatValue)

    @JvmStatic
    public fun trackAppLaunch(): String? = shared.trackAppLaunch()

    @JvmStatic
    public fun trackOnboardingStarted(): String? = shared.trackOnboardingStarted()

    @JvmStatic
    public fun trackOnboardingCompleted(): String? = shared.trackOnboardingCompleted()

    @JvmStatic
    public fun trackPaywallViewed(plan: String? = null): String? = shared.trackPaywallViewed(plan)

    @JvmStatic
    public fun trackPurchaseCompleted(plan: String? = null, amount: Double? = null): String? =
        shared.trackPurchaseCompleted(plan, amount)

    @JvmStatic
    public fun trackError(name: String, message: String? = null): String? = shared.trackError(name, message)

    @JvmStatic
    public fun flush(): Future<AppMetricsFlushResult> = shared.flush()

    @JvmStatic
    public fun flushBlocking(): AppMetricsFlushResult = shared.flushBlocking()
}

/**
 * Instance-based client for advanced integrations and tests.
 */
public class AppMetricsClient internal constructor(
    private val transport: AppMetricsTransport,
    private val idGenerator: () -> String,
    private val clockMillis: () -> Long,
    private val sleeper: (Long) -> Unit,
) {
    public constructor() : this(
        transport = HttpUrlConnectionTransport(),
        idGenerator = { UUID.randomUUID().toString() },
        clockMillis = { System.currentTimeMillis() },
        sleeper = { Thread.sleep(it) },
    )

    private val lock = Any()
    private val flushExecutor = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "AppMetricsKitFlush").apply { isDaemon = true }
    }
    private val scheduler = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "AppMetricsKitTimer").apply { isDaemon = true }
    }

    private var config: AppMetricsConfig? = null
    private var metadataProvider: AppMetricsMetadataProvider = EmptyMetadataProvider
    private var queueStore: AppMetricsQueueStore = InMemoryQueueStore()
    private var queue: MutableList<AppMetricsQueuedEvent> = mutableListOf()
    private var anonymousUserId: String? = null
    private var sessionId: String = idGenerator()
    private var isCollectionEnabled: Boolean = true
    private var isFlushing: Boolean = false
    private var scheduledFlush: ScheduledFuture<*>? = null

    public val pendingEventCount: Int
        get() = synchronized(lock) { queue.size }

    public fun configure(context: Context, config: AppMetricsConfig) {
        val appContext = context.applicationContext
        val queueFile = File(File(appContext.filesDir, "appmetricskit"), QUEUE_FILE_NAME)
        configureInternal(
            config = config,
            metadataProvider = AndroidMetadataProvider(appContext),
            queueStore = FileQueueStore(queueFile),
        )
    }

    internal fun configureForTests(
        config: AppMetricsConfig,
        metadataProvider: AppMetricsMetadataProvider,
        queueStore: AppMetricsQueueStore,
    ) {
        configureInternal(config, metadataProvider, queueStore)
    }

    private fun configureInternal(
        config: AppMetricsConfig,
        metadataProvider: AppMetricsMetadataProvider,
        queueStore: AppMetricsQueueStore,
    ) {
        synchronized(lock) {
            this.config = config
            this.metadataProvider = metadataProvider
            this.queueStore = queueStore
            this.queue = queueStore.load().toMutableList()
            this.isCollectionEnabled = true
            this.sessionId = idGenerator()
            scheduleFlushLocked(config)
        }

        if (config.automaticAppLaunchTracking) {
            trackAppLaunch()
        }
    }

    public fun identify(userId: String?) {
        synchronized(lock) {
            anonymousUserId = userId
                ?.takeIf { it.isNotBlank() }
                ?.let(::sha256)
        }
    }

    public fun resetIdentity() {
        synchronized(lock) {
            anonymousUserId = null
            sessionId = idGenerator()
        }
    }

    public fun setCollectionEnabled(enabled: Boolean) {
        synchronized(lock) {
            isCollectionEnabled = enabled
        }
    }

    @JvmOverloads
    public fun track(
        eventName: String,
        payload: Map<String, Any?> = emptyMap(),
        floatValue: Double? = null,
    ): String? {
        val eventId = idGenerator()
        var shouldFlush = false

        synchronized(lock) {
            val currentConfig = config
            if (currentConfig == null) {
                debugWarn("Call configure(context, config) before tracking events.")
                return null
            }

            if (!isCollectionEnabled) {
                return null
            }

            if (!isValidEventName(eventName)) {
                debugWarn("Dropped event with invalid name '$eventName'. Use Namespace.action, for example Paywall.viewed.")
                return null
            }

            if (floatValue != null && !floatValue.isFinite()) {
                debugWarn("Dropped event '$eventName' because floatValue is not finite.")
                return null
            }

            val metadata = metadataProvider.metadata()
            val scrubbedPayload = scrubPayload(payload, currentConfig)
            val event = AppMetricsQueuedEvent(
                eventName = eventName,
                anonymousUserId = anonymousUserId,
                sessionId = sessionId,
                eventTime = clockMillis(),
                isTestMode = currentConfig.testMode,
                platform = "android",
                appVersion = metadata.appVersion,
                buildNumber = metadata.buildNumber,
                osVersion = metadata.osVersion,
                deviceModel = metadata.deviceModel,
                locale = metadata.locale,
                timezone = metadata.timezone,
                floatValue = floatValue,
                eventId = eventId,
                payload = scrubbedPayload.ifEmpty { null },
            )

            queue.add(event)
            trimQueueIfNeededLocked(currentConfig.normalizedMaxQueueSize())
            persistQueueLocked()
            shouldFlush = queue.size >= currentConfig.normalizedBatchSize()
        }

        if (shouldFlush) {
            flush()
        }

        return eventId
    }

    public fun trackAppLaunch(): String? = track(AppMetricsEvent.appLaunch)

    public fun trackOnboardingStarted(): String? = track(AppMetricsEvent.onboardingStarted)

    public fun trackOnboardingCompleted(): String? = track(AppMetricsEvent.onboardingCompleted)

    public fun trackPaywallViewed(plan: String? = null): String? {
        val payload = plan?.let { mapOf("plan" to it) } ?: emptyMap()
        return track(AppMetricsEvent.paywallViewed, payload)
    }

    public fun trackPurchaseCompleted(plan: String? = null, amount: Double? = null): String? {
        val payload = plan?.let { mapOf("plan" to it) } ?: emptyMap()
        return track(AppMetricsEvent.purchaseCompleted, payload, amount)
    }

    public fun trackError(name: String, message: String? = null): String? {
        val payload = buildMap<String, Any?> {
            put("errorName", name)
            if (message != null) {
                put("message", message)
            }
        }
        return track(AppMetricsEvent.errorOccurred, payload)
    }

    public fun flush(): Future<AppMetricsFlushResult> =
        flushExecutor.submit<AppMetricsFlushResult> { flushBlocking() }

    public fun flushBlocking(): AppMetricsFlushResult {
        val prepared = prepareFlushBatch() ?: return AppMetricsFlushResult.empty
        val (currentConfig, batch) = prepared
        val attempted = batch.size
        val body = AppMetricsIngestEnvelope(batch).toJson().toString()
        var lastStatusCode: Int? = null

        try {
            for (attempt in 0 until MAX_FLUSH_ATTEMPTS) {
                try {
                    val response = transport.post(
                        url = currentConfig.ingestUrl,
                        ingestKey = currentConfig.ingestKey,
                        body = body,
                    )
                    lastStatusCode = response.statusCode

                    if (response.statusCode in 200..299) {
                        removeBatch(batch)
                        return AppMetricsFlushResult(
                            attempted = attempted,
                            delivered = attempted,
                            dropped = 0,
                            willRetry = false,
                            statusCode = response.statusCode,
                        )
                    }

                    if (!isRetryableStatus(response.statusCode)) {
                        removeBatch(batch)
                        debugWarn("Dropped $attempted event(s) after non-retryable ingest status ${response.statusCode}.")
                        return AppMetricsFlushResult(
                            attempted = attempted,
                            delivered = 0,
                            dropped = attempted,
                            willRetry = false,
                            statusCode = response.statusCode,
                        )
                    }
                } catch (error: IOException) {
                    debugWarn("Flush attempt ${attempt + 1} failed: ${error.message ?: "network error"}")
                }

                if (attempt < MAX_FLUSH_ATTEMPTS - 1) {
                    sleeper(RETRY_BACKOFF_MILLIS[attempt])
                }
            }

            return AppMetricsFlushResult(
                attempted = attempted,
                delivered = 0,
                dropped = 0,
                willRetry = true,
                statusCode = lastStatusCode,
            )
        } finally {
            synchronized(lock) {
                isFlushing = false
            }
        }
    }

    private fun prepareFlushBatch(): Pair<AppMetricsConfig, List<AppMetricsQueuedEvent>>? =
        synchronized(lock) {
            val currentConfig = config ?: return@synchronized null
            if (isFlushing || queue.isEmpty()) {
                return@synchronized null
            }

            isFlushing = true
            val batch = queue.take(currentConfig.normalizedBatchSize())
            currentConfig to batch
        }

    private fun removeBatch(batch: List<AppMetricsQueuedEvent>) {
        val ids = batch.mapTo(mutableSetOf()) { it.eventId }
        synchronized(lock) {
            queue.removeAll { ids.contains(it.eventId) }
            persistQueueLocked()
        }
    }

    private fun scheduleFlushLocked(config: AppMetricsConfig) {
        scheduledFlush?.cancel(false)
        scheduledFlush = null

        val interval = config.normalizedFlushIntervalMillis()
        if (interval == 0L) {
            return
        }

        scheduledFlush = scheduler.scheduleWithFixedDelay(
            { flushBlocking() },
            interval,
            interval,
            TimeUnit.MILLISECONDS,
        )
    }

    private fun trimQueueIfNeededLocked(maxQueueSize: Int) {
        if (queue.size <= maxQueueSize) {
            return
        }

        val dropCount = queue.size - maxQueueSize
        repeat(dropCount) {
            queue.removeAt(0)
        }
        debugWarn("Offline queue exceeded $maxQueueSize events. Oldest events were dropped.")
    }

    private fun persistQueueLocked() {
        queueStore.save(queue)
    }

    private fun scrubPayload(payload: Map<String, Any?>, config: AppMetricsConfig): Map<String, Any> {
        val blocked = config.blockedPayloadKeys.mapTo(mutableSetOf()) { it.lowercase(Locale.US) }
        val allowed = config.allowedPayloadKeys?.mapTo(mutableSetOf()) { it.lowercase(Locale.US) }
        val output = linkedMapOf<String, Any>()

        for ((key, rawValue) in payload) {
            if (output.size >= MAX_PAYLOAD_KEYS) {
                debugWarn("Dropped payload key '$key' because payloads are limited to $MAX_PAYLOAD_KEYS keys.")
                continue
            }

            if (key.length > MAX_PAYLOAD_KEY_LENGTH) {
                debugWarn("Dropped payload key '$key' because it exceeds $MAX_PAYLOAD_KEY_LENGTH characters.")
                continue
            }

            val normalizedKey = key.lowercase(Locale.US)
            if (blocked.contains(normalizedKey)) {
                debugWarn("Dropped blocked payload key '$key'.")
                continue
            }

            if (allowed != null && !allowed.contains(normalizedKey)) {
                continue
            }

            when (rawValue) {
                null -> Unit
                is String -> {
                    if (looksLikePII(rawValue)) {
                        debugWarn("Dropped payload key '$key' because its value looks like personal data.")
                    } else {
                        output[key] = rawValue.take(MAX_PAYLOAD_VALUE_LENGTH)
                    }
                }
                is Boolean -> output[key] = rawValue
                is Float -> {
                    if (rawValue.isFinite()) {
                        output[key] = rawValue.toDouble()
                    } else {
                        debugWarn("Dropped payload key '$key' because its number is not finite.")
                    }
                }
                is Double -> {
                    if (rawValue.isFinite()) {
                        output[key] = rawValue
                    } else {
                        debugWarn("Dropped payload key '$key' because its number is not finite.")
                    }
                }
                is Number -> {
                    if (rawValue.toDouble().isFinite()) {
                        output[key] = rawValue
                    } else {
                        debugWarn("Dropped payload key '$key' because its number is not finite.")
                    }
                }
                else -> debugWarn("Dropped payload key '$key' because payload values must be strings, numbers, or booleans.")
            }
        }

        return output
    }

    internal companion object {
        private const val QUEUE_FILE_NAME = "appmetricskit-queue-v1.json"
        private const val MAX_FLUSH_ATTEMPTS = 3
        private const val MAX_PAYLOAD_KEYS = 64
        private const val MAX_PAYLOAD_KEY_LENGTH = 64
        private const val MAX_PAYLOAD_VALUE_LENGTH = 1_024
        private const val MAX_EVENT_NAME_LENGTH = 80
        private val RETRY_BACKOFF_MILLIS = longArrayOf(500L, 1_000L)
        private val EVENT_NAME_REGEX = Regex("^[A-Za-z][A-Za-z0-9]*\\.[A-Za-z][A-Za-z0-9]*$")
        private val EMAIL_REGEX = Regex("[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}", RegexOption.IGNORE_CASE)
        private val PHONE_REGEX = Regex("^\\+?[0-9][0-9 .()\\-]{7,}[0-9]$")
        private val CREDIT_CARD_REGEX = Regex("^(?:\\d[ -]*?){13,19}$")

        internal fun sha256(value: String): String {
            val bytes = MessageDigest.getInstance("SHA-256")
                .digest(value.toByteArray(Charsets.UTF_8))
            return bytes.joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }
        }

        private fun isValidEventName(eventName: String): Boolean =
            eventName.isNotEmpty() &&
                eventName.length <= MAX_EVENT_NAME_LENGTH &&
                EVENT_NAME_REGEX.matches(eventName)

        private fun looksLikePII(value: String): Boolean =
            EMAIL_REGEX.containsMatchIn(value) ||
                PHONE_REGEX.matches(value) ||
                CREDIT_CARD_REGEX.matches(value)

        private fun isRetryableStatus(statusCode: Int): Boolean =
            statusCode == 408 || statusCode == 429 || statusCode >= 500

        private fun debugWarn(message: String) {
            if (BuildConfig.DEBUG) {
                Log.w("AppMetricsKit", message)
            }
        }
    }
}

internal data class AppMetricsIngestEnvelope(
    val events: List<AppMetricsQueuedEvent>,
) {
    fun toJson(): JSONObject {
        val array = JSONArray()
        events.forEach { array.put(it.toJson()) }
        return JSONObject().put("events", array)
    }
}

internal data class AppMetricsQueuedEvent(
    val eventName: String,
    val anonymousUserId: String?,
    val sessionId: String?,
    val eventTime: Long,
    val isTestMode: Boolean,
    val platform: String,
    val appVersion: String?,
    val buildNumber: String?,
    val osVersion: String?,
    val deviceModel: String?,
    val locale: String?,
    val timezone: String?,
    val floatValue: Double?,
    val eventId: String,
    val payload: Map<String, Any>?,
) {
    fun toJson(): JSONObject {
        val json = JSONObject()
            .put("eventName", eventName)
            .put("eventTime", eventTime)
            .put("isTestMode", isTestMode)
            .put("platform", platform)
            .put("eventId", eventId)

        json.putNullable("anonymousUserId", anonymousUserId)
        json.putNullable("sessionId", sessionId)
        json.putNullable("appVersion", appVersion)
        json.putNullable("buildNumber", buildNumber)
        json.putNullable("osVersion", osVersion)
        json.putNullable("deviceModel", deviceModel)
        json.putNullable("locale", locale)
        json.putNullable("timezone", timezone)
        json.putNullable("floatValue", floatValue)

        if (!payload.isNullOrEmpty()) {
            json.put("payload", JSONObject(payload))
        }

        return json
    }

    companion object {
        fun fromJson(json: JSONObject): AppMetricsQueuedEvent? =
            runCatching {
                AppMetricsQueuedEvent(
                    eventName = json.getString("eventName"),
                    anonymousUserId = json.optNullableString("anonymousUserId"),
                    sessionId = json.optNullableString("sessionId"),
                    eventTime = json.getLong("eventTime"),
                    isTestMode = json.optBoolean("isTestMode", false),
                    platform = json.optString("platform", "android"),
                    appVersion = json.optNullableString("appVersion"),
                    buildNumber = json.optNullableString("buildNumber"),
                    osVersion = json.optNullableString("osVersion"),
                    deviceModel = json.optNullableString("deviceModel"),
                    locale = json.optNullableString("locale"),
                    timezone = json.optNullableString("timezone"),
                    floatValue = json.optNullableDouble("floatValue"),
                    eventId = json.getString("eventId"),
                    payload = json.optPayload(),
                )
            }.getOrNull()
    }
}

internal data class AppMetricsResponse(val statusCode: Int)

internal interface AppMetricsTransport {
    @Throws(IOException::class)
    fun post(url: String, ingestKey: String, body: String): AppMetricsResponse
}

internal class HttpUrlConnectionTransport : AppMetricsTransport {
    @Throws(IOException::class)
    override fun post(url: String, ingestKey: String, body: String): AppMetricsResponse {
        val bodyBytes = body.toByteArray(Charsets.UTF_8)
        val connection = (URL(url).openConnection() as HttpURLConnection).apply {
            requestMethod = "POST"
            connectTimeout = 10_000
            readTimeout = 10_000
            doOutput = true
            setRequestProperty("Authorization", "Bearer $ingestKey")
            setRequestProperty("Content-Type", "application/json")
            setRequestProperty("User-Agent", "AppMetricsKit-Android/0.1.0")
            setRequestProperty("Content-Length", bodyBytes.size.toString())
        }

        return try {
            connection.outputStream.use { stream -> stream.write(bodyBytes) }
            val status = connection.responseCode
            runCatching {
                (connection.errorStream ?: connection.inputStream)?.close()
            }
            AppMetricsResponse(status)
        } finally {
            connection.disconnect()
        }
    }
}

internal data class AppMetricsMetadata(
    val appVersion: String?,
    val buildNumber: String?,
    val osVersion: String?,
    val deviceModel: String?,
    val locale: String?,
    val timezone: String?,
)

internal interface AppMetricsMetadataProvider {
    fun metadata(): AppMetricsMetadata
}

internal object EmptyMetadataProvider : AppMetricsMetadataProvider {
    override fun metadata(): AppMetricsMetadata = AppMetricsMetadata(
        appVersion = null,
        buildNumber = null,
        osVersion = null,
        deviceModel = null,
        locale = Locale.getDefault().toLanguageTag(),
        timezone = TimeZone.getDefault().id,
    )
}

internal class AndroidMetadataProvider(context: Context) : AppMetricsMetadataProvider {
    private val appContext = context.applicationContext

    override fun metadata(): AppMetricsMetadata {
        val packageInfo = runCatching {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                appContext.packageManager.getPackageInfo(
                    appContext.packageName,
                    PackageManager.PackageInfoFlags.of(0),
                )
            } else {
                @Suppress("DEPRECATION")
                appContext.packageManager.getPackageInfo(appContext.packageName, 0)
            }
        }.getOrNull()

        val buildNumber = packageInfo?.let {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                it.longVersionCode.toString()
            } else {
                @Suppress("DEPRECATION")
                it.versionCode.toString()
            }
        }

        return AppMetricsMetadata(
            appVersion = packageInfo?.versionName,
            buildNumber = buildNumber,
            osVersion = "${Build.VERSION.RELEASE} (${Build.VERSION.SDK_INT})",
            deviceModel = listOf(Build.MANUFACTURER, Build.MODEL)
                .filter { it.isNotBlank() }
                .joinToString(" "),
            locale = Locale.getDefault().toLanguageTag(),
            timezone = TimeZone.getDefault().id,
        )
    }
}

internal interface AppMetricsQueueStore {
    fun load(): List<AppMetricsQueuedEvent>
    fun save(events: List<AppMetricsQueuedEvent>)
}

internal class FileQueueStore(private val file: File) : AppMetricsQueueStore {
    override fun load(): List<AppMetricsQueuedEvent> {
        if (!file.exists()) {
            return emptyList()
        }

        return runCatching {
            val array = JSONArray(file.readText(Charsets.UTF_8))
            buildList {
                for (index in 0 until array.length()) {
                    val event = AppMetricsQueuedEvent.fromJson(array.getJSONObject(index))
                    if (event != null) {
                        add(event)
                    }
                }
            }
        }.getOrElse {
            emptyList()
        }
    }

    override fun save(events: List<AppMetricsQueuedEvent>) {
        runCatching {
            file.parentFile?.mkdirs()
            val array = JSONArray()
            events.forEach { array.put(it.toJson()) }
            file.writeText(array.toString(), Charsets.UTF_8)
        }
    }
}

internal class InMemoryQueueStore(
    private val storedEvents: MutableList<AppMetricsQueuedEvent> = mutableListOf(),
) : AppMetricsQueueStore {
    override fun load(): List<AppMetricsQueuedEvent> = storedEvents.toList()

    override fun save(events: List<AppMetricsQueuedEvent>) {
        storedEvents.clear()
        storedEvents.addAll(events)
    }
}

private fun JSONObject.putNullable(key: String, value: Any?) {
    if (value != null) {
        put(key, value)
    }
}

private fun JSONObject.optNullableString(key: String): String? =
    if (has(key) && !isNull(key)) optString(key) else null

private fun JSONObject.optNullableDouble(key: String): Double? =
    if (has(key) && !isNull(key)) optDouble(key) else null

private fun JSONObject.optPayload(): Map<String, Any>? {
    if (!has("payload") || isNull("payload")) {
        return null
    }

    val json = optJSONObject("payload") ?: return null
    val output = linkedMapOf<String, Any>()
    val keys = json.keys()
    while (keys.hasNext()) {
        val key = keys.next()
        when (val value = json.get(key)) {
            is String -> output[key] = value
            is Boolean -> output[key] = value
            is Number -> output[key] = value
        }
    }

    return output.ifEmpty { null }
}
