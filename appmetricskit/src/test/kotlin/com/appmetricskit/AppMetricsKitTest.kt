package com.appmetricskit

import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.IOException
import java.security.MessageDigest
import java.util.Locale

class AppMetricsKitTest {
    @Test
    fun encodesBackendIngestEnvelopeAndHeaders() {
        val transport = FakeTransport(statuses = mutableListOf(200))
        val client = makeClient(transport)

        client.configureForTests(
            config = config(
                testMode = true,
                allowedPayloadKeys = setOf("plan", "source", "trial"),
            ),
            metadataProvider = fakeMetadata,
            queueStore = InMemoryQueueStore(),
        )

        val eventId = client.track(
            AppMetricsEvent.paywallViewed,
            payload = mapOf("plan" to "annual", "source" to "onboarding", "trial" to true),
        )
        val result = client.flushBlocking()

        assertNotNull(eventId)
        assertEquals(1, result.delivered)
        assertEquals("https://example.com/api/ingest", transport.requests.single().url)
        assertEquals("amk_live_test", transport.requests.single().ingestKey)

        val event = transport.firstEventJson()
        assertEquals(AppMetricsEvent.paywallViewed, event.getString("eventName"))
        assertEquals(true, event.getBoolean("isTestMode"))
        assertEquals("android", event.getString("platform"))
        assertEquals("1.0", event.getString("appVersion"))
        assertEquals("42", event.getString("buildNumber"))
        assertEquals("Android 16", event.getString("osVersion"))
        assertEquals("Pixel Test", event.getString("deviceModel"))
        assertEquals("en-US", event.getString("locale"))
        assertEquals("Europe/Brussels", event.getString("timezone"))
        assertEquals("id-3", event.getString("eventId"))
        assertEquals("id-2", event.getString("sessionId"))

        val payload = event.getJSONObject("payload")
        assertEquals("annual", payload.getString("plan"))
        assertEquals("onboarding", payload.getString("source"))
        assertEquals(true, payload.getBoolean("trial"))
    }

    @Test
    fun filtersBlockedUnallowedAndPiiPayloadValues() {
        val transport = FakeTransport(statuses = mutableListOf(200))
        val client = makeClient(transport)

        client.configureForTests(
            config = config(
                allowedPayloadKeys = setOf("plan", "email", "phone", "debug"),
                blockedPayloadKeys = setOf("phone"),
            ),
            metadataProvider = fakeMetadata,
            queueStore = InMemoryQueueStore(),
        )

        client.track(
            AppMetricsEvent.purchaseCompleted,
            payload = mapOf(
                "plan" to "annual",
                "email" to "marwan@example.com",
                "phone" to "+32 470 12 34 56",
                "debug" to true,
                "ignored" to "not-allowed",
            ),
            floatValue = 59.99,
        )
        val result = client.flushBlocking()

        assertEquals(1, result.delivered)
        val event = transport.firstEventJson()
        assertEquals(59.99, event.getDouble("floatValue"), 0.0)

        val payload = event.getJSONObject("payload")
        assertEquals("annual", payload.getString("plan"))
        assertEquals(true, payload.getBoolean("debug"))
        assertFalse(payload.has("email"))
        assertFalse(payload.has("phone"))
        assertFalse(payload.has("ignored"))
    }

    @Test
    fun hashesUserIdOnDevice() {
        val transport = FakeTransport(statuses = mutableListOf(200))
        val client = makeClient(transport)

        client.configureForTests(config(), fakeMetadata, InMemoryQueueStore())
        client.identify("user-123")
        client.track(AppMetricsEvent.featureUsed, payload = mapOf("feature" to "export"))
        client.flushBlocking()

        val event = transport.firstEventJson()
        assertEquals(sha256("user-123"), event.getString("anonymousUserId"))
        assertNotEquals("user-123", event.getString("anonymousUserId"))
    }

    @Test
    fun rejectsInvalidEventNamesBeforeQueueing() {
        val transport = FakeTransport(statuses = mutableListOf(200))
        val client = makeClient(transport)

        client.configureForTests(config(), fakeMetadata, InMemoryQueueStore())

        val eventId = client.track("bad event name", payload = mapOf("plan" to "annual"))

        assertNull(eventId)
        assertEquals(0, client.pendingEventCount)
        assertTrue(transport.requests.isEmpty())
    }

    @Test
    fun persistsOfflineQueueToStore() {
        val store = InMemoryQueueStore()
        val firstClient = makeClient(FakeTransport(statuses = mutableListOf(500, 500, 500)))

        firstClient.configureForTests(config(), fakeMetadata, store)
        firstClient.track(AppMetricsEvent.paywallViewed, payload = mapOf("plan" to "monthly"))

        val secondClient = makeClient(FakeTransport(statuses = mutableListOf(200)))
        secondClient.configureForTests(config(), fakeMetadata, store)

        assertEquals(1, secondClient.pendingEventCount)
    }

    @Test
    fun retriesRetryableStatusAndKeepsQueueAfterFailures() {
        val transport = FakeTransport(statuses = mutableListOf(500, 500, 500))
        val client = makeClient(transport)

        client.configureForTests(config(), fakeMetadata, InMemoryQueueStore())
        client.track(AppMetricsEvent.paywallViewed)

        val result = client.flushBlocking()

        assertEquals(1, result.attempted)
        assertEquals(0, result.delivered)
        assertEquals(0, result.dropped)
        assertTrue(result.willRetry)
        assertEquals(1, client.pendingEventCount)
        assertEquals(3, transport.requests.size)
    }

    @Test
    fun dropsBatchOnNonRetryableStatus() {
        val transport = FakeTransport(statuses = mutableListOf(401))
        val client = makeClient(transport)

        client.configureForTests(config(), fakeMetadata, InMemoryQueueStore())
        client.track(AppMetricsEvent.paywallViewed)

        val result = client.flushBlocking()

        assertEquals(1, result.attempted)
        assertEquals(0, result.delivered)
        assertEquals(1, result.dropped)
        assertFalse(result.willRetry)
        assertEquals(0, client.pendingEventCount)
        assertEquals(1, transport.requests.size)
    }

    @Test
    fun trackErrorUsesNonPiiPayloadKey() {
        val transport = FakeTransport(statuses = mutableListOf(200))
        val client = makeClient(transport)

        client.configureForTests(config(), fakeMetadata, InMemoryQueueStore())
        client.trackError(name = "network_timeout", message = "Request timed out")
        client.flushBlocking()

        val payload = transport.firstEventJson().getJSONObject("payload")
        assertEquals("network_timeout", payload.getString("errorName"))
        assertEquals("Request timed out", payload.getString("message"))
        assertFalse(payload.has("name"))
    }

    private fun makeClient(transport: FakeTransport): AppMetricsClient {
        var counter = 0
        return AppMetricsClient(
            transport = transport,
            idGenerator = {
                counter += 1
                "id-$counter"
            },
            clockMillis = { 1781640000000L },
            sleeper = {},
        )
    }

    private fun config(
        testMode: Boolean = false,
        allowedPayloadKeys: Set<String>? = null,
        blockedPayloadKeys: Set<String> = AppMetricsConfig.defaultBlockedPayloadKeys,
    ) = AppMetricsConfig(
        ingestUrl = "https://example.com/api/ingest",
        ingestKey = "amk_live_test",
        testMode = testMode,
        flushIntervalMillis = 0,
        allowedPayloadKeys = allowedPayloadKeys,
        blockedPayloadKeys = blockedPayloadKeys,
        automaticAppLaunchTracking = false,
    )

    private fun sha256(value: String): String {
        val bytes = MessageDigest.getInstance("SHA-256")
            .digest(value.toByteArray(Charsets.UTF_8))
        return bytes.joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }
    }
}

private data class FakeRequest(
    val url: String,
    val ingestKey: String,
    val body: String,
)

private class FakeTransport(
    private val statuses: MutableList<Int> = mutableListOf(200),
    private val errors: MutableList<IOException> = mutableListOf(),
) : AppMetricsTransport {
    val requests = mutableListOf<FakeRequest>()

    override fun post(url: String, ingestKey: String, body: String): AppMetricsResponse {
        requests.add(FakeRequest(url, ingestKey, body))
        if (errors.isNotEmpty()) {
            throw errors.removeAt(0)
        }
        return AppMetricsResponse(if (statuses.isEmpty()) 200 else statuses.removeAt(0))
    }

    fun firstEventJson(): JSONObject {
        val envelope = JSONObject(requests.first().body)
        return envelope.getJSONArray("events").getJSONObject(0)
    }
}

private val fakeMetadata = object : AppMetricsMetadataProvider {
    override fun metadata(): AppMetricsMetadata = AppMetricsMetadata(
        appVersion = "1.0",
        buildNumber = "42",
        osVersion = "Android 16",
        deviceModel = "Pixel Test",
        locale = Locale.US.toLanguageTag(),
        timezone = "Europe/Brussels",
    )
}
