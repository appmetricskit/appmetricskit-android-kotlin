# AppMetricsKit Android SDK

Privacy-first mobile analytics for Android apps. The SDK batches events, retries
safely, works offline, hashes user IDs on device, and sends only flat primitive
payloads to your AppMetricsKit ingest endpoint.

## Installation

Add Maven Central to your repositories:

```kotlin
// settings.gradle.kts
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
    }
}
```

Add AppMetricsKit to your app module:

```kotlin
// app/build.gradle.kts
dependencies {
    implementation("com.appmetricskit:appmetricskit-android:0.1.0")
}
```

## Quickstart

Create an app and ingest key inside AppMetricsKit, then configure the SDK when
your Android app starts:

```kotlin
import android.app.Application
import com.appmetricskit.AppMetricsConfig
import com.appmetricskit.AppMetricsKit

class ExampleApp : Application() {
    override fun onCreate() {
        super.onCreate()

        AppMetricsKit.configure(
            context = this,
            config = AppMetricsConfig(
                ingestUrl = "https://appmetricskit.com/api/ingest",
                ingestKey = "amk_live_...",
                testMode = false,
            ),
        )
    }
}
```

Track events:

```kotlin
AppMetricsKit.track("Paywall.viewed", mapOf("plan" to "annual"))
AppMetricsKit.track(
    eventName = "Purchase.completed",
    payload = mapOf("plan" to "annual"),
    floatValue = 59.99,
)
```

Identify a user without sending the raw user ID:

```kotlin
AppMetricsKit.identify(userId = user.id)
```

`identify(userId)` hashes the value with SHA-256 on device and sends it as
`anonymousUserId`.

## Backend Contract

The SDK sends batches to:

```text
POST /api/ingest
Authorization: Bearer amk_live_...
Content-Type: application/json
```

Body:

```json
{
  "events": [
    {
      "eventName": "Paywall.viewed",
      "anonymousUserId": "hashed-user-id",
      "sessionId": "uuid",
      "eventTime": 1781640000000,
      "isTestMode": false,
      "platform": "android",
      "appVersion": "1.0",
      "buildNumber": "42",
      "osVersion": "16 (36)",
      "deviceModel": "Google Pixel 9",
      "locale": "en-US",
      "timezone": "Europe/Brussels",
      "eventId": "uuid",
      "payload": {
        "plan": "annual"
      }
    }
  ]
}
```

## Event Naming

Event names must use `Namespace.action` format:

```kotlin
AppMetricsKit.track("Onboarding.started")
AppMetricsKit.track("Onboarding.completed")
AppMetricsKit.track("Paywall.viewed")
AppMetricsKit.track("Purchase.completed")
AppMetricsKit.track("Feature.used", mapOf("feature" to "export"))
AppMetricsKit.track("Error.occurred", mapOf("errorName" to "network_timeout"))
```

Common event constants are included:

```kotlin
import com.appmetricskit.AppMetricsEvent

AppMetricsKit.track(AppMetricsEvent.paywallViewed)
AppMetricsKit.track(AppMetricsEvent.purchaseCompleted)
```

Convenience helpers are included:

```kotlin
AppMetricsKit.trackAppLaunch()
AppMetricsKit.trackOnboardingStarted()
AppMetricsKit.trackOnboardingCompleted()
AppMetricsKit.trackPaywallViewed(plan = "annual")
AppMetricsKit.trackPurchaseCompleted(plan = "annual", amount = 59.99)
AppMetricsKit.trackError(name = "network_timeout", message = "Request timed out")
```

## Payload Rules

Payloads are flat maps containing only strings, numbers, and booleans:

```kotlin
AppMetricsKit.track(
    "Feature.used",
    mapOf(
        "feature" to "csv_export",
        "count" to 3,
        "isPro" to true,
    ),
)
```

The SDK drops:

- Blocked keys such as `email`, `phone`, `name`, `idfa`, `ipAddress`, and location keys.
- Values that look like email addresses, phone numbers, or credit card numbers.
- Keys not present in `allowedPayloadKeys`, when an allowlist is configured.
- Invalid event names.
- Non-finite numeric values.
- Nested objects, arrays, and other non-primitive values.

Example strict configuration:

```kotlin
AppMetricsKit.configure(
    context = applicationContext,
    config = AppMetricsConfig(
        ingestUrl = "https://appmetricskit.com/api/ingest",
        ingestKey = "amk_live_...",
        allowedPayloadKeys = setOf("plan", "source", "feature", "price"),
        blockedPayloadKeys = setOf("email", "phone", "name"),
    ),
)
```

## Offline Queue And Retries

Events are persisted under the app files directory and flushed in batches.
Defaults:

- `batchSize`: 25 events
- `flushIntervalMillis`: 30,000 milliseconds
- `maxQueueSize`: 10,000 events
- backend max batch size: 500 events

The SDK retries network errors, `408`, `429`, and `5xx` responses with backoff.
It drops non-retryable failures such as `400`, `401`, `403`, `404`, and `413` to
avoid retry loops.

Manually flush:

```kotlin
val future = AppMetricsKit.flush()
val result = future.get()
println(result.delivered)
```

Or flush synchronously:

```kotlin
val result = AppMetricsKit.flushBlocking()
```

Pause or resume collection:

```kotlin
AppMetricsKit.setCollectionEnabled(false)
AppMetricsKit.setCollectionEnabled(true)
```

## Test Mode

Use test mode while instrumenting your app:

```kotlin
AppMetricsKit.configure(
    context = applicationContext,
    config = AppMetricsConfig(
        ingestUrl = "https://appmetricskit.com/api/ingest",
        ingestKey = "amk_test_...",
        testMode = true,
    ),
)
```

Test events are marked with `isTestMode: true` so they can be separated from
production analytics.

## Android Examples

Jetpack Compose button:

```kotlin
import androidx.compose.material3.Button
import androidx.compose.material3.Text
import com.appmetricskit.AppMetricsKit

@Composable
fun PaywallButton() {
    Button(
        onClick = {
            AppMetricsKit.track(
                "Paywall.ctaTapped",
                mapOf("plan" to "annual", "placement" to "hero"),
            )
        },
    ) {
        Text("Start annual plan")
    }
}
```

Google Play Billing:

```kotlin
fun onPurchaseStarted(productId: String) {
    AppMetricsKit.track("Purchase.started", mapOf("productId" to productId))
}

fun onPurchaseCompleted(productId: String, price: Double) {
    AppMetricsKit.track(
        eventName = "Purchase.completed",
        payload = mapOf("productId" to productId),
        floatValue = price,
    )
}

fun onPurchaseFailed(productId: String, reason: String) {
    AppMetricsKit.track(
        "Purchase.failed",
        mapOf("productId" to productId, "reason" to reason),
    )
}
```

## Troubleshooting

- **No events in dashboard**: confirm the ingest key starts with `amk_live_` or
  `amk_test_`, and that the ingest URL points to your deployed SaaS:
  `https://your-domain.com/api/ingest`.
- **401 responses**: regenerate the app ingest key in AppMetricsKit and update
  your app configuration.
- **413 responses**: reduce custom payload size or batch size.
- **Payload values missing**: check `allowedPayloadKeys`, `blockedPayloadKeys`,
  and the privacy filters.
- **Nothing flushes in local tests**: call `flushBlocking()` after tracking so
  the process does not exit before the async flush finishes.

## Development

Run tests:

```bash
./gradlew test
```

Build the release AAR:

```bash
./gradlew :appmetricskit:assembleRelease
```

Publish to your local Maven cache:

```bash
./gradlew :appmetricskit:publishToMavenLocal
```

To test an unpublished local build in another Android app, add `mavenLocal()`
before Maven Central in that app's repositories:

```kotlin
dependencyResolutionManagement {
    repositories {
        mavenLocal()
        google()
        mavenCentral()
    }
}
```

Then depend on the local artifact:

```kotlin
dependencies {
    implementation("com.appmetricskit:appmetricskit-android:0.1.0")
}
```

The SDK has no runtime third-party dependencies.

## Publishing To Maven Central

The project is configured for Maven Central publishing with the Vanniktech Maven
Publish plugin. Releases publish the `release` AAR, sources jar, javadoc jar,
POM metadata, signatures, and Gradle module metadata.

The Maven coordinates are:

```text
com.appmetricskit:appmetricskit-android:<version>
```

Maintainers need these GitHub Actions secrets:

```text
MAVEN_CENTRAL_USERNAME
MAVEN_CENTRAL_PASSWORD
SIGNING_IN_MEMORY_KEY
SIGNING_IN_MEMORY_KEY_PASSWORD
```

`MAVEN_CENTRAL_USERNAME` and `MAVEN_CENTRAL_PASSWORD` must come from a Maven
Central user token, not your normal login password.

Generate the signing key value with:

```bash
gpg --export-secret-keys --armor <key-id>
```

Release a version by pushing a tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The publish workflow runs tests, lint, and then:

```bash
./gradlew publishAndReleaseToMavenCentral --no-configuration-cache
```

You can also run a manual release from GitHub Actions by providing the exact
version number.

## License

MIT. See [LICENSE](LICENSE).
