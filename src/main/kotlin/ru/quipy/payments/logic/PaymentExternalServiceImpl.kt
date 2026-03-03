package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.future.await
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindowAsync
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
        private val properties: PaymentAccountProperties,
        private val paymentESService:
                EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
        private val paymentProviderHostPort: String,
        private val token: String,
        private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = HttpRequest.BodyPublishers.noBody()
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val metrics = PaymentMetrics(meterRegistry, accountName)
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val slidingWindow =
            SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindowAsync(parallelRequests)
    private val minDeadlineDelta = 10

    private val executor = Executors.newFixedThreadPool(100)

    private val client =
            HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_2)
                    .connectTimeout(Duration.ofSeconds(1))
                    .build()

    override suspend fun performPaymentAsync(
            paymentId: UUID,
            amount: Int,
            paymentStartedAt: Long,
            deadline: Long
    ) {
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val current = now()
        if (deadline - current < minDeadlineDelta) {
            logger.warn(
                    "[$accountName] Skipping payment $paymentId: deadline exceeded, $deadline, $current"
            )
            return
        }

        val request =
                HttpRequest.newBuilder()
                        .uri(
                                URI.create(
                                        "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                                )
                        )
                        .POST(emptyBody)
                        .timeout(Duration.ofSeconds(deadline - current))
                        .build()

        ongoingWindow.withSlot { trySendRequest(request, paymentId, transactionId, deadline) }
    }

    suspend private fun trySendRequest(
            request: HttpRequest,
            paymentId: UUID,
            transactionId: UUID,
            deadline: Long
    ) {
        var attemptIndex = 0
        val maxAttempts = 1

        repeat(maxAttempts) {
            val current = now()
            if (deadline - current < minDeadlineDelta) {
                logger.warn(
                        "[$accountName] Skipping payment $paymentId: deadline exceeded, $deadline, $current"
                )
                return
            }

            slidingWindow.tickAsync()
            attemptIndex += 1
            metrics.retryCounter.increment()
            val sample = Timer.start(meterRegistry)

            try {
                var response =
                        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()

                val body =
                        try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                    "[$accountName] [ERROR] txId=$transactionId payment=$paymentId code=${response.statusCode()} reason=${response.body()}"
                            )
                            ExternalSysResponse(
                                    transactionId.toString(),
                                    paymentId.toString(),
                                    false,
                                    e.message
                            )
                        }

                metrics.retriesPerRequestSummary.record((attemptIndex).toDouble())

                if (response.statusCode() in 200..299) {
                    logger.warn(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
                    )

                    return
                }
            } catch (e: Exception) {
                when {
                    e is TimeoutCancellationException || e is HttpTimeoutException -> {
                        logger.error(
                                "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                                e
                        )
                    }
                    else -> {
                        logger.error(
                                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                                e
                        )
                    }
                }
            } finally {
                sample.stop(metrics.requestDurationTimer)
            }
        }

        metrics.retriesPerRequestSummary.record(maxAttempts.toDouble())
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
