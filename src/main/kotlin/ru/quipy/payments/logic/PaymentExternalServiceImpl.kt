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
import kotlinx.coroutines.*
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.future.await
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout
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
                    .connectTimeout(Duration.ofMillis(1500))
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

        ongoingWindow.withSlot {
            withTimeout(1500) {
                select<Result<String>> {
                    async { trySendRequest(request, paymentId, transactionId, deadline) }

                    async {
                        delay(50)
                        trySendRequest(request, paymentId, transactionId, deadline)
                    }

                    async {
                        delay(50)
                        trySendRequest(request, paymentId, transactionId, deadline)
                    }
                }
            }
        }
    }

    suspend private fun trySendRequest(
            request: HttpRequest,
            paymentId: UUID,
            transactionId: UUID,
            deadline: Long
    ): Boolean {
        val currentTime = now()
        if (deadline - currentTime < minDeadlineDelta) {
            logger.warn(
                    "[$accountName] Skipping payment $paymentId: deadline exceeded, $deadline, $currentTime"
            )
            return false
        }

        slidingWindow.tickAsync()
        metrics.retryCounter.increment()
        val sample = Timer.start(meterRegistry)

        try {
            sendRequest(request, paymentId, transactionId)
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

        return false
    }

    suspend private fun sendRequest(
            request: HttpRequest,
            paymentId: UUID,
            transactionId: UUID
    ): Boolean {
        var response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
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

        if (response.statusCode() in 200..299) {
            logger.warn(
                    "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
            )

            return true
        }

        return false
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
