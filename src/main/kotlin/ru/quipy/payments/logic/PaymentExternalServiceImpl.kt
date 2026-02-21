package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.eclipse.jetty.websocket.api.StatusCode
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.OngoingWindowAsync
import java.io.InterruptedIOException
import java.io.IOException
import java.net.SocketTimeoutException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.net.URI
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException
import javax.management.RuntimeErrorException
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlin.math.min
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.CancellationException
import kotlin.coroutines.resume
import kotlinx.coroutines.future.await
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
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

    private val slidingWindow = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindowAsync(parallelRequests)
    private val minDeadlineDelta = 10

    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build()


    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (deadline - now() < minDeadlineDelta) {
            val current = now()
            logger.warn("[$accountName] Skipping payment $paymentId: deadline exceeded, $deadline, $current")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), null, reason = "Deadline exceeded")
            }
            return
        }

        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val request = HttpRequest.newBuilder()
                .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                .POST(emptyBody)
                .timeout(Duration.ofSeconds(deadline - now()))
                .build()

        ongoingWindow.acquire()
        try {
            trySendRequest(request, paymentId, transactionId, deadline)
        } finally {
            ongoingWindow.release()
        }
    }

    suspend private fun trySendRequest(request: HttpRequest, paymentId: UUID, transactionId: UUID, deadline: Long) {
        var attemptIndex = 0
        val maxAttempts = 3

        repeat(maxAttempts) {
            if (deadline - now() < minDeadlineDelta) {
                val current = now()
                logger.warn("[$accountName] Skipping payment $paymentId: deadline exceeded, $deadline, $current")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), null, reason = "Deadline exceeded")
                }
                return
            }

            slidingWindow.tickAsync()
            attemptIndex += 1

            metrics.retryCounter.increment()

            val sample = Timer.start(meterRegistry)


            try {
                var response = client
                    .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .await()

                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] txId=$transactionId payment=$paymentId code=${response.statusCode()} reason=${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                metrics.retriesPerRequestSummary.record((attemptIndex).toDouble())

                if (response.statusCode() in 200..299) {
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    return
                }
            } catch (e: Exception) {
                when {
                    e is TimeoutCancellationException || e is HttpTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    } 
                    
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                sample.stop(metrics.requestDurationTimer)
            } 
        }
        
        metrics.retriesPerRequestSummary.record(maxAttempts.toDouble())
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "request failed")
        }

    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()