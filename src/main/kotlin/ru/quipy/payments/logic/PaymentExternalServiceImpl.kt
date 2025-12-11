package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.OngoingWindow
import java.io.InterruptedIOException
import java.io.IOException
import java.net.SocketTimeoutException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.URI
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.Executors
import javax.management.RuntimeErrorException
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlin.math.min
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.CancellationException
import kotlin.coroutines.resume
import kotlinx.coroutines.future.await
import kotlin.coroutines.resumeWithException


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

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    
    private val metrics = PaymentMetrics(meterRegistry, accountName)
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val slidingWindow = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    val dispatcher = Dispatcher().apply {
        maxRequests = 200_000
        maxRequestsPerHost = 200_000
    }

    // private val client = OkHttpClient
    //     .Builder()
    //     .connectTimeout(10, TimeUnit.SECONDS)    // TCP connection timeout
    //     .readTimeout(30, TimeUnit.SECONDS)       // Waiting for response data
    //     .writeTimeout(10, TimeUnit.SECONDS)      // Sending request body
    //     .callTimeout(45, TimeUnit.SECONDS)
    //     .dispatcher(dispatcher)
    //     .build()
    private val client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(100))
        .version(HttpClient.Version.HTTP_2)
        .build()

    class RateLimitExceededException() : Exception("Rate limit exceeded")
    class RetryAfterException(val interval: Long) : Exception("Retry after $interval ms")

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val request = HttpRequest.newBuilder()
                .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                .POST(HttpRequest.BodyPublishers.ofString(emptyBody.toString()))
                .build()

            trySendRequest(request, paymentId, transactionId, deadline)
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                } else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    // var quntileResponseTime = 1030L // ms 0.9 quantile response time

    // suspend fun Call.await(): Response {
    //     return suspendCancellableCoroutine { continuation ->
    //         // Ensure the HTTP call is cancelled if the coroutine is cancelled
    //         continuation.invokeOnCancellation { cancel() }

    //         enqueue(object : Callback {
    //             override fun onFailure(call: Call, e: IOException) {
    //                 continuation.resumeWithException(e)
    //             }

    //             override fun onResponse(call: Call, response: Response) {
    //                 // Important: do NOT close the response here!
    //                 // Let the caller manage it with .use { }
    //                 continuation.resume(response)
    //             }
    //         })
    //     }
    // }

    suspend private fun trySendRequest(request: HttpRequest, paymentId: UUID, transactionId: UUID, deadline: Long) {
        // val estimatedRemainingTime = deadline - quntileResponseTime
        
        var attemptIndex = 0
        val maxAttempts = 1
        // val baseDelayMs = 100L
        // val maxDelayMs = 5000L

        // var shouldRetry = true

        repeat(maxAttempts) {
            // if (!shouldRetry) return@repeat
            // if (attemptIndex > 0) {
                // val delayMs = minOf(baseDelayMs * (1L shl (attemptIndex - 1)), maxDelayMs) // битовый сдвиг эквивалентен возведению в степень, типа экспоненциальный бэкофф

                // if (estimatedRemainingTime - now() < delayMs) {
                //     throw RetryAfterException(quntileResponseTime)
                // }

                // try {
                //     Thread.sleep(delayMs)
                // } catch (e: InterruptedException) {
                //     Thread.currentThread().interrupt()
                //     logger.error("[$accountName] Interrupted during backoff delay for payment: $paymentId", e)
                //     return@repeat
                // }
            // }
            
            // shouldRetry = false
            attemptIndex++

            ongoingWindow.acquire()
            slidingWindow.tickBlocking()

            // if (estimatedRemainingTime - now() < 0) {
            //     throw RetryAfterException(quntileResponseTime)
            // }

            metrics.retryCounter.increment()

            val sample = Timer.start(meterRegistry)
            // val remainingTime = min(deadline - now().toLong(), quntileResponseTime)
            // val callTimeout = remainingTime.coerceAtLeast(1L)
            try {

                var response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                // client.newCall(request).await().use { response ->
                val responseBodyString = response.body()

                val body = try {
                    mapper.readValue(responseBodyString, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: $responseBodyString")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message ?: "Parse failed")
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                metrics.retriesPerRequestSummary.record((attemptIndex).toDouble())
                // }
                // val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                // call.timeout().timeout(10_000, TimeUnit.MILLISECONDS)
                // resp.use { response ->
                        // return@trySendRequest
                    // }
                // }

                // logger.warn("OUTSIDE CLIENT.CALl")
            } catch (e: CancellationException) {
                logger.warn("cancelattion exception:()")
                throw e

            } catch (e: Exception) {
                when (e) {
                    // is InterruptedIOException -> {
                    //     shouldRetry = true
                    // }
                    is RetryAfterException -> {
                        throw e
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                        // shouldRetry = true
                    }
                }
            } finally {
                sample.stop(metrics.requestDurationTimer)
                ongoingWindow.release()
            }
        }

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "request failed")
        }
        // Если все 5 попыток не увенчались успехом, записываем максимальное количество попыток
        metrics.retriesPerRequestSummary.record(maxAttempts.toDouble())
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()