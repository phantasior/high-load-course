package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.OngoingWindow
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import javax.management.RuntimeErrorException
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlin.math.min


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

    private val client = OkHttpClient.Builder().build()

    class RateLimitExceededException() : Exception("Rate limit exceeded")
    class RetryAfterException(val interval: Long) : Exception("Retry after $interval ms")

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
           val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            trySendRequest(request, paymentId, transactionId, deadline)
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
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
        }
    }

    var quntileResponseTime = 1030L // ms 0.9 quantile response time

    private fun trySendRequest(request: Request, paymentId: UUID, transactionId: UUID, deadline: Long) {
        val estimatedRemainingTime = deadline - quntileResponseTime
        
        var attemptIndex = 0
        val maxAttempts = 5
        val baseDelayMs = 100L
        val maxDelayMs = 5000L

        var shouldRetry = true

        repeat(maxAttempts) {
            if (!shouldRetry) return@repeat
            if (attemptIndex > 0) {
                val delayMs = minOf(baseDelayMs * (1L shl (attemptIndex - 1)), maxDelayMs) // битовый сдвиг эквивалентен возведению в степень, типа экспоненциальный бэкофф

                if (estimatedRemainingTime - now() < delayMs) {
                    throw RetryAfterException(quntileResponseTime)
                }

                try {
                    Thread.sleep(delayMs)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    logger.error("[$accountName] Interrupted during backoff delay for payment: $paymentId", e)
                    return@repeat
                }
            }
            
            shouldRetry = false
            attemptIndex++

            ongoingWindow.acquire()
            slidingWindow.tickBlocking()

            if (estimatedRemainingTime - now() < 0) {
                throw RetryAfterException(quntileResponseTime)
            }

            metrics.retryCounter.increment()

            val sample = Timer.start(meterRegistry)
            val remainingTime = min(deadline - now().toLong(), quntileResponseTime)
            val callTimeout = remainingTime.coerceAtLeast(1L)
            try {
                val call = client.newCall(request)
                call.timeout().timeout(callTimeout, TimeUnit.MILLISECONDS)
                call.execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    if (body.result) {
                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }

                        metrics.retriesPerRequestSummary.record((attemptIndex).toDouble())
                        return@trySendRequest
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is InterruptedIOException -> {
                        shouldRetry = true
                    }
                    is RetryAfterException -> {
                        throw e
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                        shouldRetry = true
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