package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer

class PaymentMetrics(
    private val meterRegistry: MeterRegistry,
    accountName: String
) {
    val retryCounter: Counter = Counter.builder("payment.retry.attempts")
        .description("Number of retry attempts for payment requests")
        .tag("account", accountName)
        .register(meterRegistry)

    val retriesPerRequestSummary: DistributionSummary = DistributionSummary.builder("payment.retries.per.request")
        .description("Average number of retries per payment request")
        .tag("account", accountName)
        .register(meterRegistry)

    val requestDurationTimer: Timer = Timer.builder("payment.request.duration")
        .publishPercentiles(0.05, 0.1, 0.5, 0.7, 0.8, 0.85, 0.9, 0.95, 0.99)
        .publishPercentileHistogram(true)
        .description("Time taken to execute payment request")
        .tag("account", accountName)
        .register(meterRegistry)
}

