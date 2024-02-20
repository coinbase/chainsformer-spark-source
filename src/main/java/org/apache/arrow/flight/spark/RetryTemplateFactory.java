package org.apache.arrow.flight.spark;

import com.google.common.collect.ImmutableMap;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.FlightRuntimeException;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

public class RetryTemplateFactory {
    // Max retry backoff = 0 + 0.1 + 0.2 + 0.4 + 0.8 + 1.6 + 3.2 + 6.4 = 12.7 seconds.
    private static final int MAX_ATTEMPTS = 8;

    public RetryTemplate apply() {
        RetryTemplate retryTemplate = new RetryTemplate();

        ExponentialBackOffPolicy exponentialBackOffPolicy = new ExponentialBackOffPolicy();
        retryTemplate.setBackOffPolicy(exponentialBackOffPolicy);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(
                MAX_ATTEMPTS,
                ImmutableMap.of(
                        FlightRuntimeException.class, true,
                        StatusRuntimeException.class, true
                ),
                true // traverseCauses
        );
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate;
    }
}
