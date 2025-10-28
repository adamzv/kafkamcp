package com.github.adamzv.kafkamcp.support;

import com.github.adamzv.kafkamcp.domain.Limits;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "limits")
public record LimitsProperties(
    @Positive(message = "limits.messagesPerCall must be > 0")
    int messagesPerCall,
    @Positive(message = "limits.bytesPerCall must be > 0")
    int bytesPerCall,
    @Positive(message = "limits.messageBytes must be > 0")
    int messageBytes,
    @Positive(message = "limits.searchMaxResults must be > 0")
    int searchMaxResults,
    @Positive(message = "limits.searchMaxScan must be > 0")
    int searchMaxScan
) {

  public Limits toDomain() {
    return new Limits(messagesPerCall, bytesPerCall, messageBytes, searchMaxResults, searchMaxScan);
  }

  @AssertTrue(message = "limits.messageBytes must be <= limits.bytesPerCall")
  public boolean isMessageWithinBudget() {
    return messageBytes <= bytesPerCall;
  }
}
