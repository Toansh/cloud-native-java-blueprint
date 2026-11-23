package dev.toansh.blueprint.infrastructure.outbox;

public enum OutboxStatus {
    PENDING,
    SENT,
    FAILED
}
