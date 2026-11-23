package dev.toansh.blueprint.domain.model;

public enum OrderStatus {
    PENDING,
    CONFIRMED,
    FAILED,
    CANCELLED;

    public boolean canTransitionTo(OrderStatus nextStatus) {
        return switch (this) {
            case PENDING -> nextStatus == CONFIRMED || nextStatus == CANCELLED || nextStatus == FAILED;
            case CONFIRMED -> nextStatus == CANCELLED;
            case FAILED, CANCELLED -> false;
        };
    }
}
