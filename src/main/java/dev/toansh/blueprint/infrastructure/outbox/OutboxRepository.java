package dev.toansh.blueprint.infrastructure.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({
            @QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000")
    })
    @Query("""
           SELECT m FROM OutboxMessage m
           WHERE m.status = :status AND m.retryCount < :maxRetries
           ORDER BY m.createdAt ASC
           """)
    List<OutboxMessage> findPendingWithLock(@Param("status") OutboxStatus status,
                                           @Param("maxRetries") int maxRetries,
                                           Pageable pageable);
}
