package com.learnkafka.jpa;

import com.learnkafka.entity.FailureRecord;
import java.util.List;
import org.springframework.data.repository.CrudRepository;

public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {

    List<FailureRecord> findAllByStatus(String retry);
}
