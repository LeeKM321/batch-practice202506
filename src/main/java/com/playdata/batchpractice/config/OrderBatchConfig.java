package com.playdata.batchpractice.config;

import com.playdata.batchpractice.entity.Order;
import com.playdata.batchpractice.entity.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDateTime;

/*
====================================
       DB → DB 배치 (주문 처리)
====================================

목표: 데이터베이스의 주문 정보를 읽어서 처리하는 배치
핵심 개념: JdbcCursorItemReader, ItemProcessor, 비즈니스 로직 적용

이전 단계에서 추가되는 내용:
- Order 엔티티 추가
- DB → DB 처리
- ItemProcessor로 비즈니스 로직 적용
*/

@Configuration
@RequiredArgsConstructor
@Slf4j
public class OrderBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;


    // 1. ItemReader - PENDING 상태 주문들을 DB에서 조회
    @Bean
    public JdbcCursorItemReader<Order> pendingOrderReader() {
        String sql = """
                SELECT id, order_number, customer_name, amount, status, order_date, processed_date
                FROM orders 
                WHERE status = 'PENDING'
                AND order_date < NOW() - INTERVAL 10 MINUTE
                ORDER BY order_date
                """;

        return new JdbcCursorItemReaderBuilder<Order>()
                .name("pendingOrderReader")
                .dataSource(dataSource)
                .sql(sql)
                .rowMapper(new BeanPropertyRowMapper<>(Order.class))
                .build();
    }

    // 2. ItemProcessor - 주문 처리 로직
    @Bean
    public ItemProcessor<Order, Order> orderProcessor() {
        return order -> {
            log.info("주문 처리 중: {} (고객: {})", order.getOrderNumber(), order.getCustomerName());

            order.setProcessedDate(LocalDateTime.now()); // 처리 시간 기록

            // 비즈니스 로직: 금액에 따른 처리
            if (order.getAmount() < 10000) {
                order.setStatus(OrderStatus.COMPLETED); // 소액은 즉시 완료
            } else {
                order.setStatus(OrderStatus.PROCESSING); // 일반 주문은 처리 중으로
            }

            return order;
        };
    }


}










