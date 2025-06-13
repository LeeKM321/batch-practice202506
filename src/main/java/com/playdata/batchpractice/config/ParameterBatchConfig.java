package com.playdata.batchpractice.config;

import com.playdata.batchpractice.entity.Order;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.LocalDateTime;

/*
====================================
      Job Parameters + 스케줄링
====================================

목표: 조건부 처리와 자동 실행으로 실무 완성
핵심 개념: @StepScope, JobParameters, @Scheduled

이 단계에서 추가되는 내용:
- Job Parameters로 동적 조건 처리
- 스케줄링으로 자동 실행
- 다중 Step 연결
*/

@Configuration
@RequiredArgsConstructor
@Slf4j
public class ParameterBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;
    private final EntityManager entityManager;


    // 1. ItemReader
    @Bean
    // step 실행 시점에 Bean 생성
    // Job Parameter에 접근 가능해게 해줌
    // 실행 시점에 값을 주입
    @StepScope
    public JdbcCursorItemReader<Order> parameterOrderReader(
            // job을 run할 때 전달하는 jobParameter 객체에서 데이터를 가져올 수 있다.
            @Value("#{jobParameters['startDate']}") String startDate,
            @Value("#{jobParameters['endDate']}") String endDate,
            @Value("#{jobParameters['minAmount']}") String minAmount
    ) {
        log.info("처리 조건 - 기간: {} ~ {}, 최소금액: {}", startDate, endDate, minAmount);

        String sql = """
                SELECT id, order_number, customer_name, amount, status, order_date, processed_date
                FROM orders 
                WHERE status = 'PENDING'
                AND DATE(order_date) BETWEEN ? AND ? 
                AND amount >= ?
                ORDER BY order_date
                """;

        return new JdbcCursorItemReaderBuilder<Order>()
                .name("pendingOrderReader")
                .dataSource(dataSource)
                .sql(sql)
                .preparedStatementSetter(ps -> {
                    ps.setString(1, startDate);
                    ps.setString(2, endDate);
                    ps.setString(3, minAmount);
                })
                .rowMapper(new BeanPropertyRowMapper<>(Order.class))
                .build();
    }

    // 2. ItemProcessor - (에러 발생 로직 추가)
    @Bean
    @StepScope
    public ItemProcessor<Order, Order> parameterProcessor(
            @Value("#{jobParameters['processingMode']}") String processingMode
    ) {
        return order -> {
            log.info("처리 모드: {}, 주문: {}", processingMode, order.getOrderNumber());

            // FAST, NORMAL, CAREFUL
            switch (processingMode.toUpperCase()) {
                case "FAST":
                    // 빠른 처리! 모든 주문 즉시 완료
                    order.setStatus(Order.OrderStatus.COMPLETED);
                    break;

                case "NORMAL":
                    // 일반 처리! 금액별 분기 (기존 로직과 동일하게)
                    if (order.getAmount() < 10000) {
                        order.setStatus(Order.OrderStatus.COMPLETED);
                    } else {
                        order.setStatus(Order.OrderStatus.PROCESSING);
                    }
                    break;

                case "CAREFUL":
                    // 신중 처리: 모든 주문을 PROCESSING으로 강제
                    order.setStatus(Order.OrderStatus.PROCESSING);
                    break;
            }

            return order;
        };
    }

    // 3. ItemWriter - 기존과 동일하게 유지
    @Bean
    public JdbcBatchItemWriter<Order> parameterWriter() {
        JdbcBatchItemWriter<Order> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("""
            UPDATE orders
            SET status = ?, processed_date = ?
            WHERE id = ?
            """);

        writer.setItemPreparedStatementSetter((order, ps) -> {
            ps.setString(1, order.getStatus().toString()); // 또는 getStatus().name() 등 원하는 값
            ps.setObject(2, order.getProcessedDate());
            ps.setLong(3, order.getId());
        });

        writer.afterPropertiesSet();
        return writer;
    }

    /*
    # chunk
    - 대량 데이터 처리
    - 여러 건씩 처리 (기준별로 데이터를 잘게 나누어 진행)
    - 데이터 변환, 이관

    # Tasklet
    - 단순 작업
    - 전체를 한 번에
    - 파일 삭제, 통계 조회, 알림
     */

    @Bean
    @StepScope
    public Tasklet beforeTasklet(
            @Value("#{jobParameters['startDate']}") String startDate,
            @Value("#{jobParameters['endDate']}") String endDate
    ) {
        return (contribution, chunkContext) -> {
            // contribution: step 실행 기여도 정보 (읽은 개수, 처리한 개수, 쓴 개수 등을 제어)
            // chunkContext: 현재 실행중인 컨텍스트 정보 (step 실행 정보, job 실행 정보)
            // Tasklet에서 실행할 내용
            String countQuery = """
            SELECT COUNT(o) FROM Order o 
            WHERE o.status = 'PENDING' 
            AND DATE(o.orderDate) BETWEEN :startDate AND :endDate
            """;

            Long pendingCount = entityManager.createQuery(countQuery, Long.class)
                    .setParameter("startDate", LocalDate.parse(startDate))
                    .setParameter("endDate", LocalDate.parse(endDate))
                    .getSingleResult();

            log.info("=== 📊 배치 처리 전 현황 ===");
            log.info("처리 대상 기간: {} ~ {}", startDate, endDate);
            log.info("처리 대상 주문 수: {}건", pendingCount);
            log.info("========================");

            // FINISHED: 작업 완료. 다음 step로 진행
            // CONTINUABLE: 작업 계속. 이 Tasklet을 다시 실행
            return RepeatStatus.FINISHED;
        };
    }

    // Tasklet을 위한 새로운 step 생성
    // 매개값은 모두 null 전달 (job Parameter에서 가져올 거니까)
    @Bean
    public Step beforeParameterStep() {
        return new StepBuilder("beforeParameterStep", jobRepository)
                .tasklet(beforeTasklet(null, null), transactionManager)
                .build();
    }


    // 4. step (예외 처리 기능이 추가된 step)
    @Bean
    public Step parameterProcessStep() {
        return new StepBuilder("parameterProcessStep", jobRepository)
                .<Order, Order>chunk(3, transactionManager)
                .reader(parameterOrderReader(null, null, null))
                .processor(parameterProcessor(null))
                .writer(parameterWriter())
                .build();
    }

    // 5. Job (step 여러 개)
    @Bean
    public Job parameterJob() {
        return new JobBuilder("parameterJob", jobRepository)
                // 실행하고자 하는 스텝을 순서대로 next로 연결
                .start(beforeParameterStep())
                .next(parameterProcessStep())
                .build();
    }


}










