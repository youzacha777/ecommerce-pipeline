package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import notifiers.SlackNotifier;
import utils.ProductDTO;
import utils.ProductLoader;
import notifiers.SlackBlockBuilder;

import java.io.InputStream;
import java.util.Properties;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class UserEventStreamProcessor {

    // 로거 설정
    private static final Logger logger = LoggerFactory.getLogger(UserEventStreamProcessor.class);

    // 토픽 설정
    private static final String INPUT_TOPIC = "user_events";
    private static final String ALERT_TOPIC = "alert_events";
    private static final String ORDER_TOPIC = "order_events";

    // ObjectMapper 재사용을 위한 설정
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 이벤트 타입 Enum
    public enum EventType {
        BROWSING("Browsing"),
        SEARCH("Search"),
        NEXTPAGECLICK("NextPageClick"),
        ADDTOCART("AddToCart"),
        PURCHASE("Purchase");

        private final String jsonName;

        EventType(String jsonName) {
            this.jsonName = jsonName;
        }

        public String jsonName() {
            return jsonName;
        }
    }

    // 메인 함수
    public static void main(String[] args) {
        Properties props = loadProperties();

        // Kafka 토픽 존재 여부 확인 및 대기
        waitForInputTopic(props, INPUT_TOPIC);

        StreamsBuilder builder = new StreamsBuilder();

        // Slack Notifier 초기화
        SlackNotifier notifier = new SlackNotifier(
            props.getProperty("slack.webhook.url"),
            props.getProperty("slack.channel"),
            logger
        );

        // Product 목록 한 번만 로드
        List<ProductDTO> products = ProductLoader.loadProducts("products.json");

        // 카프카 스트림 토폴로지 정의

        // 전체 이벤트 스트림
        KStream<String, String> events = builder.stream(INPUT_TOPIC);

        // 👀 디버깅용 peek 추가: 들어오는 메시지 바로 확인
        events.peek((key, value) -> logger.info("👀 Received event from input topic: key={} value={}", key, value));

        // 1차 브랜치 분기 : alert / order 
        KStream<String, String>[] mainBranches = branchMainEvents(events);
        KStream<String, String> alertEvents = mainBranches[0];
        KStream<String, String> orderEvents = mainBranches[1];

        // 1차 분기 이벤트들 토픽으로 저장 + peek
        alertEvents.peek((k, v) -> logger.info("ALERT EVENT (before send to topic): {}", v));
        orderEvents.peek((k, v) -> logger.info("ORDER EVENT (before send to topic): {}", v));
        
        // 2차 브랜치 분기 : alertEvents -> BROWSING / SEARCH / NEXTPAGECLICK
        KStream<String, String>[] alertBranches = branchAlertEvents(alertEvents);
        KStream<String, String> browsingBranch = alertBranches[0];
        KStream<String, String> searchBranch = alertBranches[1];
        KStream<String, String> nextPageClickBranch = alertBranches[2];

        // 브랜치별 이벤트 처리 및 로그
        browsingBranch.peek((k, v) -> logger.info("BROWSING EVENT: {}", v))
                      .foreach((k,v) -> handleBrowsingEvent(v, notifier, products));

        searchBranch.peek((k, v) -> logger.info("SEARCH EVENT: {}", v))
                    .foreach((k,v) -> handleSearchEvent(v, notifier, products));

        nextPageClickBranch.peek((k, v) -> logger.info("NEXTPAGECLICK EVENT: {}", v))
                           .foreach((k,v) -> handleNextPageClickEvent(v, notifier, products));

        // 1차 분기 브랜치 토픽으로 전송
        sendToTopicAndLog(alertEvents, orderEvents);

        // Kafka Streams 실행
        KafkaStreams streams = createKafkaStreams(props, builder);

        // 상태 로깅
        logger.info("Kafka Streams started. Current state: " + streams.state());
    }

    // Properties 불러오기
    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = UserEventStreamProcessor.class.getClassLoader()
                .getResourceAsStream("streams_config.properties")) {
            if (input == null) {
                System.out.println("설정 파일을 찾을 수 없습니다.");
                System.exit(1);
            }
            props.load(input);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return props;
    }

    // 토픽 생성 여부 확인 함수
    private static void waitForInputTopic(Properties props, String inputTopic) {
        try (AdminClient admin = AdminClient.create(props)) {
            boolean exists = false;
            while (!exists) {
                ListTopicsOptions options = new ListTopicsOptions();
                options.timeoutMs(5000);
                options.listInternal(false);

                ListTopicsResult topicsResult = admin.listTopics(options);
                Set<String> existingTopics = topicsResult.names().get(5, TimeUnit.SECONDS);

                if (existingTopics.contains(inputTopic)) {
                    exists = true;
                    System.out.println("✅ 입력 토픽 준비 완료: " + inputTopic);
                } else {
                    System.out.println("⏳ 입력 토픽이 아직 없습니다. 5초 후 다시 확인...");
                    Thread.sleep(5000);
                }
            }
        } catch (Exception e) {
            logger.error("토픽 확인 중 오류 발생: {}", e.getMessage());
            System.exit(1);
        }
    }

    // Kafka Streams 1차 브랜치 분기 함수
    private static KStream<String, String>[] branchMainEvents(KStream<String, String> events) {
        return events.branch(
            (k,v) -> isAlertEvent(v),
            (k,v) -> isOrderEvent(v)
        );
    }

    // 2차 브랜치 alertEvents -> BROWSING / SEARCH / NEXTPAGECLICK
    private static KStream<String, String>[] branchAlertEvents(KStream<String, String> alertEvents) {
        return alertEvents.branch(
            (k, v) -> isEventType(v, EventType.BROWSING),
            (k, v) -> isEventType(v, EventType.SEARCH),
            (k, v) -> isEventType(v, EventType.NEXTPAGECLICK)
        );
    }
    
    // Alert 이벤트 확인
    private static boolean isAlertEvent(String value) {
        try {
            JsonNode node = objectMapper.readTree(value);
            String type = node.get("event_type").asText();
            return type.equals(EventType.BROWSING.jsonName()) ||
                    type.equals(EventType.SEARCH.jsonName()) ||
                    type.equals(EventType.NEXTPAGECLICK.jsonName());
        } catch (Exception e) {
            logger.warn("Alert 이벤트 JSON 파싱 실패 : {}", value);
            return false;
        }
    }

    private static boolean isOrderEvent(String value) {
        try {
            JsonNode node = objectMapper.readTree(value);
            String type = node.get("event_type").asText();
            return type.equals(EventType.ADDTOCART.jsonName()) ||
                    type.equals(EventType.PURCHASE.jsonName());
        } catch (Exception e) {
            logger.warn("Order 이벤트 JSON 파싱 실패 : {}", value);
            return false;
        }
    }

    private static boolean isEventType(String value, EventType targetType) {
        try {
            JsonNode node = objectMapper.readTree(value);
            return node.get("event_type").asText().equals(targetType.jsonName());
        } catch (Exception e) {
            logger.warn("Event 타입 확인 실패 :  {}", value);
            return false;
        }
    }


    private static void handleBrowsingEvent(String value, SlackNotifier notifier, List<ProductDTO> products) {
        // BROWSING 이벤트 처리
        try {
            JsonNode node = objectMapper.readTree(value);
            List<Map<String, Object>> blocks = SlackBlockBuilder.buildBrowsingBlocks(products);
            String summaryText = "E 스토어 신상품 입고 알림";
            notifier.sendMessage(blocks, summaryText);
        } catch (Exception e) {
            logger.error("BROWSING 이벤트 Slack 알림 실패: {}", e.getMessage());
        }
    }

    private static void handleSearchEvent(String value, SlackNotifier notifier, List<ProductDTO> products) {
        // SEARCH 이벤트 처리
        try {
            JsonNode node = objectMapper.readTree(value);
            String keyword = node.get("keyword").asText();  // 이벤트에서 keyword 추출
            List<Map<String, Object>> blocks = SlackBlockBuilder.buildSearchBlocks(keyword, products);
            String summaryText = keyword + " 관련 추천 신규 상품";
            notifier.sendMessage(blocks, summaryText);
        } catch (Exception e) {
            logger.error("SEARCH 이벤트 Slack 알림 실패: {}", e.getMessage());
        }
    }

    private static void handleNextPageClickEvent(String value, SlackNotifier notifier, List<ProductDTO> products) {
        // NEXTPAGECLICK 이벤트 처리
            try {
            JsonNode node = objectMapper.readTree(value);
            String keyword = node.get("keyword").asText();  // 이벤트에서 keyword 추출
            List<Map<String, Object>> blocks = SlackBlockBuilder.buildNextPageClickBlocks(keyword, products);
            String summaryText = keyword + " 관련 최저가 상품 추천 알림";
            notifier.sendMessage(blocks, summaryText);
        } catch (Exception e) {
            logger.error("NEXTPAGECLICK 이벤트 Slack 알림 실패: {}", e.getMessage());
        }
    }

    // orderEvents 처리 (분류 메시지 토픽으로 전달)
    private static void sendToTopicAndLog(KStream<String, String> alertEvents, KStream<String, String> orderEvents) {
        orderEvents.to(ORDER_TOPIC);
        alertEvents.to(ALERT_TOPIC);
        orderEvents.foreach((k, v) -> logger.info("ORDER EVENT : {} -> {}", k, v));
        alertEvents.foreach((k, v) -> logger.info("ALERT EVENT : {} -> {}", k, v));
    }

    // Kafka Streams 생성
    private static KafkaStreams createKafkaStreams(Properties props, StreamsBuilder builder) {
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        return streams;
    }

}
