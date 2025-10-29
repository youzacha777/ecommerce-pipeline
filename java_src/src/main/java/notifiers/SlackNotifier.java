package notifiers;


import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

import utils.ProductDTO;

import org.slf4j.Logger;

public class SlackNotifier {
    private final String webhookUrl;
    private final String channel;
    private final Logger logger;
    private final ObjectMapper objectMapper;

    public SlackNotifier(String webhookUrl, String channel, Logger logger) {
        this.webhookUrl = webhookUrl;
        this.channel = channel;
        this.logger = logger;
        this.objectMapper = new ObjectMapper();

        if (webhookUrl == null || webhookUrl.isEmpty()) {
            logger.warn("Slack Webhook URL이 설정되지 않았습니다. 알림이 비활성화됩니다.");
        } else {
            logger.info("Slack 알림 초기화 완료. 채널: " + channel);
        }
    }

    // Slack 메시지 전송 함수
    public boolean sendMessage(List<Map<String, Object>> blocks, String text) {
        if (webhookUrl == null || webhookUrl.isEmpty()) {
            logger.warn("Webhook URL이 없어 메시지를 전송할 수 없습니다.");
            return false;
        }

        HttpURLConnection conn = null;

        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("text", text);
            payload.put("blocks", blocks);

            URL url = new URL(webhookUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            
            // JSON 직렬화 후 전송
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = objectMapper.writeValueAsBytes(payload);
                os.write(input, 0, input.length);
            }

            int status = conn.getResponseCode();

            if (status == 200) {
                logger.info("Slack 메시지 전송 성공");
                return true;
            } else {
                // 메시지 전송 실패시 에러 응답 본문 확인 가능
                try (InputStream es = conn.getErrorStream()) {
                    if (es != null) {
                        String errorResponse = new String(es.readAllBytes(), StandardCharsets.UTF_8);
                        logger.error("Slack 메시지 전송 실패: " + status + " 응답: " + errorResponse);
                    } else {
                        logger.error("Slack 메시지 전송 실패: " + status + " (응답 없음)");
                    }
                }
                return false;
            }
        } catch (Exception e) {
            logger.error("Slack 메시지 전송 중 오류 발생: " + e.getMessage());
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    // BROWSING 이벤트 알림 
    public boolean sendBrowsingAlert(String store, List<ProductDTO> products) {
        // 이벤트 블록 생성
        List<Map<String, Object>> blocks = SlackBlockBuilder.buildBrowsingBlocks(products);
        // Slack 전송 함수로 리턴
        return sendMessage(blocks, store + " 신상품 입고 알림");
    }

    // SEARCH 이벤트 알림
    public boolean sendSearchAlert(String keyword, List<ProductDTO> products) {
        // 이벤트 블록 생성
        List<Map<String, Object>> blocks = SlackBlockBuilder.buildSearchBlocks(keyword, products);
        // Slack 전송 함수로 리턴
        return sendMessage(blocks, keyword + " 관련 추천 신규 상품입니다!");
    }

    /** NEXTPAGECLICK 이벤트 알림 */
    public boolean sendNextPageClickAlert(String keyword, List<ProductDTO> products) {
        // 이벤트 블록 생성
        List<Map<String, Object>> blocks = SlackBlockBuilder.buildNextPageClickBlocks(keyword, products);
        // Slack 전송 함수로 리턴
        return sendMessage(blocks, keyword + "최저가 상품 추천 알림");
    }



    
}

