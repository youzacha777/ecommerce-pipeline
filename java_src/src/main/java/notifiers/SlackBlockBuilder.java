package notifiers;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.ProductDTO;

public class SlackBlockBuilder {
    
    // BROWSING 이벤트 블록 생성
    public static List<Map<String, Object>> buildBrowsingBlocks(List<ProductDTO> products) {
        String emoji = "🚀";
        String store = "E";

        // header 섹션 생성
        List<Map<String, Object>> blocks = new ArrayList<>();
        
        blocks.add(Map.of(
            "type", "header",
            "text", Map.of("type", "plain_text", "text", emoji + store + " 스토어 새로 입점! 신규상품을 확인해보세요!")
        ));
        
        // fields 섹션 생성
        List<Map<String, Object>> fields = new ArrayList<>();
        for (ProductDTO p : products) {
            if (!store.equals(p.getStore())) continue; // store 필터링
            fields.add(Map.of(
                "type", "mrkdwn",
                "text", "*" + p.getProductName() + "* 이벤트가 단돈" + p.getPrice() + "원!"
            ));
        }
        blocks.add(Map.of("type", "section", "fields", fields));

        // context 섹션 생성
        blocks.add(Map.of(
            "type", "context",
            "elements", List.of(
                Map.of("type", "mrkdwn", "text", "해당 상품의 할인 이벤트는 2025-10-30 까지 입니다."),
                Map.of("type", "mrkdwn","text","알림 시간 : " +
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
            )
        ));
        return blocks;
    }

    // SEARCH 이벤트 블록 생성
    public static List<Map<String, Object>> buildSearchBlocks(String keyword, List<ProductDTO> products) {
        String emoji = "🔍";
        List<Map<String, Object>> blocks = new ArrayList<>();

        // header 섹션 생성
        blocks.add(Map.of(
            "type", "header",
            "text", Map.of("type", "plain_text", "text", emoji + keyword + " 검색 관련 관련 추천 신규 상품입니다!")
        ));

        // 신규 입점 스토어(E) 상품 필터링
        List<ProductDTO> eStoreProducts = new ArrayList<>();
        for (ProductDTO p : products) {
            if ("E".equals(p.getStore())) {
                eStoreProducts.add(p);
            }
        }

        // 키워드별 필터링
        List<ProductDTO> filteredProducts = new ArrayList<>();

        if (List.of("A", "B", "C", "D").contains(keyword)) {
            // 키워드가 스토어명이면 E 스토어 전체 상품
            filteredProducts.addAll(eStoreProducts);
        } else {
            // 키워드가 상품명일 경우
            for (ProductDTO p : eStoreProducts) {
                if (p.getProductName().contains(keyword)) {
                    filteredProducts.add(p);
                }
            }
        }

        // fields 섹션 생성
        List<Map<String, Object>> fields = new ArrayList<>();
        for (ProductDTO p : filteredProducts) {
            fields.add(Map.of(
                "type", "mrkdwn",
                "text", "*" + p.getProductName() + "("+ p.getStore() +")* 단돈 " + p.getPrice() + "원!"
            ));
        }
        blocks.add(Map.of("type", "section", "fields", fields));

        // context 섹션 생성
        blocks.add(Map.of(
        "type", "context",
        "elements", List.of(
            Map.of("type", "mrkdwn", "text", "해당 상품의 할인 이벤트는 2025-10-30 까지입니다."),
            Map.of("type", "mrkdwn", "text", "알림 시간: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
        )
        ));

        return blocks;
    }

    /** NEXTPAGECLICK 이벤트 블록 생성 */
    public static List<Map<String, Object>> buildNextPageClickBlocks(String keyword, List<ProductDTO> products) {
        List<Map<String, Object>> blocks = new ArrayList<>();

        // header 섹션
        blocks.add(Map.of(
            "type", "header",
            "text", Map.of("type", "plain_text", "text", "아직 결정하지 못하셨나요? 최저가 상품 추천드려요!")
        ));

        List<ProductDTO> lowestProducts = new ArrayList<>();

        // 1. 키워드가 스토어명인 경우 → 카테고리별 최저가
        if (List.of("A", "B", "C", "D", "E").contains(keyword)) {
            Map<String, ProductDTO> lowestByCategory = new HashMap<>();

            for (ProductDTO p : products) {
                // 카테고리명 = productName에서 뒤에 "-a", "-b" 이런 suffix 제거
                String category = p.getProductName().split("-")[0];

                if (!lowestByCategory.containsKey(category) ||
                    p.getPrice() < lowestByCategory.get(category).getPrice()) {
                    lowestByCategory.put(category, p);
                }
            }
            lowestProducts.addAll(lowestByCategory.values());
        } 
        // 2. 키워드가 상품명일 경우 → 해당 상품의 최저가만
        else {
            ProductDTO lowest = null;
            for (ProductDTO p : products) {
                if (p.getProductName().contains(keyword)) {
                    if (lowest == null || p.getPrice() < lowest.getPrice()) {
                        lowest = p;
                    }
                }
            }
            if (lowest != null) {
                lowestProducts.add(lowest);
            }
        }

        // fields 섹션
        List<Map<String, Object>> fields = new ArrayList<>();
        for (ProductDTO p : lowestProducts) {
            fields.add(Map.of(
                "type", "mrkdwn",
                "text", "*" + p.getProductName() + " (" + p.getStore() + ")* 단돈 " + p.getPrice() + "원!"
            ));
        }
        blocks.add(Map.of("type", "section", "fields", fields));

        // context 섹션
        blocks.add(Map.of(
            "type", "context",
            "elements", List.of(
                Map.of("type", "mrkdwn", "text", "가격은 실시간 변동될 수 있습니다."),
                Map.of("type", "mrkdwn", "text", "알림 시간: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
            )
        ));

        return blocks;
    }





}