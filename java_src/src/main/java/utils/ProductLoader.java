package utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

public class ProductLoader {

    /**
     * resources 폴더 안에 있는 JSON 파일을 읽어서 List<ProductDTO> 반환
     * @param resourcePath JSON 파일 경로 (예: "products.json")
     * @return JSON 데이터를 매핑한 ProductDTO 리스트
     */
    public static List<ProductDTO> loadProducts(String resourcePath) {
        ObjectMapper mapper = new ObjectMapper();

        // ClassLoader를 사용해 resources에서 InputStream 얻기
        try (InputStream is = ProductLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                System.err.println("JSON 파일을 찾을 수 없습니다: " + resourcePath);
                return Collections.emptyList();
            }

            return mapper.readValue(is, new TypeReference<List<ProductDTO>>() {});
        } catch (IOException e) {
            System.err.println("JSON 파일 읽기/파싱 중 오류 발생: " + e.getMessage());
            return Collections.emptyList();
        }
    }
}
