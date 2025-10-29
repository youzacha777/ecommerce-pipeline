package utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProductDTO {
    
    @JsonProperty("store")
    private String store;

    @JsonProperty("product_id")
    private String product_id;

    @JsonProperty("product_name")
    private String product_name;

    @JsonProperty("price")
    private int price;

    public ProductDTO() {}

    public ProductDTO(String store, String product_id, String product_name, int price) {
        this.store = store;
        this.product_id = product_id;
        this.product_name = product_name;
        this.price = price;
    }

    // Getter
    public String getStore() {
        return store;
    }

    public String getProductId() {
        return product_id;
    }

    public String getProductName() {
        return product_name;
    }

    public int getPrice() {
        return price;
    }

    // toString() 오버라이드 (디버깅용)
    @Override
    public String toString() {
        return "ProductDTO{" +
                "store='" + store + '\'' +
                ", product_id='" + product_id + '\'' +
                ", product_name='" + product_name + '\'' +
                ", price=" + price +
                '}';
    }
}

