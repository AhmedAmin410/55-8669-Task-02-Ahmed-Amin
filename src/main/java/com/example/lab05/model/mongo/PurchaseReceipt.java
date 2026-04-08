package com.example.lab05.model.mongo;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;
import java.util.Map;

@Document(collection = "purchase_receipts")
public class PurchaseReceipt {

    @Id
    private String id;
    private String personName;
    private String productName;
    private String productCategory;
    private Integer quantity;
    private Double unitPrice;
    private Double totalPrice;
    private Map<String, Object> purchaseDetails;
    private LocalDateTime purchasedAt;

    public PurchaseReceipt() {}

    public PurchaseReceipt(String personName, String productName, String productCategory,
                           Integer quantity, Double unitPrice, Map<String, Object> purchaseDetails) {
        this.personName = personName;
        this.productName = productName;
        this.productCategory = productCategory;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.totalPrice = unitPrice * quantity;
        this.purchaseDetails = purchaseDetails;
        this.purchasedAt = LocalDateTime.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getPersonName() { return personName; }
    public void setPersonName(String personName) { this.personName = personName; }
    public String getProductName() { return productName; }
    public void setProductName(String productName) { this.productName = productName; }
    public String getProductCategory() { return productCategory; }
    public void setProductCategory(String productCategory) { this.productCategory = productCategory; }
    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }
    public Double getUnitPrice() { return unitPrice; }
    public void setUnitPrice(Double unitPrice) { this.unitPrice = unitPrice; }
    public Double getTotalPrice() { return totalPrice; }
    public void setTotalPrice(Double totalPrice) { this.totalPrice = totalPrice; }
    public Map<String, Object> getPurchaseDetails() { return purchaseDetails; }
    public void setPurchaseDetails(Map<String, Object> purchaseDetails) { this.purchaseDetails = purchaseDetails; }
    public LocalDateTime getPurchasedAt() { return purchasedAt; }
    public void setPurchasedAt(LocalDateTime purchasedAt) { this.purchasedAt = purchasedAt; }
}
