package com.example.lab05.repository.mongo;

import com.example.lab05.model.mongo.PurchaseReceipt;
import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.List;

public interface PurchaseReceiptRepository extends MongoRepository<PurchaseReceipt, String> {
    List<PurchaseReceipt> findByPersonName(String personName);
    List<PurchaseReceipt> findByProductCategory(String category);
}