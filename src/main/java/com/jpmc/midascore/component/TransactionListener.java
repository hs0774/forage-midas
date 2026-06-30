package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
       UserRecord sender = userRepository.findById(transaction.getSenderId());
       UserRecord receiver = userRepository.findById(transaction.getRecipientId());
       float amount = transaction.getAmount();
       if(sender == null) {
           return;
       }
       if(receiver == null) {
           return;
       }
       if (amount > sender.getBalance()) {
           return;
       }
       sender.setBalance(sender.getBalance() - amount);
       receiver.setBalance(receiver.getBalance() + amount);

       userRepository.save(sender);
       userRepository.save(receiver);

        TransactionRecord record = new TransactionRecord(sender,receiver,amount);
        transactionRepository.save(record);
    }
}
