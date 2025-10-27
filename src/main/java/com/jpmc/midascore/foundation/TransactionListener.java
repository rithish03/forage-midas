package com.jpmc.midascore.foundation;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.jpmc.midascore.foundation.Transaction;

@Service
public class TransactionListener {
    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if(sender != null && recipient != null){
            if(sender.getBalance() >= transaction.getAmount()){
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount());

                userRepository.save(sender);
                userRepository.save(recipient);

                TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
                transactionRecordRepository.save(record);

                System.out.println("Record Transaction Successful: " + record.getId() + " Amount: " + transaction.getAmount());
            }
            else{
                System.out.println("Insuffecient funds for sender ID: " + sender.getId());
            }
        }
        else{
            System.out.println("Invalid sender or recipient in Transaction: " + transaction);
        }
        System.out.println("POST-PROCESS: wilbur balance = " + userRepository.findByName("wilbur").getBalance());

    }
}