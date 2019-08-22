package co.com.poc.sqslistener.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class SpringCloudSQS {
	private static final Logger logger = LoggerFactory.getLogger(SpringCloudSQS.class);
	static final String QUEUE_NAME = "queue-pedro.fifo";

	@Autowired
	QueueMessagingTemplate queueMessagingTemplate;


	@SqsListener(value= QUEUE_NAME, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
	public void recibirLosMensajes(String message, @Header("SenderId") String senderId) {
		logger.info("Received message: {}, having SenderId: {}", message, senderId);
		System.err.println("mensaje: " + message + "id : " + senderId);
		throw new RuntimeException("Pedro estas el error por ende va para la cola");

	}	
}
