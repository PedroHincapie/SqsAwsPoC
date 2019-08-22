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
	static final String QUEUE_AL_FALLA_CONSUMIDOR = "al-fallar-consumidor.fifo";
	static final String QUEUE_AL_NO_EXISTIR_MENSAJE= "al-no-encontrar-mensajes-en-cola.fifo";

	@Autowired
	QueueMessagingTemplate queueMessagingTemplate;

	//	Este método tiene por objetivo presentar la
	//	estrategia diseñada para eventualidades de fallos en el consumidor para una cola. 
	@SqsListener(value= QUEUE_AL_FALLA_CONSUMIDOR, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
	public void recibirMensajesYErrorEnCliente(String message, @Header("SenderId") String senderId) {
		logger.info("Received message: {}, having SenderId: {}", message, senderId);
		System.err.println("mensaje: " + message + "id : " + senderId);
		throw new RuntimeException("Pedro estas el error por ende va para la cola");

	}

	//	Este método tiene por objetivo presentar la estrategia diseñada para 
	//	eventualidades en las que no se reciben mensajes por parte 
	//	de la cola y mitigar costos.
	@SqsListener(value= QUEUE_AL_FALLA_CONSUMIDOR, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
	public void recibirCeroMensajes(String message, @Header("SenderId") String senderId) {
		logger.info("Received message: {}, having SenderId: {}", message, senderId);
		System.err.println("mensaje: " + message + "id : " + senderId);
		throw new RuntimeException("Pedro estas el error por ende va para la cola");

	}	
}
