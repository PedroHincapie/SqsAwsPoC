package co.com.poc.sqslistener.sqs;



import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

@Component
public class SpringCloudSQS {
	private static final Logger logger = LoggerFactory.getLogger(SpringCloudSQS.class);
	static final boolean IMPRESORA_EN_FALLO = true;

	static final String QUEUE_AL_FALLA_CONSUMIDOR = "al-fallar-consumidor.fifo";
	static final String QUEUE_AL_NO_EXISTIR_MENSAJE= "al-no-encontrar-mensajes-en-cola.fifo";
	static final Integer TIEMPO_DE_ESPERA_SEGUNDOS = 20;

	@Autowired
	QueueMessagingTemplate queueMessagingTemplate;

	//	Este método tiene por objetivo presentar la
	//	estrategia diseñada para eventualidades de fallos en el consumidor para una cola. 
	@SqsListener(value= QUEUE_AL_FALLA_CONSUMIDOR, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
	public void recibirMensajesYErrorEnCliente(String message, @Header("SenderId") String senderId) {
		logger.info("Received message: {}, having SenderId: {}", message, senderId);

		//Validar el estado de la impresora
		if(IMPRESORA_EN_FALLO) {
			//Crear el cliente
			final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

			//Consultar la URL de la cola que se ajustara
			final String queueUrl= sqs.getQueueUrl(QUEUE_AL_FALLA_CONSUMIDOR).getQueueUrl();

			//Contruir lista con el atributo de la cola a necesitar
			List<String> attributeNames = new ArrayList<>();
			attributeNames.add("VisibilityTimeout");

			//Crear Objeto del request con el objeto a solicitar
			GetQueueAttributesRequest requestAtri = new GetQueueAttributesRequest(queueUrl);
			requestAtri.setAttributeNames(attributeNames);

			//Realizar la solicitud con el cliente y obtener el valor
			Integer tiempoVisibilidad = Integer.parseInt(sqs.getQueueAttributes(requestAtri).getAttributes().get("VisibilityTimeout"));

			System.err.println("Este es el tiempo :" + tiempoVisibilidad);

			//Incrementar valor
			Integer tiempoVisibilidadIncrementado = tiempoVisibilidad + TIEMPO_DE_ESPERA_SEGUNDOS;

			System.err.println("Este es el tiempo resultante :" + tiempoVisibilidadIncrementado);

			//Crear objeto con el valor de la visibilidad
			final SetQueueAttributesRequest request = new SetQueueAttributesRequest()
					.withQueueUrl(queueUrl)
					.addAttributesEntry(QueueAttributeName.VisibilityTimeout
							.toString(), tiempoVisibilidadIncrementado.toString());

			//Seterar el valor en aws
			sqs.setQueueAttributes(request);

			throw new RuntimeException("Pedro estas el error por ende va para la cola");
		}

		System.err.println("mensaje exitoso: " + message + "id : " + senderId);




	}

	//	Este método tiene por objetivo presentar la estrategia diseñada para 
	//	eventualidades en las que no se reciben mensajes por parte 
	//	de la cola y mitigar costos.
	//	@SqsListener(value= QUEUE_AL_FALLA_CONSUMIDOR, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
	//	public void recibirCeroMensajes(String message, @Header("SenderId") String senderId) {
	//		logger.info("Received message: {}, having SenderId: {}", message, senderId);
	//		System.err.println("mensaje: " + message + "id : " + senderId);
	//	}	
}
