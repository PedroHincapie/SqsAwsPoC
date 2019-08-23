package co.com.poc.sqslistener.sqs;



import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

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
	static final boolean IMPRESORA_EN_FALLO = false;

	static final String QUEUE_AL_FALLA_CONSUMIDOR = "al-fallar-consumidor.fifo";
	static final String QUEUE_AL_NO_EXISTIR_MENSAJE= "al-no-encontrar-mensajes-en-cola.fifo";
	static final Integer TIEMPO_DE_ESPERA_SEGUNDOS = 20;
	static final String TIEMPO_POR_DEFAULT = "30";

	@Autowired
	QueueMessagingTemplate queueMessagingTemplate;

	//	Este método tiene por objetivo presentar la
	//	estrategia diseñada para eventualidades de fallos en el consumidor para una cola. 
	@SqsListener(value= QUEUE_AL_FALLA_CONSUMIDOR, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
	public void recibirMensajesYErrorEnCliente(String message, @Header("SenderId") String senderId) {
		logger.info("Mensaje a procesar : {}, having SenderId: {}", message, senderId);

		//Validar el estado de la impresora
		if(IMPRESORA_EN_FALLO) {
			incrementarTiemporDeVisibilidadDeMensaje();
			throw new RuntimeException("Pedro estas el error por ende va para la cola");
		}

		if(hayQueResetTiempoVisibilidad()) {
			resetTiemporDeVisibilidadDeMensaje();
		}

		logger.info("Mensaje exitoso y  procesado : {}, having SenderId: {}", message, senderId);
	}


	private void incrementarTiemporDeVisibilidadDeMensaje() {
		//Crear el cliente
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

		//Consultar la URL de la cola que se ajustara
		final String queueUrl= sqs.getQueueUrl(QUEUE_AL_FALLA_CONSUMIDOR).getQueueUrl();

		//Contruir lista con el atributo de la cola a necesitar
		List<String> attributeNames = new ArrayList<>();
		attributeNames.add("All");

		//Crear Objeto del request con el objeto a solicitar
		GetQueueAttributesRequest requestAtri = new GetQueueAttributesRequest(queueUrl);
		requestAtri.setAttributeNames(attributeNames);

		//Realizar la solicitud con el cliente y obtener el valor
		sqs.getQueueAttributes(requestAtri);
		for(Entry<String, String> datos : sqs.getQueueAttributes(requestAtri).getAttributes().entrySet()) {
			logger.info("Valor : {}  dato : {}",datos.getKey(),datos.getValue());
		}              
		Integer tiempoVisibilidad = Integer.parseInt(sqs.getQueueAttributes(requestAtri).getAttributes().get("VisibilityTimeout"));

		logger.info("Tiempo de visibilidad actual : {} ", tiempoVisibilidad);

		//Incrementar valor
		Integer tiempoVisibilidadIncrementado = tiempoVisibilidad + TIEMPO_DE_ESPERA_SEGUNDOS;

		logger.info("Tiempo de visibilidad resultante : {} ", tiempoVisibilidadIncrementado);

		//Crear objeto con el valor de la visibilidad
		final SetQueueAttributesRequest request = new SetQueueAttributesRequest()
				.withQueueUrl(queueUrl)
				.addAttributesEntry(QueueAttributeName.VisibilityTimeout
						.toString(), tiempoVisibilidadIncrementado.toString());

		//Seterar el valor en aws
		sqs.setQueueAttributes(request);

		logger.info("Valor incrementado exitosamente");
	}

	private void resetTiemporDeVisibilidadDeMensaje() {
		//Crear el cliente
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

		//Consultar la URL de la cola que se ajustara
		final String queueUrl= sqs.getQueueUrl(QUEUE_AL_FALLA_CONSUMIDOR).getQueueUrl();

		//Crear objeto con el valor de la visibilidad
		final SetQueueAttributesRequest request = new SetQueueAttributesRequest()
				.withQueueUrl(queueUrl)
				.addAttributesEntry(QueueAttributeName.VisibilityTimeout
						.toString(), TIEMPO_POR_DEFAULT);

		//Seterar el valor en aws
		sqs.setQueueAttributes(request);

		logger.info("Valor Reset exitosamente");
	}

	private boolean hayQueResetTiempoVisibilidad() {
		//Crear el cliente
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

		//Consultar la URL de la cola que se ajustara
		final String queueUrl= sqs.getQueueUrl(QUEUE_AL_FALLA_CONSUMIDOR).getQueueUrl();

		//Contruir lista con el atributo de la cola a necesitar
		List<String> attributeNames = new ArrayList<>();
		attributeNames.add("All");

		//Crear Objeto del request con el objeto a solicitar
		GetQueueAttributesRequest requestAtri = new GetQueueAttributesRequest(queueUrl);
		requestAtri.setAttributeNames(attributeNames);

		//Realizar la solicitud con el cliente y obtener el valor
		String tiempoVisibilidad = sqs.getQueueAttributes(requestAtri).getAttributes().get("VisibilityTimeoutdato");

		logger.info("El tiempo de visibilidad actual es de {}", tiempoVisibilidad);

		return !TIEMPO_POR_DEFAULT.equals(tiempoVisibilidad);
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
