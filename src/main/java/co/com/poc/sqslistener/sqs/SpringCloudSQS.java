package co.com.poc.sqslistener.sqs;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class SpringCloudSQS {
	private static final Logger logger = LoggerFactory.getLogger(SpringCloudSQS.class);

	static final boolean IMPRESORA_EN_FALLO = false;
	static final String QUEUE_AL_FALLA_CONSUMIDOR = "al-fallar-consumidor.fifo";
	static final String QUEUE_AL_NO_EXISTIR_MENSAJE= "al-no-encontrar-mensajes-en-cola.fifo";
	static final String TIEMPO_DE_ESPERA_SEGUNDOS = "20";
	static final String TIEMPO_POR_DEFAULT = "30";
	static final String TIEMPO_DE_VISIBILIDAD = "VisibilityTimeout";

	@Autowired
	ClienteAWS clienteAws;

	@SqsListener(value= QUEUE_AL_FALLA_CONSUMIDOR, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
	public void recibirMensajesYErrorEnCliente(String message, @Header("SenderId") String senderId) {
		logger.info("Mensaje a procesar : {}, having SenderId: {}", message, senderId);

		if(IMPRESORA_EN_FALLO) {
			incrementarTiemporDeVisibilidadDeMensaje(QUEUE_AL_FALLA_CONSUMIDOR, TIEMPO_DE_VISIBILIDAD, TIEMPO_DE_ESPERA_SEGUNDOS);
			throw new RuntimeException("Pedro estas el error por ende va para la cola");
		}

		if(hayQueResetTiempoVisibilidad(QUEUE_AL_FALLA_CONSUMIDOR, TIEMPO_DE_VISIBILIDAD)) {
			resetTiemporDeVisibilidadDeMensaje(QUEUE_AL_FALLA_CONSUMIDOR, TIEMPO_DE_VISIBILIDAD, TIEMPO_POR_DEFAULT);
		}

		logger.info("Mensaje exitoso y  procesado : {}, having SenderId: {}", message, senderId);
	}

	@SqsListener(value= QUEUE_AL_NO_EXISTIR_MENSAJE)
	public void recibirMensaje(String message, @Header("SenderId") String senderId) {
		logger.info("Mensaje a procesar : {}, having SenderId: {}", message, senderId);

		Map<String, String> listaAtributos = clienteAws.obtenerListaAtributos(QUEUE_AL_NO_EXISTIR_MENSAJE);
		for(Entry<String, String> datos : listaAtributos.entrySet()) {
			System.err.println(datos.getKey() + " ::::" + datos.getValue());
		}
	}

	private void incrementarTiemporDeVisibilidadDeMensaje(String nombreCola, String nombreAtributo, String valorAtributo) {
		logger.info("Nombre de la Cola: {} *** Nombre Atributo: {} *** Valor a incrementar: {}", nombreCola, nombreAtributo, valorAtributo);
		clienteAws.setearValorAtributo(nombreCola, nombreAtributo, valorAtributo);
		logger.info("Valor incrementado exitosamente");
	}

	private void resetTiemporDeVisibilidadDeMensaje(String nombreCola, String nombreAtributo, String valorAtributo) {
		logger.info("Nombre de la Cola: {} *** Nombre Atributo: {} *** Valor a incrementar: {}", nombreCola, nombreAtributo, valorAtributo);
		clienteAws.setearValorAtributo(nombreCola, nombreAtributo, valorAtributo);
		logger.info("Valor Reset exitosamente");
	}


	private boolean hayQueResetTiempoVisibilidad(String nombreCola, String nombreAtributo) {
		logger.info("Nombre de la Cola: {} *** Nombre Atributo: {} ***", nombreCola, nombreAtributo);
		String tiempoVisibilidad = clienteAws.obtenerValorDeUnAtributo(nombreCola, nombreAtributo);
		logger.info("El tiempo de visibilidad actual es de {}", tiempoVisibilidad);
		return !TIEMPO_POR_DEFAULT.equals(tiempoVisibilidad);
	}
}