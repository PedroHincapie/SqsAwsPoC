package co.com.poc.sqslistener.sqs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

@Component
public class ClienteAWSSDK implements ClienteAWS{

	final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

	@Override
	public Map<String, String> obtenerListaAtributos(String nombreCola) {
		//Consultar la URL de la cola que se ajustara
		final String queueUrl= sqs.getQueueUrl(nombreCola).getQueueUrl();

		//Contruir lista con el atributo de la cola a necesitar
		List<String> attributeNames = new ArrayList<>();
		attributeNames.add("All");

		//Crear Objeto del request con el objeto a solicitar
		GetQueueAttributesRequest requestAtri = new GetQueueAttributesRequest(queueUrl);
		requestAtri.setAttributeNames(attributeNames);

		return sqs.getQueueAttributes(requestAtri).getAttributes();
	}

	@Override
	public void setearValorAtributo(String nombreCola, String nombreAtributo, String valorAtributo) {
		//Consultar la URL de la cola que se ajustara
		final String queueUrl= sqs.getQueueUrl(nombreCola).getQueueUrl();

		//Crear objeto con el valor de la visibilidad
		final SetQueueAttributesRequest request = new SetQueueAttributesRequest()
				.withQueueUrl(queueUrl)
				.addAttributesEntry(nombreAtributo, valorAtributo);

		//Seterar el valor en aws
		sqs.setQueueAttributes(request);
	}

	@Override
	public String obtenerValorDeUnAtributo(String nombreCola, String nombreAtributo) {
		//Consultar la URL de la cola que se ajustara
		final String queueUrl= sqs.getQueueUrl(nombreCola).getQueueUrl();

		//Contruir lista con el atributo de la cola a necesitar
		List<String> attributeNames = new ArrayList<>();
		attributeNames.add("All");

		//Crear Objeto del request con el objeto a solicitar
		GetQueueAttributesRequest requestAtri = new GetQueueAttributesRequest(queueUrl);
		requestAtri.setAttributeNames(attributeNames);

		return sqs.getQueueAttributes(requestAtri).getAttributes().get(nombreAtributo); 
	}

}
