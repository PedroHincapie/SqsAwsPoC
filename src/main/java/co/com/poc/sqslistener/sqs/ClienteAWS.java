package co.com.poc.sqslistener.sqs;

import java.util.Map;

public interface ClienteAWS {

	//Obtener la lista de atributos
	Map<String, String>obtenerListaAtributos(String nombreCola);

	//Seterar valor a un atributo de la lista
	void setearValorAtributo(String nombreCola, String nombreAtributo, String valorAtributo);

	//Obtener el valor de un atributo
	String obtenerValorDeUnAtributo(String nombreCola, String nombreAtributo);
}
