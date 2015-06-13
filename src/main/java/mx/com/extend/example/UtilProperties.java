/**
 *
 */
package mx.com.extend.example;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @author alexander
 *
 */
public class UtilProperties {

	/**
	 * Path del archivo properties
	 */
	private static final String AVRO_PROPERTIES = "C:\\ws\\avro\\avro-example\\src\\main\\resources\\avro.properties";

	/**
	 *
	 */
	public UtilProperties() {
	}

	public static String getProperty(String nameProperty) {

		/**Creamos un Objeto de tipo Properties*/
	    Properties propiedades = new Properties();

	    /**Cargamos el archivo desde la ruta especificada*/
	    try {
			propiedades.load(new FileInputStream(AVRO_PROPERTIES));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	    /**Obtenemos los parametros definidos en el archivo*/
	    String property = propiedades.getProperty(nameProperty);

	    return property;

	}

}
