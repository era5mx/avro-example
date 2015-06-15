/**
 *
 */
package mx.com.extend.example;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import avro.gen.User;

import com.thoughtworks.xstream.XStream;

/**
 * @author alexander
 *
 */
public class Avro {

	private static Logger logger = LogManager.getLogger(Avro.class);

	/** Constante para cargar el path configurado */
	private static final String PATH_AVRO = "pathAvro";
	/** Constante para cargar el namefile configurado */
	private static final String NAME_FILE_AVRO = "nameFileAvro";
	/** Constante para cargar el path configurado para los XMLs */
	private static final String RESULT_XML = "pathResultXML";

	/** Multiplicador */
	private static int limit = 100000;
	private static int multiplicador = 10;

	/**
	 * Constructor
	 */
	public Avro() {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Avro avro = new Avro();
		List<User> listUsers = avro.generateUsers();
		avro.execute(listUsers);
		logger.info("Ejecucion completada");
	}

	/**
	 * @param listUsers
	 */
	private void execute(List<User> listUsers) {

		logger.info("Iniciando la ejecucion con [" + listUsers.size() + "] Users.");

		long tiempoInicio = System.currentTimeMillis();

		// Engordamos la lista
		do {
			listUsers.addAll(this.generateUsers());
			--multiplicador;
		} while (multiplicador>0);

		// Serializamos los usuarios
		this.serializing(listUsers);

		// Deserializamos los usuarios
		List<User> listUsersDeserializated = this.deserializing();

		// Se itera la lista de Users deserializados
		for (User user2 : listUsersDeserializated) {
			logger.debug(user2);
			this.generaXML(user2, multiplicador); //Genera el XML del User
		}

		terminaProceso("execute", tiempoInicio);

		logger.info("Terminada la ejecucion con [" + listUsers.size() + "] Users.");

		// Si el multiplicador no ha alcanzado el limite definido se incrementa en 10 y se repite la ejecucion
		if(multiplicador<limit) {
			multiplicador = multiplicador*10;
			execute(listUsers);
		}
	}

	/**
	 * @param user
	 */
	private void generaXML(User user, int multiplicador) {

		XStream xs = new XStream();

		// OBJECT --> XML
		String xml = xs.toXML(user);

		//Creamos el nombe del archivo XML
		String nameFileXML = UUID.randomUUID().toString().concat(".xml");

		// Crea el archivo XML
		File file = this.createFile(nameFileXML, generatePathResult(String.valueOf(multiplicador)));

		// Escribe el archivo XML
		try {
			FileWriter w = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(w);
			PrintWriter wr = new PrintWriter(bw);
			wr.write(xml);// escribimos en el archivo
			wr.close();
			bw.close();
		} catch (IOException e) {
			logger.error("Fallo la generacion del XML " + nameFileXML);
		}

	}

//	/**
//	 * @return
//	 */
//	private String generatePathResult() {
//		return generatePathResult(StringUtils.EMPTY);
//	}

	/**
	 * @return
	 */
	private String generatePathResult(String ejecucion) {
		Calendar cal = Calendar.getInstance();
		String pathResultXML = UtilProperties.getProperty(RESULT_XML)
				.concat(String.valueOf(cal.get(Calendar.YEAR)))
				.concat(String.valueOf(cal.get(Calendar.MONTH)))
				.concat(String.valueOf(cal.get(Calendar.DAY_OF_MONTH)))
				.concat(String.valueOf(cal.get(Calendar.HOUR_OF_DAY)))
				;
		return StringUtils.isNotBlank(ejecucion)?pathResultXML.concat(ejecucion).trim():pathResultXML;
	}

	/**
	 * Create some Users and set their fields.
	 * Avro objects can be created either by invoking a constructor directly or by using a builder.
	 * Additionally, builders validate the data as it set, where as objects constructed directly will not cause an error until the object is serialized.
	 * However, using constructors directly generally offers better performance, as builders create a copy of the datastructure before it is written.
	 * @return List<User>
	 */
	private List<User> generateUsers(){

		User user1 = new User();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		// Leave favorite color null

		// Alternate constructor
		User user2 = new User("Ben", 7, "red");

		// Construct via builder
		User user3 = User.newBuilder()
		             .setName("Charlie")
		             .setFavoriteColor("blue")
		             .setFavoriteNumber(null)
		             .build();

		List<User> listUsers = new ArrayList<User>();
		listUsers.add(user1);
		listUsers.add(user2);
		listUsers.add(user3);

		return listUsers;
	}

	/**
	 * Serialize our Users to disk.
	 */
	private void serializing(List<User> listUsers) {
		long tiempoInicio = System.currentTimeMillis();
		// We create a DatumWriter, which converts Java objects into an in-memory serialized format.
		// The SpecificDatumWriter class is used with generated classes and extracts the schema from the specified generated type.
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
		// We create a DataFileWriter, which writes the serialized records, as well as the schema,
		// to the file specified in the dataFileWriter.create call.
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);

		try {
			File file = createFile();
			dataFileWriter.create(((User) listUsers.get(0)).getSchema(), file);
			for (User user : listUsers) {
				// We write our users to the file via calls to the dataFileWriter.append method.
				dataFileWriter.append(user);
			}
			// When we are done writing, we close the data file.
			dataFileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("Ocurrio un error durante la serializacion");
		}
		terminaProceso("serializing", tiempoInicio);
	}

	/**
	 * Crea el File con el nombre recibido en el path recibido
	 * @return File
	 */
	private File createFile(String nameFile, String path) {
		String msgError="";
		if(StringUtils.isBlank(path)){
			msgError.concat(" [El path recibido es blanco o nulo]");
		}
		if(StringUtils.isBlank(nameFile)){
			msgError.concat(" [El nameFile recibido es blanco o nulo]");
		}

		File file = null;

		if(StringUtils.isBlank(msgError)) {
			File folder = new File(path);
			if (!folder.exists()) { // Se crea la carpeta, independientemente que exista el path completo,
				folder.mkdirs();    // si no existe crea toda la ruta necesaria
			}
			String absolutePathFile = folder.getPath().concat("\\").concat(nameFile);

			file = new File(absolutePathFile);
		}

		return file;
	}

//	/**
//	 * Crea el File con el nombre recibido
//	 * @return File
//	 */
//	private File createFile(String nameFile) {
//		return createFile(UtilProperties.getProperty(PATH_AVRO), nameFile);
//	}

	/**
	 * Crea el File
	 * @return File
	 */
	private File createFile() {
		return createFile(UtilProperties.getProperty(PATH_AVRO), UtilProperties.getProperty(NAME_FILE_AVRO));
	}

	/**
	 * Deserialize Users from disk
	 */
	private List<User> deserializing() {
		long tiempoInicio = System.currentTimeMillis();
		// We create a SpecificDatumReader, analogous to the SpecificDatumWriter we used in serialization,
		// which converts in-memory serialized items into instances of our generated class, in this case User.
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);

		//Creamos la lista de retorno
		List<User> listUser = new ArrayList<User>();

		// We pass the DatumReader and the previously created File to a DataFileReader, analogous to the DataFileWriter,
		// which reads the data file on disk.
		try {
			File file = createFile();
			@SuppressWarnings("resource")
			DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);

			User user = null;
			// Next we use the DataFileReader to iterate through the serialized Users and print the deserialized object to stdout.
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				user = dataFileReader.next(user);
				//agregamos el objeto user a la lista de retorno
				listUser.add(user);
			}
			// DataFileReader no cuenta con un metodo close()
			dataFileReader=null;
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("Ocurrio un error durante la deserializacion");
		}
		terminaProceso("deserializing", tiempoInicio);

		//retornamos la lista de objetos user
		return listUser;
	}

	/**
	 * @param nameProcess
	 * @param tiempoInicio
	 */
	private void terminaProceso(String nameProcess, long tiempoInicio) {
		long totalTiempo = System.currentTimeMillis() - tiempoInicio;
		logger.info("El tiempo del proceso " + nameProcess + " es : [" + totalTiempo + " miliseg.]");
	}

}
