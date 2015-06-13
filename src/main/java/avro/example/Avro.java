/**
 *
 */
package avro.example;

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

import com.thoughtworks.xstream.XStream;

import avro.gen.User;

/**
 * @author alexander
 *
 */
public class Avro {

	/** Constante para cargar el path configurado */
	private static final String PATH_AVRO = "pathAvro";
	/** Constante para cargar el namefile configurado */
	private static final String NAME_FILE_AVRO = "nameFileAvro";
	/** Constante para cargar el path configurado para los XMLs */
	private static final String RESULT_XML = "pathResultXML";
	/** Multiplicador */
	//private static int multiplicador = 1000000;
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

		//Engordamos la lista a 3 millones de registros
		do {
			listUsers.addAll(avro.generateUsers());
			--multiplicador;
		} while (multiplicador>0);

		//Test generacion del nombre del archivo
		//System.out.println(UUID.randomUUID().toString().concat(".xml"));

		//Serializamos los usuarios
		avro.serializing(listUsers);

		//Deserializamos los usuarios
		List<User> listUsersDeserializated = avro.deserializing();


		for (User user2 : listUsersDeserializated) {

			//Mostramos los usuarios en consola
			System.out.println(user2);

			//Genera el XML del User
			avro.generaXML(user2);

		}

	}

	/**
	 * @param user
	 */
	private void generaXML(User user) {

		XStream xs = new XStream();

		// OBJECT --> XML
		String xml = xs.toXML(user);



		// Crea los archivo xml
		File file = this.createFile(generatePathResult(),
						UUID.randomUUID().toString().concat(".xml"));

		// Escritura
		try {
			FileWriter w = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(w);
			PrintWriter wr = new PrintWriter(bw);
			wr.write(xml);// escribimos en el archivo
			wr.close();
			bw.close();
		} catch (IOException e) {
		}

	}

	/**
	 * @return
	 */
	private String generatePathResult() {
		String pathResultXML = UtilProperties.getProperty(RESULT_XML)
				.concat(String.valueOf(Calendar.YEAR))
				.concat(String.valueOf(Calendar.MONTH))
				.concat(String.valueOf(Calendar.DAY_OF_MONTH)
				.concat(String.valueOf(Calendar.HOUR_OF_DAY)
				.concat(String.valueOf(Calendar.MINUTE)
				.concat(String.valueOf(Calendar.MILLISECOND)))));

		System.out.println(pathResultXML);
		return pathResultXML;
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
		// We create a DataFileWriter, which writes the serialized records, as well as the schema, to the file specified in the dataFileWriter.create call.
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
		}
		terminaProceso("serializing", tiempoInicio);
	}

	/**
	 * Crea el File con el nombre recibido
	 * @return File
	 */
	private File createFile(String path, String nameFile) {
		String msgError="";
		if(StringUtils.isBlank(path)){
			msgError.concat(" ").concat("[El path recibido es blanco o nulo]");
		}
		if(StringUtils.isBlank(nameFile)){
			msgError.concat(" ").concat("[El nameFile recibido es blanco o nulo]");
		}

		File file = null;

		if(StringUtils.isBlank(msgError)) {
			File folder = new File(path);
			if (!folder.exists()) {
				folder.mkdirs(); // esto crea la carpeta java, independientemente que exista el path completo, si no existe crea toda la ruta necesaria
			}
			String absolutePathFile = folder.getPath().concat("\\").concat(nameFile);
			System.out.println(absolutePathFile);

			file = new File(absolutePathFile);
		}

		return file;
	}

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
		// We create a SpecificDatumReader, analogous to the SpecificDatumWriter we used in serialization, which converts in-memory serialized items into instances of our generated class, in this case User.
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);

		//Creamos la lista de retorno
		List<User> listUser = new ArrayList<User>();

		// We pass the DatumReader and the previously created File to a DataFileReader, analogous to the DataFileWriter, which reads the data file on disk.
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
		System.out.println("El tiempo del proceso " + nameProcess + " es :" + totalTiempo + " miliseg");
	}

}
