/**
 *
 */
package mx.com.extend.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import avro.gen.User;

/**
 * @author alexander
 *
 */
public class Avro {

	private static final String NAME_FILE_AVRO = "nameFileAvro";
	private static final String PATH_AVRO = "pathAvro";

	/**
	 *
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
		int i = 1000000;
		do {
			listUsers.addAll(avro.generateUsers());
			--i;
		} while (i>0);

		avro.serializing(listUsers);
		avro.deserializing();
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
	 * Crea el File
	 * @return File
	 */
	private File createFile() {
		File folder = new File(UtilProperties.getProperty(PATH_AVRO));
		if (!folder.exists()) {
			folder.mkdirs(); // esto crea la carpeta java, independientemente que exista el path completo, si no existe crea toda la ruta necesaria
		}
		String absolutePathFile = folder.getPath().concat("\\").concat(UtilProperties.getProperty(NAME_FILE_AVRO));
		System.out.println(absolutePathFile);

		File file = new File(absolutePathFile);
		return file;
	}

	/**
	 * Deserialize Users from disk
	 */
	private void deserializing() {
		long tiempoInicio = System.currentTimeMillis();
		// We create a SpecificDatumReader, analogous to the SpecificDatumWriter we used in serialization, which converts in-memory serialized items into instances of our generated class, in this case User.
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
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
				System.out.println(user);
			}
			// DataFileReader no cuenta con un metodo close()
			dataFileReader=null;
		} catch (IOException e) {
			e.printStackTrace();
		}
		terminaProceso("deserializing", tiempoInicio);
	}

	private void terminaProceso(String nameProcess, long tiempoInicio) {
		long totalTiempo = System.currentTimeMillis() - tiempoInicio;
		System.out.println("El tiempo del proceso " + nameProcess + " es :" + totalTiempo + " miliseg");
	}

}
