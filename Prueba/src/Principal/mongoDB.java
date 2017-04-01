package Principal;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.MarshalException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
 
public class mongoDB {
	public void InsertarMongo(String ruta) throws IOException, JSONException{
		
		try {
			MongoClient mongoClient = null;
			  mongoClient = new MongoClient( "localhost" , 27017 );
			  DB db = mongoClient.getDB( "datos" );
			  DBCollection collection = db.getCollection("contratos");
			String sDirectorio = ruta;
			File f = new File(sDirectorio);
			File[] ficheros = f.listFiles();
			for (int x=0;x<ficheros.length;x++){
			  
			  String content = readFile(sDirectorio+ficheros[x].getName(), StandardCharsets.UTF_8);
			
			  JSONArray jsonarr = new JSONArray(content);

			  	System.out.println(jsonarr.length());
			    for(int i = 0; i < jsonarr.length(); i++){
			    	
			    JSONObject jsonobj = jsonarr.getJSONObject(i);

			 
			    DBObject dbObject = (DBObject)JSON.parse(jsonobj.toString());
			    collection.insert(dbObject);
			   
			    }
			  
			 

			  	
			  
			}
			

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MarshalException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertarMongoTokens(String Tokens) throws IOException, JSONException{
		
		
		MongoClient mongoClient = null;
		mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "datos" );
		DBCollection collection = db.getCollection("Tokens");
		BasicDBObject doc = new BasicDBObject();
		doc.put("Numero_Token", Tokens);
		collection.insert(doc);
		

}

	static String readFile(String path, Charset encoding) 
			  throws IOException 
			{
			  byte[] encoded = Files.readAllBytes(Paths.get(path));
			  return new String(encoded, encoding);
	}

	
}
