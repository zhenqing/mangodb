
package mangodb;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
public class MongodbTest {
	MongoClient mongoClient;
	DB db;
	DBCollection col;
	public MongodbTest() {
		super();
		init();
	}
	public  void insert(BasicDBObject doc ){
		col.insert(doc);
	}
	public  void delete(DBObject jo){
		col.remove(jo);
	}
	public  void update(DBObject query, DBObject update){
		col.updateMulti(query,update);
	}
	public  void get(){
		 BasicDBObject keys = new BasicDBObject();
		 keys.put("id", "1");
		 DBCursor  cursor = col.find(keys);
		 try {
			   while(cursor.hasNext()) {
			       System.out.println(cursor.next());
			   }
			} finally {
			   cursor.close();
			}
	}
	public void getAll(){
		DBCursor cursor = col.find();
		try {
		   while(cursor.hasNext()) {
		       System.out.println(cursor.next());
		   }
		} finally {
		   cursor.close();
		}
	}
	public long getCount(){
		long c = col.getCount();
		System.out.println(c);
		return c;
	}
	public void createTable(){
		BasicDBObject student = new BasicDBObject("id", "1").
                append("name", "Lucy").
                append("age", 17).
                append("gender", 'F');
		db.createCollection("student",student);
	}
	public void dropTable(){
		col.drop();
	}
	public void init(){
					
		try {
			mongoClient =  new MongoClient("localhost",27017);
			db = mongoClient.getDB( "test" );
			boolean auth = db.authenticate("root", "root".toCharArray());
			if(auth){
				col = db.getCollection("student");
				Set<String> colls = db.getCollectionNames();
//				for (String s : colls) {
//				    System.out.println(s);
//				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void main(String[] args)  {
		// TODO Auto-generated method stub
		// To directly connect to a single MongoDB server (note that this will not auto-discover the primary even
		MongodbTest mongo = new MongodbTest();	
//		BasicDBObject student = new BasicDBObject("id", "1").
//                append("name", "Lucy").
//                append("age", 17).
//                append("gender", 'F');
//		BasicDBObject student1 = new BasicDBObject("id", "3").
//                append("name", "Tom").
//                append("age", 18).
//                append("gender", 'M');
//		mongo.insert(student);
//		mongo.getAll();
//		mongo.insert(student1);
//		//mongo.get();
//		BasicDBObject query = new BasicDBObject();
//		query.put("id", "1");
//		BasicDBObject update = new BasicDBObject();
//		//update.put("name", "Lily");
//		update.append("$set", 
//				new BasicDBObject().append("name", "Jenny"));
//		mongo.update(query,update);
//		mongo.getAll();
//		BasicDBObject delete = new BasicDBObject();
//		delete.put("name", "Tom");
//		mongo.delete(delete);
//		mongo.getAll();
		mongo.dropTable();
	}

}
