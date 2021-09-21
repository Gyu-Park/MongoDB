package com.mongodb;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.in;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import static com.mongodb.client.model.Updates.set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
/**
 * athor @gyuseok
 *
 */
public class MongoMain 
{
    public static void main( String[] args ) {
    try{
        // connect mongoDB by using ConnectionString, MongoClientSettings, and MongoClient
        ConnectionString connectionString = new ConnectionString("mongodb+srv://admin:rbtjr133@cluster0.0k7o4.mongodb.net/myFirstDatabase?retryWrites=true&w=majority");
        MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(connectionString)
        .retryWrites(true)
        .build();
        MongoClient mongoClient = MongoClients.create(settings);
        
        // Logger to set level (can hide some inforemation)
        Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);
        
        if (mongoClient.getDatabase("MongoDB").getCollection("cookies").equals(false)) {
        mongoClient.getDatabase("MongoDB").createCollection("cookies");
        }
        MongoCollection<Document> mongoCollection = mongoClient.getDatabase("MongoDB").getCollection("cookies");
        
        // call createDocuments and deleteDocuments method
        // deleteDocuments(mongoCollection);
        // createDocuments(mongoCollection);
        // updateDocuments(mongoCollection);
        findDocuments(mongoCollection);
        
    }catch(Exception e){
        System.out.println(e);
    }
}

/** example of inserting data into a database
// create or get database
MongoDatabase db = mongoClient.getDatabase("MongoDB");
// get collection
MongoCollection collection = db.getCollection("test");
// create document
Document document = new Document("name", "Peter");
// append data
document.append("Sex", "male");
document.append("Age", "28");
// insert data into the database
collection.insertOne(document);
**/

private static void findDocuments(final MongoCollection<Document> mongoCollection) {
    System.out.println("Find Operation");
    List<Document> lowCaloriesCookies = mongoCollection.find(Filters.gte("calories", 500)).into(new ArrayList<Document>());  // lte is "less than," and gte is "greater than."
    lowCaloriesCookies.forEach(print -> System.out.println(print.toJson()));
}

private static void updateDocuments(final MongoCollection<Document> mongoCollection) {
    System.out.println("Update Operation");
    final Random random = new Random();
    // mongoCollection.updateMany(new Document(), set("calories", random.nextInt(1000))); // using Updates.set method
    List<Document> cookiesList = mongoCollection.find().into(new ArrayList<Document>());
    cookiesList.forEach(cookie -> {
        Object id = cookie.get("_id");
        Document filter = new Document("_id", id); // "_id" is like a primary key in a database
        Bson update = set("calories", random.nextInt(1000));
        FindOneAndUpdateOptions foauOptions = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
        Document c = mongoCollection.findOneAndUpdate(filter, update, foauOptions);
        System.out.println(c.toJson());
    });
}

private static void deleteDocuments(final MongoCollection<Document> mongoCollection) {
    mongoCollection.deleteMany(new Document()); // delete all the data in the database
    // mongoCollection.deleteMany(new Document("color", "orange")); // delete specific data in the database // if there's no those documents, this statement does not delete anything.
    // mongoCollection.deleteMany(in("color", Arrays.asList("red", "pink"))); // using Filters.in method
}

private static void createDocuments(final MongoCollection<Document> mongoCollection) {
    List<Document> cookiesList = new ArrayList<>();
    List<String> ingredients = Arrays.asList("flour", "eggs", "butter", "sugar", "red food coloring");
    
    for (int i = 0; i < 10; i++) {
        cookiesList.add(new Document("cookie_id", i).append("color", "red").append("ingredients", ingredients));
    }
    mongoCollection.insertMany(cookiesList);
}

private static void printDatabases(MongoClient mongoClient) {
    /** a way to list all the databases
    MongoIterable<String> str = mongoClient.listDatabaseNames();
    MongoCursor<String> cursor = str.cursor();
    while(cursor.hasNext()) {
        System.out.println(cursor.next());
    }**/
    // an easier way to list all the databases
    List<String> databases = mongoClient.listDatabaseNames().into(new ArrayList<String>());
    List<Document> dbDoc = mongoClient.listDatabases().into(new ArrayList<Document>());
    System.out.println(databases);
    databases.forEach(document -> System.out.println(document.toString()));
    databases.forEach(System.out::println);
    System.out.println(dbDoc);
    dbDoc.forEach(document -> System.out.println(document.toJson()));
    dbDoc.forEach(System.out::println);
}

}