import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.MongoException;
import com.mongodb.BasicDBList;
import org.bson.Document;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.util.Arrays.asList;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class GetActiveUsersLastWeek {

	public static void main(String[] args) {
		
		Date date = new Date();
		String current_datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
       	java.util.Date event_time_from = null;
       	java.util.Date event_time_till = null;
       	//java.util.Date submit_time_m = null;
       	//java.util.Date start_time_m = null;
    	String event_time_1 = "2015-09-01 00:00:00";
       	try {
       		event_time_from = format.parse(event_time_1);
         	} catch (ParseException e) {
         	  System.err.println("Could not parse date: " + event_time_1);
         	}
    	String event_time_2 = "2015-09-07 23:59:59";
       	try {
       		event_time_till = format.parse(event_time_2);
         	} catch (ParseException e) {
         	  System.err.println("Could not parse date: " + event_time_2);
         	}
		
        /*** Disable mongo logging to console ***/
        Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
        mongoLogger.setLevel(Level.SEVERE);
        /*** ***/
    	MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
    	MongoDatabase database = mongoClient.getDatabase("mydb");
    	MongoCollection<Document> collection = database.getCollection("test");
    	

        try {
           	//FindIterable<Document> iterable = database.getCollection("test").find();

/*        	db.test.aggregate( 
        			  [
        			    { 
        			      $match: 
        			            {
        			              event_time: { $gte: ISODate("2015-09-01T00:00:00.000Z"), $lt: ISODate("2015-09-07T23:59:59.000Z") }
        			            }
        			    },
        			    {
        			      $project: 
        			      { 
        			        user_name : 1, event_time : 1, start_time : 1, dateDifference: 
        			        { 
        			          $divide: 
        			          [ 
        			            { 
        			              $subtract: 
        			              [ "$event_time" , "$start_time" ] 
        			            } , 1000 
        			          ] 
        			        } 
        			      } 
        			    }, 
        			    { 
        			      $group: 
        			            {
        			            _id: "$user_name",             
        			            total : { 
        			                $sum : "$dateDifference"
        			                    }
        			            }
        			    } 
        			  ] 
        			)*/
        	
 
        	//AggregateIterable<Document> iterable = db.getCollection("restaurants").aggregate(asList(
        	//        new Document("$match", new Document("borough", "Queens").append("cuisine", "Brazilian")),
        	//        new Document("$group", new Document("_id", "$address.zipcode").append("count", new Document("$sum", 1)))));
        	BasicDBList substract_args  = new BasicDBList();
        	substract_args.add("$event_time");
        	substract_args.add("$start_time");       	
        	
        	BasicDBList divide_args  = new BasicDBList();
        	divide_args.add(new Document("$subtract", substract_args));
        	divide_args.add(1000);

        	
        	AggregateIterable<Document> iterable = database.getCollection("test").aggregate(asList
        			(new Document
        					("$match", new Document
        							("event_time", new Document
        									(new Document("$gt", event_time_from)
        									).append("$lt", event_time_till)
        							)
        					),
        			new Document
        					("$project", new Document
        							("user_name", 1).append("event_time", 1).append("start_time", 1).append
        								("dateDifference", new Document
        									("$divide", divide_args
        									)
        								)
        					),
        			new Document
        					("$group", new Document
        							("_id", "$user_name").append
        								("total", new Document
        									("$sum", "$dateDifference")
        								)
        					)
        			)
        			
        		);
        	        //new Document("$group", new Document("_id", "$address.zipcode").append("count", new Document("$sum", 1)))));
        	//event_time: { $gte: ISODate("2015-09-01T00:00:00.000Z"), $lt: ISODate("2015-09-07T23:59:59.000Z") }
        	        //collection.find(and(gt("i", 50), lte("i", 100))).forEach(printBlock);

           	
           	
           	for (Document document : iterable) {
           	    System.out.println(document);
           	}
        } catch (  MongoException e) {
    		System.err.println("Duplicate: " + e);
    	}
        
        finally {
            try {
                if (mongoClient != null) {
                	mongoClient.close();
                }

            } catch (MongoException ex) {
                Logger lgr = Logger.getLogger(Collector.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }

	}

  }
}
