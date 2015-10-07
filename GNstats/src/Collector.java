import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Date;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.MongoException;
import org.bson.Document;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;



public class Collector {

	public static void main(String[] args) {
        
		Date date = new Date();
		String current_datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        String url = "jdbc:mysql://localhost:3306/lsf";
        String user = "root";
        String password = "";
        
        int duplicate_counter = 0;
        int insert_counter = 0;
    	
        /*** Disable mongo logging to console ***/
        Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
        mongoLogger.setLevel(Level.SEVERE);
        /*** ***/
    	MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
    	MongoDatabase database = mongoClient.getDatabase("mydb");
    	MongoCollection<Document> collection = database.getCollection("test");


        try {
            conn = DriverManager.getConnection(url, user, password);
            st = conn.createStatement();
            rs = st.executeQuery("SELECT * FROM jobs_short WHERE queue = 'gnh' ");
            
           	DateFormat format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
           	java.util.Date event_time_m = null;
           	java.util.Date submit_time_m = null;
           	java.util.Date start_time_m = null;
           	
            try
            {
                //database.getCollection("test").createIndex(new Document(new Document("event_time", 1).append("job_id", 1)).append("unique", true));
            	database.getCollection("test").createIndex(new Document("event_time", 1).append("job_id", 1), new IndexOptions().unique(true));
            } catch (  MongoException e) {
            	System.out.println("Exceprion: create index");
            }

            while (rs.next()){ 
            	String event_time = rs.getString("event_time");
               	try {
               		event_time_m = format.parse(event_time);
                 	} catch (ParseException e) {
                 	  System.err.println("Could not parse date: " + event_time);
                 	}
            	String job_id = rs.getString("job_id");
            	String user_id = rs.getString("user_id");
            	String num_processors = rs.getString("num_processors");
            	String submit_time = rs.getString("submit_time");
               	try {
               		submit_time_m = format.parse(submit_time);
                 	} catch (ParseException e) {
                 	  System.err.println("Could not parse date: " + submit_time);
                 	}
            	String start_time = rs.getString("start_time");
               	try {
               		start_time_m = format.parse(start_time);
                 	} catch (ParseException e) {
                 	  System.err.println("Could not parse date: " + start_time);
                 	}
            	String user_name = rs.getString("user_name");
            	String queue = rs.getString("queue");
            	String res_req = rs.getString("res_req");
            	String depend_cond = rs.getString("depend_cond");
            	String from_host = rs.getString("from_host");
            	String cwd = rs.getString("cwd");
            	String num_exec_hosts = rs.getString("num_exec_hosts");
            	String exec_host = rs.getString("exec_host");
            	String job_name = rs.getString("job_name");
            	String cpu_time = rs.getString("cpu_time");
            	String ru_utime = rs.getString("ru_utime");
            	String ru_stime = rs.getString("ru_stime");
            	String ru_inblock = rs.getString("ru_inblock");
            	String ru_oublock = rs.getString("ru_oublock");
            	String project_name = rs.getString("project_name");
            	String exit_status = rs.getString("exit_status");
            	String max_num_processors = rs.getString("max_num_processors");
            	String max_r_mem = rs.getString("max_r_mem");
            	String max_r_swap = rs.getString("max_r_swap");
            	String app = rs.getString("app");
            	String job_description = rs.getString("job_description");
            	String custom_app = rs.getString("custom_app");
            	String custom_iteration = rs.getString("custom_iteration");
            	
            	/***** Mongodb *****/
            	try {
               	database.getCollection("test").insertOne(
               	        new Document()
			               	.append("event_time", event_time_m)
			               	.append("job_id", job_id)
			               	.append("user_id", user_id)
			               	.append("num_processors", num_processors)
			               	.append("submit_time", submit_time_m)
			               	.append("start_time", start_time_m)
			               	.append("user_name", user_name)
			               	.append("queue", queue)
			               	.append("res_req", res_req)
			               	.append("depend_cond", depend_cond)
			               	.append("from_host", from_host)
			               	.append("cwd", cwd)
			               	.append("num_exec_hosts", num_exec_hosts)
			               	.append("exec_host", exec_host)
			               	.append("job_name", job_name)
			               	.append("cpu_time", cpu_time)
			               	.append("ru_utime", ru_utime)
			               	.append("ru_stime", ru_stime)
			               	.append("ru_inblock", ru_inblock)
			               	.append("ru_oublock", ru_oublock)
			               	.append("project_name", project_name)
			               	.append("exit_status", exit_status)
			               	.append("max_num_processors", max_num_processors)
			               	.append("max_r_mem", max_r_mem)
			               	.append("max_r_swap", max_r_swap)
			               	.append("app", app)
			               	.append("job_description", job_description)
			               	.append("custom_app", custom_app)
			               	.append("custom_iteration", custom_iteration));
               	 insert_counter = insert_counter + 1;
            	}  catch (  MongoException e) {
            		//System.err.println("Duplicate: " + e);
            		duplicate_counter = duplicate_counter + 1;
            	}
            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Collector.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
        
        //database.getCollection("test").createIndex(new Document(new Document("event_time", 1).append("job_id", 1)).append("unique", true));
        //try {
        //   	FindIterable<Document> iterable = database.getCollection("test").find();
        //   	for (Document document : iterable) {
        //   	    System.out.println(document);
        //   	}
        //} 

      finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (conn != null) {
                    conn.close();
                }
                if (mongoClient != null) {
                	mongoClient.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(Collector.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
            
        }

        System.out.println(current_datetime + " Inserted: " + insert_counter + " Duplicates: " + duplicate_counter);

	}
}