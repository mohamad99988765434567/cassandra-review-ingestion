package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Set;
import java.util.concurrent.Semaphore;

import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import bigdatacourse.hw2.HW2API;


public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=			"na";
	public static final	double		NOT_AVAILABLE_DOUBLE_VALUE 	=		-1.0;
	
	// CQL stuff
	//TODO: add here create table and query designs 
	private static final String		TABLE_ITEMS	 = "items";
	private static final String		TABLE_REVIEW_BY_ITEM = "reviewByItem";
	private static final String		TABLE_REVIEW_BY_USER = "reviewByUser";
	
	// ITEMS TABLE QUERIES
	private static final String		CQL_CREATE_TABLE_ITEMS = 
			"CREATE TABLE " + TABLE_ITEMS	+" (" 	+ 
				"asin TEXT,"			+
				"title TEXT,"			+
				"image TEXT,"			+
				"categories SET<text>,"	+
				"description TEXT,"		+
				"PRIMARY KEY (asin)"	+
			") ";
	
	private static final String		CQL_CREATE_TABLE_REVIEW_BY_ITEM = 
			"CREATE TABLE " + TABLE_REVIEW_BY_ITEM	+" (" 	+ 
					"asin TEXT,"			+
					"reviewer_id TEXT,"		+
					"time TIMESTAMP,"		+
					"reviewer_name TEXT,"	+
					"rating DOUBLE, "			+
					"summary TEXT, "		+
					"review_text TEXT, "	+
					"PRIMARY KEY ((asin), time, reviewer_id)"	+
				") " +
				"WITH CLUSTERING ORDER BY (time DESC, reviewer_id ASC)";
	private static final String		CQL_CREATE_TABLE_REVIEW_BY_USER = 
			"CREATE TABLE " + TABLE_REVIEW_BY_USER	+" (" 	+ 
					"asin TEXT,"			+
					"reviewer_id TEXT,"		+
					"time TIMESTAMP,"		+
					"reviewer_name TEXT,"	+
					"rating DOUBLE, "			+
					"summary TEXT, "		+
					"review_text TEXT, "	+
					"PRIMARY KEY ((reviewer_id), time, asin)"	+
				") " +
				"WITH CLUSTERING ORDER BY (time DESC, asin ASC)";

	private static final String		CQL_ITEMS_INSERT = 
			"INSERT INTO " + TABLE_ITEMS + "(asin, title, image, categories, description) VALUES(?, ?, ?, ?, ?)";
	private static final String		CQL_ITEMS_SELECT = 
			"SELECT * FROM " + TABLE_ITEMS + " WHERE asin = ?";

	private static final String		CQL_REVIEW_BY_ITEM_INSERT = 
			"INSERT INTO " + TABLE_REVIEW_BY_ITEM + "(asin, reviewer_id, time, reviewer_name, rating, summary, review_text) VALUES(?, ?, ?, ?, ?, ?, ?)";
	private static final String		CQL_REVIEW_BY_ITEM_SELECT = 
			"SELECT * FROM " + TABLE_REVIEW_BY_ITEM + " WHERE asin = ?";
	
	private static final String		CQL_REVIEW_BY_USER_INSERT = 
			"INSERT INTO " + TABLE_REVIEW_BY_USER + "(reviewer_id, time, asin, reviewer_name, rating, summary, review_text) VALUES(?, ?, ?, ?, ?, ?, ?)";
	private static final String		CQL_REVIEW_BY_USER_SELECT = 
			"SELECT * FROM " + TABLE_REVIEW_BY_USER + " WHERE reviewer_id = ?";
	
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	PreparedStatement pstmtItemsAdd;
	PreparedStatement pstmtItemsSelect;
	PreparedStatement pstmtreviewByItemAdd;
	PreparedStatement pstmtreviewByItemSelect;
	PreparedStatement pstmtreviewByUserAdd;
	PreparedStatement pstmtreviewByUserSelect;
	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
	}


	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	
	
	@Override
	public void createTables() {
		session.execute(CQL_CREATE_TABLE_ITEMS);
		System.out.println("created table: " + TABLE_ITEMS);
		session.execute(CQL_CREATE_TABLE_REVIEW_BY_ITEM);
		System.out.println("created table: " + TABLE_REVIEW_BY_ITEM);
		session.execute(CQL_CREATE_TABLE_REVIEW_BY_USER);
		System.out.println("created table: " + TABLE_REVIEW_BY_USER);
	}

	@Override
	public void initialize() {
		pstmtItemsAdd = session.prepare(CQL_ITEMS_INSERT);
		pstmtItemsSelect = session.prepare(CQL_ITEMS_SELECT);
		
		pstmtreviewByItemAdd = session.prepare(CQL_REVIEW_BY_ITEM_INSERT);
		pstmtreviewByItemSelect = session.prepare(CQL_REVIEW_BY_ITEM_SELECT);
		
		pstmtreviewByUserAdd = session.prepare(CQL_REVIEW_BY_USER_INSERT);
		pstmtreviewByUserSelect = session.prepare(CQL_REVIEW_BY_USER_SELECT);
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {

		String line;
		int maxThreads	= 100;

		//	creating the thread factors
		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);		
        BufferedReader br = new BufferedReader(new FileReader(pathItemsFile));
        try {
            while ((line = br.readLine()) != null) {
            	JSONObject json = new JSONObject(line);
            	executor.execute(new Runnable() {
    				@Override
    				public void run() {
    					insertItem(session, pstmtItemsAdd, json);
    				}
    			});
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);
        } finally {
            br.close();
        }
		
	}


	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
	    final int MAX_THREADS = 250;  // Per guideline - prevents exceeding AstraDB limits
	    final int MAX_CONCURRENT_REQUESTS = 4000; // AstraDB limit
	    final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_REQUESTS);

	    ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);

	    try (BufferedReader br = new BufferedReader(new FileReader(pathReviewsFile))) {
	        String line;
	        while ((line = br.readLine()) != null) {
	            JSONObject json = new JSONObject(line);
	            semaphore.acquire();  // **⬅️ Throttles requests to avoid rate limit**

	            executor.submit(() -> {
	                try {
	                    insertReview(session, pstmtreviewByItemAdd, pstmtreviewByUserAdd, json);
	                } finally {
	                    semaphore.release(); // **⬅️ Frees up a slot after execution**
	                }
	            });
	        }
	    }

	    executor.shutdown();
	    executor.awaitTermination(30, TimeUnit.MINUTES);
	}


	public static void insertReview(CqlSession session, PreparedStatement pstmtByItem, PreparedStatement pstmtByUser, JSONObject json) {
	    long ts = json.getLong("unixReviewTime");
	    String asin = json.getString("asin");
	    String reviewerID = json.getString("reviewerID");
	    String reviewerName = json.optString("reviewerName", NOT_AVAILABLE_VALUE);
	    double rating = json.optDouble("overall", NOT_AVAILABLE_DOUBLE_VALUE);
	    String summary = json.optString("summary", NOT_AVAILABLE_VALUE);
	    String reviewText = json.optString("reviewText", NOT_AVAILABLE_VALUE);

	    BoundStatement bstmtByItem = pstmtByItem.bind()
	            .setInstant("time", Instant.ofEpochSecond(ts))
	            .setString("asin", asin)
	            .setString("reviewer_id", reviewerID)
	            .setString("reviewer_name", reviewerName)
	            .setDouble("rating", rating)
	            .setString("summary", summary)
	            .setString("review_text", reviewText);

	    BoundStatement bstmtByUser = pstmtByUser.bind()
	            .setInstant("time", Instant.ofEpochSecond(ts))
	            .setString("asin", asin)
	            .setString("reviewer_id", reviewerID)
	            .setString("reviewer_name", reviewerName)
	            .setDouble("rating", rating)
	            .setString("summary", summary)
	            .setString("review_text", reviewText);

	    BatchStatement batch = BatchStatement.builder(BatchType.UNLOGGED)
	            .addStatement(bstmtByItem)
	            .addStatement(bstmtByUser)
	            .build();

	    session.execute(batch);
	}


	@Override
	public String item(String asin) {
	    BoundStatement bstmt = pstmtItemsSelect.bind().setString("asin", asin);
	    ResultSet rs = session.execute(bstmt);
	    Row row = rs.one();
	    if (row != null) {
	        Set<String> categories = new TreeSet<>(row.getSet("categories", String.class)); //this line makes sure that categories are sorted
	        return formatItem(
	            row.getString("asin"),
	            row.getString("title"),
	            row.getString("image"),
	            categories,
	            row.getString("description")
	        );
	    } else {
	        return "Item not exists";
	    }
	}

	
	@Override
	public Iterable<String> userReviews(String reviewerID) {
	    BoundStatement bstmt = pstmtreviewByUserSelect.bind().setString("reviewer_id", reviewerID);
	    ResultSet rs = session.execute(bstmt);
	    List<String> reviews = new ArrayList<>();
	    Row row = rs.one();
	    while (row != null) {
	        reviews.add(formatReview(
	            row.getInstant("time"),
	            row.getString("asin"),
	            row.getString("reviewer_id"),
	            row.getString("reviewer_name"),
	            (int) row.getDouble("rating"),
	            row.getString("summary"),
	            row.getString("review_text")
	        ));
	        row = rs.one();
	    }
	    return reviews;
	}


	@Override
	public Iterable<String> itemReviews(String asin) {
	    BoundStatement bstmt = pstmtreviewByItemSelect.bind().setString("asin", asin);
	    ResultSet rs = session.execute(bstmt);
	    List<String> reviews = new ArrayList<>();
	    Row row = rs.one();
	    while (row != null) {
	        reviews.add(formatReview(
	            row.getInstant("time"),
	            row.getString("asin"),
	            row.getString("reviewer_id"),
	            row.getString("reviewer_name"),
	            (int) row.getDouble("rating"),
	            row.getString("summary"),
	            row.getString("review_text")
	        ));
	        row = rs.one();
	    }
	    return reviews;
	}

	
	// Formatting methods, do not change!
		private String formatItem(String asin, String title, String imageUrl, Set<String> categories, String description) {
			String itemDesc = "";
			itemDesc += "asin: " + asin + "\n";
			itemDesc += "title: " + title + "\n";
			itemDesc += "image: " + imageUrl + "\n";
			itemDesc += "categories: " + categories.toString() + "\n";
			itemDesc += "description: " + description + "\n";
			return itemDesc;
		}

		private String formatReview(Instant time, String asin, String reviewerId, String reviewerName, Integer rating, String summary, String reviewText) {
			String reviewDesc = 
				"time: " + time + 
				", asin: " 	+ asin 	+
				", reviewerID: " 	+ reviewerId +
				", reviewerName: " 	+ reviewerName 	+
				", rating: " 		+ rating	+ 
				", summary: " 		+ summary +
				", reviewText: " 	+ reviewText + "\n";
			return reviewDesc;
		}

	// --------------- NEW FUNCTIONS -----------------------
		public static void insertItem(CqlSession session, PreparedStatement pstmt, JSONObject json) {
		    
		    String asin = json.getString("asin");
		    String title = (json.has("title") ? json.getString("title") : NOT_AVAILABLE_VALUE);
		    String image = (json.has("imUrl") ? json.getString("imUrl") : NOT_AVAILABLE_VALUE);
		    String description = (json.has("description") ? json.getString("description") : NOT_AVAILABLE_VALUE);
		    JSONArray arrCategories = json.getJSONArray("categories");
		    Set<String> categories = new TreeSet<String>();
		    
		    for (int i=0; i<arrCategories.length(); i++) {
		    	JSONArray arr = arrCategories.getJSONArray(i);
		    	for (Object o: arr) {
		    		if (o instanceof String) {
		    			categories.add(o.toString());
		    		}
		    	}
		    }

			BoundStatement bstmt = pstmt.bind()
			.setString("asin", asin)
			.setSet("categories", categories, String.class)
			.setString("description", description)
			.setString("image", image)
			.setString("title", title);

			session.execute(bstmt);
		}
		
}
	


		