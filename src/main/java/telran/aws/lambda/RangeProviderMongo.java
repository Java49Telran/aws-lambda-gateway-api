package telran.aws.lambda;

import java.io.*;

import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.mongodb.client.*;

@SuppressWarnings("unchecked")
public class RangeProviderMongo implements RequestStreamHandler{

	@Override
	public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
		var logger = context.getLogger();
		String mongoURI = System.getenv("MONGO_URI");
		logger.log(String.format("Mongo URI is %s", mongoURI));
		JSONParser parser = new JSONParser();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String response = "";
		try {
			JSONObject inputMap = (JSONObject) parser.parse(reader);
			JSONObject pathParametersMap = (JSONObject) inputMap.get("pathParameters");
			long sensorId = Long.parseLong((String)pathParametersMap.get("id"));
			logger.log("sensor id is " + sensorId);
			String sensorRangeJSON = getSensorRangeJSON(mongoURI, sensorId, logger);
			response = sensorRangeJSON != "" ? createResponse(sensorRangeJSON, 200) :
					createResponse(String.format("Sensor with id %d not found", sensorId), 404);
		} catch (Exception e) {
			String message = "error: " + e.toString();
			logger.log(message);
			response = createResponse(message, 400);
		}
		PrintStream printStream = new PrintStream(output);
		printStream.println(response);
		printStream.close();
		
	}
	private String getSensorRangeJSON(String mongoURI, long sensorId, LambdaLogger logger) {
		String result = "";
		MongoClient client = MongoClients.create(mongoURI);
		MongoDatabase database = client.getDatabase("sensors");
		MongoCollection<Document> collection = database.getCollection("sensor-ranges");
		Document document = collection.find(new Document("_id", sensorId)).first();
		if(document != null) {
			JSONObject mapDocument = new JSONObject();
		mapDocument.put("minValue", document.get("minValue"));
		mapDocument.put("maxValue", document.get("maxValue"));
		logger.log("mapDocument is " + mapDocument);
		result = mapDocument.toString();
		}
		
		return result;
	}
	
	String createResponse(String body, int statusCode) {
		JSONObject headerObject = new JSONObject();
		
		JSONObject responseObject = new JSONObject();
		if (statusCode == 200) {
			headerObject.put("Content-Type", "application/json");
			responseObject.put("headers", headerObject);
		}
		responseObject.put("statusCode", statusCode);
		responseObject.put("body", body);
		String jsonStr = responseObject.toString();
		return jsonStr;
	}

}
