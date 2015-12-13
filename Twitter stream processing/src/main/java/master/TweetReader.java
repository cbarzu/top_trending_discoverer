package master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;

public class TweetReader {
	private OAuthService service;
	private Token accessToken;
	
	private static final String STREAM_URI = "https://stream.twitter.com/1.1/statuses/filter.json";
	
	public TweetReader() throws IOException{
		service = new ServiceBuilder().provider(TwitterApi.class).apiKey("Y3VdpNIgRPorBp6uVpH2gvEZW").apiSecret("2eMBf7AsZdLELJeF19HVyo4OZgGMdpHbVejFc3PKdjtIGatUVA").build();		
		accessToken = new Token("4450825533-9drOs8cEBKnwBZqL07zpCg6tRMV3MIFq9Iox0U2","s82Wai5tZjmuXBR9gbRp5eVMWHNZheYieXAIyMZcXQwCt");
		
		// Let's generate the request
        System.out.println("Connecting to Twitter Public Stream");
        OAuthRequest request = new OAuthRequest(Verb.POST, STREAM_URI);
        request.addHeader("version", "HTTP/1.1");
        request.addHeader("host", "stream.twitter.com");
        request.setConnectionKeepAlive(true);
        request.addHeader("user-agent", "Twitter Stream Reader");
        request.addBodyParameter("track", "java,heroku,twitter"); // Set keywords you'd like to track here
        service.signRequest(accessToken, request);
        Response response = request.send();

        // Create a reader to read Twitter's stream
        BufferedReader reader = new BufferedReader(new InputStreamReader(response.getStream()));

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            break;
        }
        
        
	
	}
}
