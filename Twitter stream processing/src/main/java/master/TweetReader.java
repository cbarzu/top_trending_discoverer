package master;

import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.Token;
import org.scribe.oauth.OAuthService;

public class TweetReader {
	private OAuthService service;
	private Token accessToken;
	
	public TweetReader(){
		service = new ServiceBuilder().provider(TwitterApi.class).apiKey("Y3VdpNIgRPorBp6uVpH2gvEZW").apiSecret("2eMBf7AsZdLELJeF19HVyo4OZgGMdpHbVejFc3PKdjtIGatUVA").build();
		
		accessToken = new Token("4450825533-9drOs8cEBKnwBZqL07zpCg6tRMV3MIFq9Iox0U2","s82Wai5tZjmuXBR9gbRp5eVMWHNZheYieXAIyMZcXQwCt");
	}
}
