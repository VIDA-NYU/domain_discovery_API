import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Iterator;
import java.net.URL;
import java.net.HttpURLConnection;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.json.JSONObject;
import org.json.JSONArray;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.search.SearchHit; 
import org.elasticsearch.search.SearchHits; 
import java.net.URI;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import java.util.Properties;
import java.io.IOException;
import java.io.FileInputStream;
import java.net.MalformedURLException;

public class CrawlerInterface implements Runnable{
    private static final Pattern linkPattern = Pattern.compile("\\s*(?i)href\\s*=\\s*(\"([^\"]*\")|'[^']*'|([^'\">\\s]+))",  Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
    private String accountKey;
    private Properties prop; 
    ArrayList<String> urls = null;
    ArrayList<String> html = null;
    String es_index = "memex";
    String es_doc_type = "page";
    String es_host = "localhost";
    Client client = null;
    String crawlType = "";
    int top = 10;
    Download download = null;
    
    public CrawlerInterface(ArrayList<String> urls, ArrayList<String> html, String crawl_type, String top, String es_index, String es_doc_type, String es_host, Client client){
	try{
	    prop = new Properties();
	    FileInputStream is = new FileInputStream("conf/config.properties");
	    prop.load(is);
	    accountKey = prop.getProperty("ACCOUNTKEY_BING");
	}   
	catch(Exception e){
	    e.printStackTrace();
	    prop = null;
	}
	
	this.urls = urls;
	this.html = html;
	if(!es_index.isEmpty())
	    this.es_index = es_index;
	if(!es_doc_type.isEmpty())
	    this.es_doc_type = es_doc_type;
	this.es_host = es_host;
	this.client = client;
	this.crawlType = crawl_type;
	this.top = Integer.parseInt(top);

	String subquery = null;
	ArrayList<String> tag = null;

	this.download = new Download("Crawl_" + this.es_index, subquery, tag, this.es_index, this.es_doc_type, this.es_host);
    }

    public ArrayList<String> crawl_backward(ArrayList<String> urls){
        /*Using backlink search to find more similar webpages
        *Args:
        *- urls: a list of relevant urls
        *Returns:
        *- res: a list of backlinks
        */
        HashSet<String> links = new HashSet<String>();
        for (String b_url: urls){
	    try {
		int step = 50; //Bing can return maximum 50 results per query
		URIBuilder builder = new URIBuilder("https://api.cognitive.microsoft.com/bing/v5.0/search");
		builder.setParameter("q", "inbody:"+b_url);
		builder.setParameter("count", String.valueOf(step));
		builder.setParameter("mkt", "en-us");
		builder.setParameter("safesearch", "Off"); // allow results to include adult content
		HttpClient httpclient = HttpClients.createDefault();
		int nStart = 0;
		ArrayList<String> b_links = new ArrayList<String>();
		for (; nStart < this.top; nStart += step){
		    builder.setParameter("offset", String.valueOf(nStart));
		    URI uri = builder.build();
		    
		    HttpGet request = new HttpGet(uri);
		    request.setHeader("Ocp-Apim-Subscription-Key", this.accountKey);
		    
		    HttpResponse response = httpclient.execute(request);
		    HttpEntity entity = response.getEntity();
		    
		    String json_string = EntityUtils.toString(entity);
		    JSONObject jsResponse = new JSONObject(json_string);
		    if(jsResponse.has("webPages")){
			JSONObject webPagesTemp = jsResponse.getJSONObject("webPages");
			
			JSONArray webpages = webPagesTemp.getJSONArray("value");
			
			for (int i=0; i<webpages.length(); i++){
			    JSONObject item = webpages.getJSONObject(i);
			    String url = (String)item.get("url");
			    try {
				//Bing Search v5 returns weird url format, e.g., http://www.bing.com/cr?IG=1C80F8C1C1B04D4C866FD62099EF9E4E&CID=2291C45E2291669401DFCEFC23976733&rd=1&h=e9ZvWOedIV321QOg-FnBNtNHTR9Oo3Yqss9bCRYsT9o&v=1&r=http://www.cse.unsw.edu.au/%7Ecs9417ml/RL1/introduction.html&p=DevEx,5168.1. The following code remove the boilerplate
				url = url.split(",")[0].split("v=1&r=")[1];
				url = java.net.URLDecoder.decode(url, "UTF-8");
			    }
			    catch (Exception ex) {
				url = (String)item.get("displayUrl");
			    }
			    links.add(url);
			    b_links.add(url);
			}
		    }
		}
		UpdateRequest updateRequest = new UpdateRequest(this.es_index, this.es_doc_type, b_url)
		    .doc(XContentFactory.jsonBuilder()
			 .startObject()
			 .field("backward_links", b_links)
			 .endObject());
		this.client.update(updateRequest).get();

		
		for(String b_link: b_links){
		    updateRequest = new UpdateRequest(this.es_index, this.es_doc_type, b_link)
			.doc(XContentFactory.jsonBuilder()
			     .startObject()
			     .field("url", b_link)
			     .field("query", "BackLink_"+b_url)
			     .endObject());
		    updateRequest.docAsUpsert(true);
		    this.client.update(updateRequest).get();
		}
	    } 
	    catch (MalformedURLException e1) {
		e1.printStackTrace();
	    } 
	    catch (IOException e) {
		e.printStackTrace();
	    }
	    catch (Exception e){
		e.printStackTrace();
	    }
        }
        
        ArrayList<String> res = new ArrayList<String>(links);

	System.out.println();
	System.out.println("Backlinks");
	System.out.println(res);
	System.out.println();

	try{
	    ArrayList<String> not_crawled = new ArrayList();
	    for(String url: res){
		SearchResponse searchResponse = client.prepareSearch(this.es_index)
		    .setTypes(this.es_doc_type)
		    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		    .setFetchSource(new String[]{"url","html","crawled_forward"}, null)
		    .setQuery(QueryBuilders.termQuery("url", url))
		    .setFrom(0).setExplain(true)
		    .execute()
		    .actionGet(5000);
		
		SearchHit[] hits = searchResponse.getHits().getHits();
		if(hits.length > 0){
		    for (SearchHit hit : hits) {
			Map map = hit.getSource();
			if(map != null){
			    if(map.get("crawled_forward") != null){
				if((Integer)map.get("crawled_forward") == 0){
				    UpdateRequest updateRequest = new UpdateRequest(this.es_index, this.es_doc_type, hit.getId())
					.doc(XContentFactory.jsonBuilder()
					     .startObject()
					     .field("crawled_forward", 1)
					     .endObject());
				    this.client.update(updateRequest).get();
				    
				    System.out.println("Crawling forward " + url);
				    System.out.println();
				    ArrayList<String> fwd_links = this.crawl_forward(url, (String)map.get("html"));
				    updateRequest = new UpdateRequest(this.es_index, this.es_doc_type, hit.getId())
					.doc(XContentFactory.jsonBuilder()
					     .startObject()
					     .field("forward_links", fwd_links)
					     .endObject());
				    this.client.update(updateRequest).get();
				}
			    }
			}
		    }
		}else {
		    not_crawled.add(url);
		}
	    }
	    
	    for(String url: not_crawled){
		// Update the crawled flag
		UpdateRequest updateRequest = new UpdateRequest(this.es_index, this.es_doc_type, url)
		    .doc(XContentFactory.jsonBuilder()
			 .startObject()
			 .field("url", url)
			 .field("crawled_forward", 1)
			 .endObject());
		updateRequest.docAsUpsert(true);
		this.client.update(updateRequest).get();
				
		//Download the page
		String domain = null;
		try{
		    domain = (new URL(url)).getHost();
		} catch(Exception e) {
		    e.printStackTrace();
		}
		this.download.setQuery("BackLink_" + url);
		JSONObject url_info = new JSONObject();
		url_info.put("link",url);
		url_info.put("snippet","");
		url_info.put("title","");
		
		this.download.addTask(url_info);
		
		//Crawl page forward
		System.out.println("Crawling forward " + url);
		System.out.println();
		
		String html = this.getContent(url);
		ArrayList<String> fwd_links = this.crawl_forward(url, html);
		updateRequest = new UpdateRequest(this.es_index, this.es_doc_type, url)
		    .doc(XContentFactory.jsonBuilder()
			 .startObject()
			 .field("forward_links", fwd_links)
			 .endObject());
		this.client.update(updateRequest).get();

	    }
	
	    return res;
	}
	catch(Exception e){
	    e.printStackTrace();
	}
	return null;
    }

    public ArrayList<String> crawl_forward(String url, String html){
        /*Extract and standarlize outlinks from the html
        *Args:
        *- url: 
        *- html: html content to be extracted
        *Returns:
        *- res: a list of urls extracted from html content
        */
        HashSet<String> links = new HashSet<String>();
        try{
	    int count = this.top;
	    int num = 0;
            Matcher pageMatcher = linkPattern.matcher(html);
            String domain = "http://" + (new URL(url)).getHost();
            while(pageMatcher.find() && num <= count){
                String link = pageMatcher.group(2);
                if (link != null){
                    //Validate and standarlize url
                    link = link.replaceAll("\"", "");
                    if (link.indexOf(".css") != -1)
                        continue;
                    if (link.startsWith("/"))
                        link = domain + link;
                    if (!link.startsWith("http://"))
                        continue;
                    links.add(link);
		    num = num + 1;
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

	this.download.setQuery("ForwardLink_" + url);

        ArrayList<String> res = new ArrayList<String>(links);
	for(String f_url: res){
	    String domain = this.es_index;
	    try{
		domain = (new URL(f_url)).getHost();
	    } catch(Exception e) {
		e.printStackTrace();
	    }

	    JSONObject url_info = new JSONObject();
	    url_info.put("link",f_url);
	    url_info.put("snippet","");
	    url_info.put("title","");

	    this.download.addTask(url_info);
	}

        return res;
    }

    public void test_backlink(String seed, String top){
        ArrayList<String> urls = new ArrayList<String>();
        urls.add(seed);
        ArrayList<String> res = crawl_backward(urls);
       
    }

    public String getContent(String seed){
        try{
            URL url = new URL(seed);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setRequestMethod("GET");
            
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            String output = "";
            String line;
            while ((line = br.readLine()) != null) {
                output = output + line;
            }
            conn.disconnect();

	    return output;

        } catch (Exception e){
            e.printStackTrace();
        }

	return "";
    }

    public void run() {
	if(this.crawlType.equals("backward")){
	    this.crawl_backward(this.urls);
	}
	else if(this.crawlType.equals("forward")){
	    for(int i=0; i < this.urls.size();++i){
		SearchResponse response = null;
		try{
		    response = client.prepareSearch(this.es_index)
			.setTypes(this.es_doc_type)
			.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			.setFetchSource(new String[]{"html"}, null)
			.setQuery(QueryBuilders.termQuery("url", this.urls.get(i)))                
			.setFrom(0).setExplain(true)
			.execute()
			.actionGet(5000);
		} catch(Exception e){
		    e.printStackTrace();
		}

		if(response == null)
		    return;
		
		String html = "";
		for (SearchHit hit : response.getHits()) {
		    Map map = hit.getSource();
		    html = (String)map.get("html");
		}
		this.crawl_forward(this.urls.get(i), html);
	    }
	}
	this.download.shutdown();
    }
    
}
