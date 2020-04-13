package com.signomix.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import static org.apache.http.HttpHeaders.USER_AGENT;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

/**
 *
 * @author greg
 */
public class RestClient {
    
    Properties props;

    public static void main(String[] args) {
        String url = null;
        String fileLocation = null;
        String deviceEUI = null;
        String authKey = null;
        int interval = 0;
        String provider = "";
        String line = null;
        RestClient restClient = new RestClient();
        restClient.readProperties();
        //prop.setProperty("db.url", "localhost");

        // create Options object
        Options options = new Options();
        options.addOption("u", "url", true, "Signomix service URL");
        options.addOption("f", "file", true, "data file location");
        options.addOption("l", "line", true, "commandline params");
        options.addOption("e", "eui", true, "device EUI");
        options.addOption("a", "auth", true, "device authorization key");
        options.addOption("i", "interval", true, "interval between transmissions");
        options.addOption("p", "provider", true, "provider type: {rest,ttn} (rest is default)");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            url = cmd.getOptionValue("u");
            if (null == url) {
                url = restClient.props.getProperty("url");
            }
            fileLocation = cmd.getOptionValue("f");
            deviceEUI = cmd.getOptionValue("e");
            if (null == deviceEUI) {
                deviceEUI = restClient.props.getProperty("eui");
            }
            authKey = cmd.getOptionValue("a");
            if (null == authKey) {
                authKey = restClient.props.getProperty("auth.key");
            }
            provider = cmd.getOptionValue("p");
            if (null == provider) {
                provider = restClient.props.getProperty("integration.type");
            }
            try {
                interval = Integer.parseInt(cmd.getOptionValue("i"));
            } catch (NumberFormatException | NullPointerException e) {
            }
            line = cmd.getOptionValue("l");
            if (null == url || null == authKey || (null == fileLocation && null == line)) {
                restClient.printHelp(options);
                System.exit(2);
            }
        } catch (ParseException ex) {
            Logger.getLogger(RestClient.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

        System.out.println("SignomixClient " + restClient.getVersionName());
        System.out.println("URL:" + url);
        System.out.println("FILE:" + fileLocation);
        System.out.println("EUI:" + deviceEUI);
        System.out.println("AUTH:" + authKey);

        // properties
        restClient.props.setProperty("url", url);
        restClient.props.setProperty("auth.key", authKey);
        restClient.props.setProperty("eui", deviceEUI);
        if(null==provider || provider.isEmpty()){
            provider="REST";
        }
        restClient.props.setProperty("integration.type", provider);

        if ("TTN".equalsIgnoreCase(provider)) {
            restClient.sendAsJson(url, authKey, fileLocation);
            restClient.saveProperties();
        } else if ("REST".equalsIgnoreCase(provider) || provider.isEmpty()) {
            if (null == deviceEUI) {
                restClient.printHelp(options);
                System.exit(2);
            }
            System.out.println("INTERVAL:" + interval + " seconds");
            if (null != fileLocation) {
                restClient.sendAsGeneric(url, authKey, deviceEUI, fileLocation, interval);
            } else {
                restClient.sendAsGeneric(url, authKey, deviceEUI, line);
            }
            restClient.saveProperties();
        } else {
            System.out.println("Provider name " + provider + " not valid");
        }
    }

    private void sendAsGeneric(String url, String authKey, String deviceEUI, String fileLocation, int interval) {
        //HttpClient client;

        FileReader reader = null;
        CSVParser parser = null;
        try {
            reader = new FileReader(fileLocation);
            parser = CSVFormat.EXCEL.parse(reader);
        } catch (IOException ex) {
            Logger.getLogger(RestClient.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(3);
        }
        String[] headers = null;
        String[] values;
        Iterator it = parser.iterator();
        CSVRecord record;
        long lineNumber = 0;
        long lastTransmission = 0;
        while (it.hasNext()) {
            lineNumber++;
            record = (CSVRecord) it.next();
            if (record.get(0).startsWith("#") && null == headers) {
                // first commented line must be header
                lineNumber--;
                headers = new String[record.size()];
                for (int i = 0; i < record.size(); i++) {
                    if (i == 0) {
                        headers[i] = record.get(i).substring(1);
                    } else {
                        headers[i] = record.get(i);
                    }
                }
            } else if (!record.get(0).startsWith("#") && null != headers) {
                values = new String[record.size()];
                for (int i = 0; i < record.size(); i++) {
                    values[i] = record.get(i);
                }
                if (values.length != headers.length) {
                    System.out.println(lineNumber + " skipped: the number of data fields different from those listed in the header");
                }
                // send data record
                while (System.currentTimeMillis() - lastTransmission < interval * 1000) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(RestClient.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                send(url, authKey, deviceEUI, headers, values, lineNumber);
                lastTransmission = System.currentTimeMillis();
            } else if (null == headers) {
                System.out.println("The file must start with a header line");
                System.exit(6);
            } else {
                // ignore comment lines
                System.out.println(lineNumber + " commented");
            }
        }
    }

    private void sendAsGeneric(String url, String authKey, String deviceEUI, String line) {
        String[] headers = null;
        String[] values = null;
        String[] parts = line.split("=");
        System.out.println("LINE:"+line);
        if (parts.length >1 ) {
            headers = parts[0].split(",");
            values = parts[1].split(",");
            props.setProperty("data.names",parts[0]);
        }else if(parts.length==1){
            headers = props.getProperty("data.names").split(",");
            values = parts[0].split(",");
        }
        long lineNumber = 0;
        send(url, authKey, deviceEUI, headers, values, lineNumber);
    }

    private void sendAsJson(String url, String authKey, String fileLocation) {
        File file;
        FileReader fr;
        try {
            file = new File(fileLocation);
            fr = new FileReader(file);

            BufferedReader br = new BufferedReader(fr);
            String line;
            String json = "";
            while ((line = br.readLine()) != null) {
                json = json.concat(line);
            }
            send(url, authKey, json);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(7);
        }
    }

    private void send(String url, String authKey, String eui, String[] names, String[] values, long lineNumber) {
        String apiPath = "api/i4t";
        if (!url.endsWith("/")) {
            apiPath = "/" + apiPath;
        }
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url + apiPath);
        // add header
        post.setHeader("User-Agent", USER_AGENT);
        post.setHeader("Authorization", authKey);
        List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
        urlParameters.add(new BasicNameValuePair("eui", eui));

        HttpResponse response;
        try {
            for (int i = 0; i < names.length; i++) {
                if ("timestamp".equals(names[i])) {
                    urlParameters.add(new BasicNameValuePair(names[i], "" + getMillis(values[i])));
                } else {
                    urlParameters.add(new BasicNameValuePair(names[i], values[i]));
                }
            }
            post.setEntity(new UrlEncodedFormEntity(urlParameters));
            response = client.execute(post);
            int statusCode = response.getStatusLine().getStatusCode();
            BufferedReader rd = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            if (statusCode == 200 || statusCode == 201) {
                System.out.println(lineNumber + " OK : " + statusCode);
                if (!result.toString().isEmpty()) {
                    System.out.println(result.toString());
                }
            } else {
                System.out.println(lineNumber + " ERROR : " + statusCode);
                System.out.println(result.toString());
            }
        } catch (java.text.ParseException ex) {
            Logger.getLogger(RestClient.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(4);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(RestClient.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(4);
        } catch (IOException ex) {
            Logger.getLogger(RestClient.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(5);
        }
    }

    private void send(String url, String authKey, String json) {
        String apiPath = "api/ttn";
        if (!url.endsWith("/")) {
            apiPath = "/" + apiPath;
        }
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url + apiPath);
        // add header
        post.setHeader("User-Agent", USER_AGENT);
        post.setHeader("Authorization", authKey);
        HttpResponse response;

        try {
            post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
            response = client.execute(post);
            int statusCode = response.getStatusLine().getStatusCode();
            BufferedReader rd = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            if (statusCode == 200 || statusCode == 201) {
                System.out.println("OK : " + statusCode);
                if (!result.toString().isEmpty()) {
                    System.out.println(result.toString());
                }
            } else {
                System.out.println("ERROR : " + statusCode);
                System.out.println(result.toString());
            }
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(RestClient.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(4);
        } catch (IOException ex) {
            Logger.getLogger(RestClient.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(5);
        }
    }

    private long getMillis(String dateStr) throws java.text.ParseException {
        long result;
        try {
            result = Long.parseLong(dateStr);
        } catch (NumberFormatException e) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date date = format.parse(dateStr);
            result = date.getTime();
        }
        return result;
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar SignomixClient.jar", options);
    }

    String getVersionName() {
        try {
            InputStream propertiesStream = getClass().getClassLoader().getResourceAsStream("META-INF/maven/com.signomix/SignomixClient/pom.properties");
            if (propertiesStream != null) {
                Properties props = new Properties();
                props.load(propertiesStream);
                return props.getProperty("version", "");
            }
        } catch (IOException e) {
        }
        return "";
    }

    private void saveProperties() {
        String usersHomeDir = System.getProperty("user.home");
        String propertyFilePath = usersHomeDir + File.separator + ".SignomixClient.properties";
        try (OutputStream output = new FileOutputStream(propertyFilePath)) {
            props.store(output, null);
        } catch (IOException io) {
            io.printStackTrace();
        }
    }

    private void readProperties() {
        props = new Properties();
        String usersHomeDir = System.getProperty("user.home");
        String propertyFilePath = usersHomeDir + File.separator + ".SignomixClient.properties";
        try (InputStream input = new FileInputStream(propertyFilePath)) {
            props.load(input);
        } catch (IOException io) {
            //io.printStackTrace();
        }
    }
}
