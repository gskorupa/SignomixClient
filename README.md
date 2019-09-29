# SignomixClient

The program allows you to send data from a CSV file to Signomix using the REST API.


```
Usage: java -jar SignomixClient.jar
 -a,--auth <arg>       device authorization key
 -e,--eui <arg>        device EUI
 -f,--file <arg>       CSV file location
 -i,--interval <arg>   interval between transmissions
 -u,--url <arg>        Signomix service URL
```

Example CSV file

```
#temperature,humidity,latitude,longitude,timestamp
10.0,55.3,51.7380978,19.4271537,2019-09-28 23:53:00
11,45.5,51.7390978,19.4281537,2019-09-28 23:54:00
```

https://signomix.com
