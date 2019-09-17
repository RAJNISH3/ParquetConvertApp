/**
 * 
 */
package com.sample.converter.controller;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.sample.converter.model.Device;
import com.sample.converter.model.DeviceList;
import com.sample.converter.service.AwsClientImpl;

import lombok.extern.slf4j.Slf4j;

/**
 * @author rajnish
 *
 */
@Slf4j
@RequestMapping("/convert")
@RestController
public class JsonToCSV_ORC_Converter {

    public static final String FILE_EXTENSION = ".csv";
    public static final String FILE_EXTENSION_ORC = ".orc";

    private AwsClientImpl awsClientImpl;

    public JsonToCSV_ORC_Converter(AwsClientImpl awsClientImpl) {
        this.awsClientImpl = awsClientImpl;
    }

    @GetMapping(value = "/test")
    @ResponseStatus(HttpStatus.OK)
    public String getTest() {

        return "Hello Sucess";
    }

    @PostMapping(value = "/toCSV")
    @ResponseStatus(HttpStatus.OK)
    public String convertJsonToCSV(@RequestBody @Validated DeviceList deviceList) {
        log.info("******************Inside the convertJsonToCSV****************");
        Map<String, String> chronoMap = getFolderBasedOnTimestamp(Instant.now().toEpochMilli());
        File csvFile = null;
        try {
            csvFile = convertToCSV(deviceList.getDeviceLst());

            if (csvFile != null && csvFile.getPath() != null) {
                final InputStream csvStream = new DataInputStream(
                        new FileInputStream(csvFile.getAbsoluteFile()));
                String fileName =  System.currentTimeMillis() * 1000 + FILE_EXTENSION;
                this.awsClientImpl.uploadToS3(chronoMap, fileName, csvStream);
            }
        } catch (Exception e) {
            log.error("exception {}", e);
            e.printStackTrace();
        }

        return " Covert from Json to CSV File Sucessful !!!";
    }

    @PostMapping(value = "/toORC")
    @ResponseStatus(HttpStatus.OK)
    public String convertJsonToORC(@RequestBody @Validated DeviceList deviceList) {
        log.info("******************Inside the convertJsonToORC****************");
        Map<String, String> chronoMap = getFolderBasedOnTimestamp(Instant.now().toEpochMilli());
        File orcFile = null;
        try {
            orcFile = convertToORC(deviceList.getDeviceLst());

            
            if (orcFile != null && orcFile.getPath() != null) {
                final InputStream orcStream = new DataInputStream(
                        new FileInputStream(orcFile.getAbsoluteFile()));
                String fileName =  System.currentTimeMillis() * 1000 + FILE_EXTENSION_ORC;
                this.awsClientImpl.uploadToS3(chronoMap, fileName, orcStream);
            }
        } catch (Exception e) {
            log.error("exception {}", e);
            e.printStackTrace();
        }

        return " Covert from Json to ORC File Sucessful !!!";
    }
 
    /**
     * @param list
     * @return temp file
     */
    private File convertToCSV(List<Device> list) {
        JavaSparkContext sparkContext = null;
        File tempFile = null;
        try (SparkSession spark = SparkSession.builder()
                .master("local[4]")
                .appName("forwarding-application")
                .getOrCreate()) {

            tempFile = this.createTempFile();

            Gson gson = new Gson();
            List<String> data = Arrays.asList(gson.toJson(list));
            sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
            Dataset<String> stringDataSet = spark.createDataset(data, Encoders.STRING());
            Dataset<Row> csvDataSet = spark.read().json(stringDataSet);
            log.info("Inserted json conversion schema and value");
            csvDataSet.printSchema();
            csvDataSet.show();
            if (tempFile != null) {
                csvDataSet.write()
                .option("compression","GZIP")
                .csv(tempFile.getPath());
                tempFile = this.retrieveCSVFileFromPath(tempFile);
            }
        } catch (Exception ex) {
            log.error("Stack Trace: {}", ex);
        } finally {
            if (sparkContext != null) {
                sparkContext.close();
            }
        }
        return tempFile;
    }

    /**
     * @param list
     * @return temp file
     */
    private File convertToORC(List<Device> list) {
        JavaSparkContext sparkContext = null;
        File tempFile = null;
        try (SparkSession spark = SparkSession.builder()
                .master("local[4]")
                .appName("forwarding-application")
                .getOrCreate()) {

            tempFile = this.createTempFile();

            Gson gson = new Gson();
            List<String> data = Arrays.asList(gson.toJson(list));
            sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
            Dataset<String> stringDataSet = spark.createDataset(data, Encoders.STRING());
            Dataset<Row> orcDataSet = spark.read().json(stringDataSet);
            log.info("Inserted json conversion schema and value");
            orcDataSet.printSchema();
            orcDataSet.show();
            if (tempFile != null) {
                orcDataSet.write().orc(tempFile.getPath());
                tempFile = this.retrieveORCFileFromPath(tempFile);
            }
        } catch (Exception ex) {
            log.error("Stack Trace: {}", ex);
        } finally {
            if (sparkContext != null) {
                sparkContext.close();
            }
        }
        return tempFile;
    }

    private File createTempFile() throws IOException {
        Path tempPath = Files.createTempDirectory("");
        File tempFile = tempPath.toFile();
        if (tempFile != null && tempFile.exists()) {
            String tempFilePath = tempFile.getAbsolutePath();
            tempFile.deleteOnExit();
            tempFile.delete();
            //Files.deleteIfExists(tempFile.toPath());
            log.debug("Deleted tempFile[ {} ]}", tempFilePath);
        }
        return tempFile;
    }

    private File retrieveCSVFileFromPath(File tempFilePath) {
        List<File> files = Arrays.asList(tempFilePath.listFiles());
        return files.stream()
                .filter(
                    tmpFile -> tmpFile.getPath().contains(FILE_EXTENSION) && tmpFile.getPath().endsWith(FILE_EXTENSION))
                .findAny()
                .orElse(null);
    }
    
    private File retrieveORCFileFromPath(File tempFilePath) {
        List<File> files = Arrays.asList(tempFilePath.listFiles());
        return files.stream()
                .filter(
                    tmpFile -> tmpFile.getPath().contains(FILE_EXTENSION_ORC) && tmpFile.getPath().endsWith(FILE_EXTENSION_ORC))
                .findAny()
                .orElse(null);
    }

    
    
    private static Map<String, String> getFolderBasedOnTimestamp(long timestamp) {
        Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.US);
        calendar.setTimeInMillis(timestamp / 1000);

        Map<String, String> chronoMap = new HashMap<>();
        chronoMap.put("year", String.valueOf(calendar.get(Calendar.YEAR)));
        chronoMap.put("month", String.valueOf(calendar.get(calendar.get(Calendar.MONTH) + 1)));
        chronoMap.put("day", String.valueOf(calendar.get(Calendar.DAY_OF_MONTH)));
        chronoMap.put("hour", String.valueOf(calendar.get(Calendar.HOUR_OF_DAY)));
        return chronoMap;
    }

}
