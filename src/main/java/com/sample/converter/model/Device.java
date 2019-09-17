/**
 * 
 */
package com.sample.converter.model;

import lombok.Data;

/**
 * @author rajnish
 *
 */
@Data
public class Device {

    private String id;
    
    private String alternateId;
    
    private Double temperature;
    
    private Double pressure;
    
    private String channelId;
    
}
