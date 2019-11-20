package com.kibria;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;



public class TaxiRideEvent{
    
    private int VendorID;	
    private Date tpep_pickup_datetime;
    private Date tpep_dropoff_datetime;
    private int passenger_count;
    private double trip_distance;
    private int PULocationID;
    private int DOLocationID;
    private int RatecodeID;
    private String store_and_fwd_flag;
    private int payment_type;
    private double fare_amount;	
    private double extra;
    private double mta_tax;
    private double tip_amount;
    private double tolls_amount;
    private double improvement_surcharge;
    private double total_amount;
    private String congestion_surcharge;

    public TaxiRideEvent() {
        super();
    }

    public TaxiRideEvent(
        int VendorID,
        String tpep_pickup_datetime,
        String tpep_dropoff_datetime, 
        int passenger_count, 
        double trip_distance,
        int PULocationID,
        int DOLocationID,
        int RatecodeID,
        String store_and_fwd_flag,
        int payment_type,
        double fare_amount,	
        double extra,
        double mta_tax,
        double tip_amount,
        double tolls_amount,
        double improvement_surcharge,
        double total_amount,
        String congestion_surcharge) throws ParseException 
    {
        
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        this.VendorID = VendorID;
        this.tpep_pickup_datetime = df.parse(tpep_pickup_datetime);
        this.tpep_dropoff_datetime = df.parse(tpep_dropoff_datetime);        
        this.passenger_count = passenger_count;
        this.trip_distance = trip_distance;
        this.PULocationID = PULocationID;
        this.DOLocationID = DOLocationID;
        this.RatecodeID = RatecodeID;
        this.store_and_fwd_flag = store_and_fwd_flag;
        this.payment_type = payment_type;
        this.fare_amount = fare_amount;
        this.extra = extra;
        this.mta_tax = mta_tax;
        this.tip_amount = tip_amount;
        this.tolls_amount = tolls_amount;
        this.improvement_surcharge = improvement_surcharge;
        this.total_amount = total_amount;
        this.congestion_surcharge = congestion_surcharge;

    }
	public int getVendorID() {
		return VendorID;
	}

	public int getPassenger_count() {
		return passenger_count;
	}
	public double getTrip_distance() {
		return trip_distance;
	}
	public int getPULocationID() {
		return PULocationID;
	}
	public int getDOLocationID() {
		return DOLocationID;
	}
	public int getPayment_type() {
		return payment_type;
	}
	public double getFare_amount() {
		return fare_amount;
	}
	public double getExtra() {
		return extra;
	}
	public double getMta_tax() {
		return mta_tax;
	}
	public double getTip_amount() {
		return tip_amount;
	}
	public double getTolls_amount() {
		return tolls_amount;
	}
	public double getImprovement_surcharge() {
		return improvement_surcharge;
	}
	public double getTotal_amount() {
		return total_amount;
	}  

    public int getRatecodeID() {
        return RatecodeID;
    }

    public String getStore_and_fwd_flag() {
        return store_and_fwd_flag;
    }

    public String getCongestion_surcharge() {
        return congestion_surcharge;
    }

    
    public static void main(String[] args) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        
        System.out.println(LocalDateTime.parse("2019-01-01 01:14:24",formatter));
    }

    public void setVendorID(int vendorID) {
        VendorID = vendorID;
    }

   

    public void setPassenger_count(int passenger_count) {
        this.passenger_count = passenger_count;
    }

    public void setTrip_distance(double trip_distance) {
        this.trip_distance = trip_distance;
    }

    public void setPULocationID(int pULocationID) {
        PULocationID = pULocationID;
    }

    public void setDOLocationID(int dOLocationID) {
        DOLocationID = dOLocationID;
    }

    public void setRatecodeID(int ratecodeID) {
        RatecodeID = ratecodeID;
    }

    public void setStore_and_fwd_flag(String store_and_fwd_flag) {
        this.store_and_fwd_flag = store_and_fwd_flag;
    }

    public void setPayment_type(int payment_type) {
        this.payment_type = payment_type;
    }

    public void setFare_amount(double fare_amount) {
        this.fare_amount = fare_amount;
    }

    public void setExtra(double extra) {
        this.extra = extra;
    }

    public void setMta_tax(double mta_tax) {
        this.mta_tax = mta_tax;
    }

    public void setTip_amount(double tip_amount) {
        this.tip_amount = tip_amount;
    }

    public void setTolls_amount(double tolls_amount) {
        this.tolls_amount = tolls_amount;
    }

    public void setImprovement_surcharge(double improvement_surcharge) {
        this.improvement_surcharge = improvement_surcharge;
    }

    public void setTotal_amount(double total_amount) {
        this.total_amount = total_amount;
    }

    public void setCongestion_surcharge(String congestion_surcharge) {
        this.congestion_surcharge = congestion_surcharge;
    }

    public Date getTpep_pickup_datetime() {
        return tpep_pickup_datetime;
    }

    public void setTpep_pickup_datetime(Date tpep_pickup_datetime) {
        this.tpep_pickup_datetime = tpep_pickup_datetime;
    }

    public Date getTpep_dropoff_datetime() {
        return tpep_dropoff_datetime;
    }

    public void setTpep_dropoff_datetime(Date tpep_dropoff_datetime) {
        this.tpep_dropoff_datetime = tpep_dropoff_datetime;
    }


}