create table steps(week_strt_day String,userId String, stepsnum Int) stored as parquetfile LOCATION 'hdfs://namenode:9000//EDL_Data/Trusted_Data_Zone/steps';MSCK REPAIR TABLE steps;

create table dimuser (id String,givenName String,familyName String,email String,gender String,phoneNumber String,dateOfBirth String) stored as parquetfile LOCATION 'hdfs://namenode:9000//EDL_Data/Trusted_Data_Zone/user';


create table bloodpressure(startDateTime String, userId String, diastolicValue_color String,diastolicValue_direction String,diastolicValue_isCustom String, diastolicValue_severity Int, systolicValue_color String, systolicValue_direction String, systolicValue_isCustom String,systolicValue_severity Int) stored as parquetfile 
LOCATION 'hdfs://namenode:9000//EDL_Data/Trusted_Data_Zone/bloodpressure';MSCK REPAIR TABLE bloodpressure;
