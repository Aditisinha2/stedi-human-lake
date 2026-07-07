-- File: customer_landing.sql
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
  customername string,
  email string,
  phone string,
  birthday string,
  serialnumber string,
  registrationdate bigint,
  lastupdatedate bigint,
  sharewithresearchasofdate bigint,
  sharewithpublicasofdate bigint,
  sharewithfriendsasofdate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  "mapping.customername" = "customerName",
  "mapping.birthday" = "birthDay",
  "mapping.serialnumber" = "serialNumber",
  "mapping.registrationdate" = "registrationDate",
  "mapping.lastupdatedate" = "lastUpdateDate",
  "mapping.sharewithresearchasofdate" = "shareWithResearchAsOfDate",
  "mapping.sharewithpublicasofdate" = "shareWithPublicAsOfDate",
  "mapping.sharewithfriendsasofdate" = "shareWithFriendsAsOfDate"
)
LOCATION 's3://stedihumanlake/customer/customer-landing/';


-- File: accelerometer_landing.sql
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
  user string,
  timestamp bigint,
  x double,
  y double,
  z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  "mapping.timestamp" = "timeStamp"
)
LOCATION 's3://stedihumanlake/accelerometer/accelerometer-landing/';


-- File: step_trainer_landing.sql
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
  sensorreadingtime bigint,
  serialnumber string,
  distancefromobject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  "mapping.sensorreadingtime" = "sensorReadingTime",
  "mapping.serialnumber" = "serialNumber",
  "mapping.distancefromobject" = "distanceFromObject"
)
LOCATION 's3://stedihumanlake/step_trainer/step_trainer-landing/';


-- File: machine_learning_curated.sql
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.machine_learning_curated (
  user_name string,
  time_stamp bigint,
  x double,
  y double,
  z double,
  sensor_reading_time bigint,
  serial_number string,
  distance_from_object int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  "mapping.user_name" = "user",
  "mapping.time_stamp" = "timeStamp",
  "mapping.sensor_reading_time" = "sensorReadingTime",
  "mapping.serial_number" = "serialNumber",
  "mapping.distance_from_object" = "distanceFromObject"
)
LOCATION 's3://stedihumanlake/machine-learning-curated/';


-- File: verification_queries.sql
-- Landing zone counts
SELECT COUNT(*) FROM stedi.customer_landing;
SELECT COUNT(*) FROM stedi.accelerometer_landing;
SELECT COUNT(*) FROM stedi.step_trainer_landing;

-- Customer landing should have blank/null shareWithResearchAsOfDate rows
SELECT COUNT(*)
FROM stedi.customer_landing
WHERE sharewithresearchasofdate IS NULL;

-- Trusted zone counts
SELECT COUNT(*) FROM stedi.customertrusted;
SELECT COUNT(*) FROM stedi.accelerometer_trusted;
SELECT COUNT(*) FROM stedi.step_trainer_trusted;

-- Customer trusted should have no blank/null shareWithResearchAsOfDate rows
SELECT COUNT(*)
FROM stedi.customertrusted
WHERE sharewithresearchasofdate IS NULL;

-- Curated zone counts
SELECT COUNT(*) FROM stedi.customercurated;
SELECT COUNT(*) FROM stedi.machine_learning_curated;

