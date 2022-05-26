DECLARE adsets_arr array<string>;
DECLARE ads_arr array<string>;
DECLARE adset_len int64;
DECLARE current_key int64 default 0;
DECLARE current_adset string;

-- SCRIPTING QUERY THAT RETURNS THE TOP 3 PERFORMING ADS, AND GENERAL AD SET DATA
-- FROM ANY AD SET THAT HAS SPEND IN THE LAST 14 DAYS (IN THIS CASE) ON FACEBOOK ADS



--A TABLE WITH ADSETS, ADS, SPEND > 0
CREATE OR REPLACE TABLE <dataset>.as_a_s
as SELECT adset_name, ad_name,spend from facebook_14 where spend > 0;

-- CREATE A TABLE WITH THE SPEND OF EACH ADSET
CREATE OR REPLACE TABLE <dataset>.adset_spend
as SELECT adset_name, sum(spend) as adset_spend from `<project_id>.<dataset>.as_a_s` group by (adset_name);


--CREATE AN ARRAY FROM DISTINCT ADSETS, ADS THAT HAVE SOME SPEND
SET adsets_arr = (select ARRAY_AGG(DISTINCT adset_name) from `<project_id>.<dataset>.as_a_s`);
SET ads_arr = (select ARRAY_AGG(DISTINCT ad_name) from `<project_id>.<dataset>.as_a_s`);


--CREATE AND JOIN ALL FACEBOOK DATA, AD SET SPECIFIC DATA, AND KEYS
CREATE OR REPLACE TABLE <dataset>.facebook_indexed AS 
SELECT
facebook_14.adset_name,
facebook_14.ad_name,
facebook_14.spend,
facebook_14.action,
facebook_14.cost_per_action,
`<project_id>.<dataset>.adsets_num`.key_,
`<project_id>.<dataset>.adset_spend`.adset_spend
FROM `<project_id>.<facebook-dataset>.<table>` AS facebook_14
LEFT JOIN  `<project_id>.<dataset>.adsets_num` on facebook_14.adset_name = `<project_id>.<dataset>.adsets_num`.ad_sets
LEFT JOIN `<project_id>.<dataset>.adset_spend` on facebook_14.adset_name = `<project_id>.<dataset>.adset_spend`.adset_name;

-- SET A VALUE OF THE ADSET LENGTH IN ORDER SO THE WHILE LOOP CREATES A TABLE FOR EACH
SET adset_len = (SELECT count(DISTINCT key_) FROM `<project_id>.<dataset>.facebook_indexed`);
SET current_adset = "(select ad_sets from `<project_id>.<dataset>.adsets_num` where key_ = @ck)";



-- WHILE LOOP TO CREATE AN ADSET TABLE FOR EACH ADSET THAT HAS SPEND IN THE LAST 14 DAYS
WHILE current_key < adset_len
DO 
SET current_key = current_key + 1;
EXECUTE IMMEDIATE
"CREATE OR REPLACE TABLE `<project_id>.<dataset>.adset_"||current_key||"` as SELECT * FROM `<project_id>.<dataset>.facebook_indexed` where key_ = @key"
USING current_key as key;
END WHILE;


-- CREATE A REPORT TABLE FOR EACH ADSET (IF YOU ARE TAKING INTO ACCOUNT ANOTHER DATASOURCE)
SET current_key = 0;

WHILE current_key < adset_len
DO 
SET current_key = current_key + 1;
EXECUTE IMMEDIATE
"CREATE OR REPLACE TABLE `<project_id>.<dataset>.adset_"||current_key||"_report`(Adset string,Adname string, Spend string, Results string, Cost_per_Result string, Acquisition string, CostPerAcq string)";
END WHILE;


-- INDEX EACH ADSET TABLE BY AD SPEND, IN ORDER TO LIMIT TO THE TOP THREE ADS
SET current_key = 0;

WHILE current_key < adset_len
DO 
SET current_key = current_key + 1;
EXECUTE IMMEDIATE
"CREATE OR REPLACE TABLE `<project_id>.<dataset>.indexed_adset_"||current_key||"` as (SELECT *, row_number() over() as ad_key FROM `<project_id>.<dataset>.adset_"||current_key||"`)";
END WHILE;



-- INSERT FIRST ROW INTO EACH REPORT DATA OF THE AD WITH THE TOP SPEND
set current_key = 0;

WHILE current_key < adset_len
DO 
SET current_key = current_key + 1;
EXECUTE IMMEDIATE
concat("INSERT INTO `<project_id>.<dataset>.adset_"||current_key||"_report`(Adset, Adname, Spend , Results , Cost_per_Result , Acquisition)","VALUES ((select adset_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 1),(select ad_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 1),concat('$',cast(round((select spend from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 1),2)as string)),(cast((select (select value from unnest(action) where action_type='onsite_conversion.messaging_conversation_started_7d') from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 1)as string)),(concat('$',cast(round((select (select value from unnest(cost_per_action) where action_type='onsite_conversion.messaging_conversation_started_7d') from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 1),2)as string))),cast((select count(ad_name) from `<project_id>.<datasource_2>.all_users` where ad_name = (select ad_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 1))as string))"
);
END WHILE;

-- INSERT SECOND...
set current_key = 0;

WHILE current_key < adset_len
DO 
SET current_key = current_key + 1;
EXECUTE IMMEDIATE
concat("INSERT INTO `<project_id>.<dataset>.adset_"||current_key||"_report`(Adset, Adname, Spend , Results , Cost_per_Result , Acquisition)","VALUES ((select adset_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 2),(select ad_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 2),concat('$',cast(round((select spend from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 2),2)as string)),(cast((select (select value from unnest(action) where action_type='onsite_conversion.messaging_conversation_started_7d') from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 2)as string)),(concat('$',cast(round((select (select value from unnest(cost_per_action) where action_type='onsite_conversion.messaging_conversation_started_7d') from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 2),2)as string))),cast((select count(ad_name) from `<project_id>.<datasource_2>.all_users` where ad_name = (select ad_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 2))as string))"
);
END WHILE;

-- THIRD...
set current_key = 0;

WHILE current_key < adset_len
DO 
SET current_key = current_key + 1;
EXECUTE IMMEDIATE
concat("INSERT INTO `<project_id>.<dataset>.adset_"||current_key||"_report`(Adset, Adname, Spend , Results , Cost_per_Result , Acquisition)","VALUES ((select adset_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 3),(select ad_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 3),concat('$',cast(round((select spend from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 3),2)as string)),(cast((select (select value from unnest(action) where action_type='onsite_conversion.messaging_conversation_started_7d') from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 3)as string)),(concat('$',cast(round((select (select value from unnest(cost_per_action) where action_type='onsite_conversion.messaging_conversation_started_7d') from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 3),2)as string))),cast((select count(ad_name) from `<project_id>.<datasource_2>.all_users` where ad_name = (select ad_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 3))as string))"
);
END WHILE;


-- AND FINALLY, THE DATA OF THE ADSET IN GENERAL
set current_key = 0;

WHILE current_key < adset_len
DO 
SET current_key = current_key + 1;
EXECUTE IMMEDIATE
concat("INSERT INTO `<project_id>.<dataset>.adset_"||current_key||"_report`(Adset, Adname, Spend , Results , Cost_per_Result , Acquisition)","VALUES ((select adset_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 1),('Adset Total'),concat('$',cast(round((select adset_spend from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 1),2)as string)),(cast((select sum((select value from unnest(action) where action_type='onsite_conversion.messaging_conversation_started_7d')) from `<project_id>.<dataset>.indexed_adset_"||current_key||"` limit 1)as string)),(concat('$',cast(round((select avg((select value from unnest(cost_per_action) where action_type='onsite_conversion.messaging_conversation_started_7d')) from `<project_id>.<dataset>.indexed_adset_"||current_key||"` limit 1),2)as string))),cast((select count(adset_name) from `<project_id>.<datasource_2>.all_users` where adset_name = (select adset_name from `<project_id>.<dataset>.indexed_adset_"||current_key||"` where ad_key = 1))as string))"
);
END WHILE;