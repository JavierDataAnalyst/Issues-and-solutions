WITH count_deals AS (SELECT
		 Id,
		 "Main Phone_1" AS "phone",
		 right_string(REPLACE(REPLACE(REPLACE(REPLACE(replace("Main Phone_1", '(', ''), ')', ''), '+', ''), ' ', ''), '-', ''), 10) AS "phone_clean",
		 "Customer Email",
		 "Preferred Name",
		 "Stage",
		 "Sales Rep",
		 "Closing Date",
		 "Pipeline",
		 COUNT(*) OVER(PARTITION BY "Main Phone_1"  ) AS total_deals,
		 ROW_NUMBER() OVER(PARTITION BY "Main Phone_1"  ORDER BY "Closing Date" DESC ) AS rn
FROM  "Deals" 
WHERE	 "Main Phone_1"  IS NOT NULL
 AND	"Pipeline"  NOT IN ( 'Commercial Solar'  )) ,
combined AS (SELECT
		 MAX('Deals') AS "Section",
		 MAX(cd.Id) AS id,
		 MAX(cd."phone") AS "Phone",
		 MAX(CONCAT('1', right_string(REPLACE(REPLACE(REPLACE(REPLACE(replace(cd."phone", '(', ''), ')', ''), '+', ''), ' ', ''), '-', ''), 10))) AS "Botmaker_phone",
		 cd."phone_clean" AS "phone_clean",
		 MAX(cd."Customer Email") AS "customer email",
		 MAX(cd."Preferred Name") AS "Full name",
		 MAX(cd."Stage") AS "stage",
		 MAX(cd."Pipeline") AS "pipeline",
		 MAX(cd."Closing Date") AS "closing date",
		 MAX(s."Sales Team Name") AS "Sales team name",
		 MAX(s."Inactive") AS Inactive_status,
		 MAX(s."Phone Number") AS "phone number"
FROM  count_deals cd
LEFT JOIN "Sales Team" s ON s.Id  = cd."Sales Rep"  
WHERE	 cd.total_deals  = 1
 AND	cd.rn  = 1
GROUP BY  cd."phone_clean" 
UNION ALL
 SELECT
		 MAX('PPS') AS "Section",
		 MAX(pps_clean.Id) AS Id,
		 MAX(pps_clean."phone") AS phone,
		 MAX("Botmaker_phone") AS "Botmaker_phone",
		 "phone_clean" AS "phone_clean",
		 MAX("Customer Email") AS "Customer Email",
		 MAX("preferred name") AS "preferred name",
		 MAX("stage") AS "stage",
		 MAX(pipeline) AS pipeline,
		 MAX("Closing Date") AS "Closing date",
		 MAX(ss."Sales Team Name") AS "Sales team name",
		 MAX(ss."Inactive") AS Inactive_status,
		 MAX(ss."Phone Number") AS "phone number"
FROM  "OQ44-pps_clean" 
LEFT JOIN "Sales Team" ss ON ss.Id  = "OQ44-pps_clean" ."Sales Rep"  
WHERE	 "OQ44-pps_clean" .total_deals  = 1
 AND	"OQ44-pps_clean" .rn  = 1
GROUP BY  "OQ44-pps_clean" ."phone_clean") ,
ranked AS (SELECT
		 "Section",
		 "id",
		 "Phone",
		 "phone_clean",
		 "Botmaker_phone",
		 "customer email",
		 "Full name",
		 "stage",
		 "pipeline",
		 "closing date",
		 "Sales team name",
		 Inactive_status,
		 "phone number",
		 row_number() over(PARTITION BY "phone_clean"  ORDER BY "closing date" DESC ) AS row_num
FROM  combined)
SELECT
		 "Section",
		 "id",
		 "Phone",
		 "phone_clean",
		 "Botmaker_phone",
		 "customer email",
		 "Full name",
		 "stage",
		 "pipeline",
		 "closing date",
		 "Sales team name",
		 "Inactive_status",
		 "phone number"
FROM  ranked 
WHERE	 "row_num"  = 1
ORDER BY "Phone" 
 
