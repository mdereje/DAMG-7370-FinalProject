select * from final_project_silver.chicago_food_inspection limit 10;
select count(inspection_id) from final_project_silver.chicago_food_inspection;
select count(distinct inspection_id) from final_project_silver.chicago_food_inspection;
select count(distinct violation_code) from final_project_silver.chicago_food_inspection;

select * from final_project_silver.dallas_food_inspection limit 10;
select count(inspection_id) from final_project_silver.dallas_food_inspection;
select count(distinct inspection_id) from final_project_silver.dallas_food_inspection;
select count(distinct violation_code) from final_project_silver.dallas_food_inspection;

select * from final_project_gold.dim_inspection limit 10;
select count(inspection_id) from final_project_gold.dim_inspection;
select count(distinct inspection_id) from final_project_gold.dim_inspection;

select * from final_project_gold.dim_violation limit 10;
select count(violation_code) from final_project_gold.dim_violation;
select count(distinct violation_code) from final_project_gold.dim_violation;

select * from final_project_gold.dim_comment limit 10;
select count(comment_key) from final_project_gold.dim_comment;

select * from final_project_gold.dim_establishment limit 10;
select count(establishment_id) from final_project_gold.dim_establishment;
select count(distinct establishment_id) from final_project_gold.dim_establishment;

select * from final_project_gold.fact_food_inspection limit 10;
select count(inspection_key) from final_project_gold.fact_food_inspection;
select count(distinct inspection_key) from final_project_gold.fact_food_inspection;


select * from final_project_gold.dim_establishment where establishment_name in (
"THE LOBBY KITCHEN",
"PHO 888",
"FRESH BREW",
"TRADER JOE'S STORE #860",
"O & M QUICK MART",
"NEW RESTAURANT A",
"NEW RESTAURANT B",
"NEW RESTAURANT C",
"NEW RESTAURANT D"
);

select * from final_project_gold.dim_establishment where establishment_name in (
"STARBUCKS COFFEE #6229 GYM",
"WILLIE'S GYM",
"MAPLE LEAF GYM",
"HENDERSON CHICKEN GYM",
"ROYAL DOLLAR & FOOD GYM");

select * from final_project_gold.fact_food_inspection where establishment_key in (36404, 36403, 36402);

select * from final_project_gold.dim_inspection where inspection_key = 1; -- 58530
select * from final_project_gold.dim_establishment where establishment_key in (36404, 36403, 36402);
-- BRAND BBQ MARKET with 3 license

select * from final_project_silver.chicago_food_inspection where inspection_id = 58530; --1986445
-- 2124478
-- 2220888

select * from final_project_gold.dim_establishment where establishment_id = "c483a960e8a4bc8ffc1f397fb87a1729158f406d852219a9728994ce639462af";

select * from final_project_gold.dim_inspection where inspection_id = 58530;

select * from final_project_silver.chicago_food_inspection where license_number in (1986445, 2124478, 2220888) and establishment_name = 'BRAND BBQ MARKET';