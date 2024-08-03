import sys
import os
from lib import configreader ,DataReader , utils, DataManipulation,Datawriter
from pyspark.sql.functions import *

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)
job_run_env = sys.argv[1]

print("Creating Spark Session")

spark = utils.get_spark_session(job_run_env)

print("Created Spark Session")

customer_raw_df = DataReader.read_customer_data(spark,job_run_env) 
#customer_raw_df.show()

loans_raw_df = DataReader.read_loan_data(spark, job_run_env) 
#loans_raw_df.show()

loans_repayment_raw_df=DataReader.read_loan_repayment_data(spark,job_run_env) 
#loans_repayment_raw_df.show()

loan_defaulter_raw_df= DataReader.read_loan_defaulter_data(spark,job_run_env) 
#loan_defaulter_raw_df.show()


############################### Cleaning of Customer Data###################################

print("Start processing of customer data")

customer_col_name_change_with_ingest_date=DataManipulation.customer_col_names_changes(customer_raw_df)
#customer_col_name_change_with_ingest_date.show()

customer_remove_null_income=DataManipulation.filter_customer_null(customer_col_name_change_with_ingest_date)
#customer_remove_null_income.show()

cus_fix_emp_length=DataManipulation.fix_employee_length_value(customer_remove_null_income)
#cus_fix_emp_length.show()

customer_final_emp_lenght=DataManipulation.fix_employee_lenght_data_type(cus_fix_emp_length)
#customer_final_emp_lenght.show()

fix_customer_final_emp_length=DataManipulation.fix_employee_lenght_data_type(cus_fix_emp_length)

fix_customer_final_emp_length.createOrReplaceTempView("customers")

avg_emp_length=spark.sql(""" select floor(avg(emp_length)) as avg_emp_length from customers """ ).collect()

avg_emp_duration = avg_emp_length[0][0]

fill_avg_emp_length=DataManipulation.drop_na_values_avg_emp_length(fix_customer_final_emp_length,avg_emp_duration)

final_customer=DataManipulation.fix_state_name(fill_avg_emp_length)

conf = configreader.get_app_config(job_run_env)

write_parquet_path = conf["clean.data.parquet.customer.path"]
write_csv_path = conf["clean.data.csv.customer.path"]

Datawriter.save_final_clean_data(final_customer, write_parquet_path,"parquet")
Datawriter.save_final_clean_data(final_customer, write_csv_path,"CSV")

print("End processing of customer data")

#####################Cleaning of Loans###########################################

print("Start processing of Loans data")

loans_current_time_stamp=DataManipulation.add_ingest_date(loans_raw_df)

columns_to_check=["loan_amount", "funded_amnt", "loan_term_months","interest_rate", "monthly_installment", "issue_date", "loan_status","loan_purpose"]

loans_drop_null_col_values=DataManipulation.drop_null_columns(loans_current_time_stamp,columns_to_check)

loans_change_col_tenure_to_years=DataManipulation.change_loan_tenure_to_years(loans_drop_null_col_values)

final_loans= DataManipulation.change_loan_purpose(loans_change_col_tenure_to_years)

write_loan_parquet_path = conf["clean.data.parquet.loan.path"]
write_loans_csv_path = conf["clean.data.csv.loan.path"]

print(write_loan_parquet_path)

Datawriter.save_final_clean_data(final_loans, write_loan_parquet_path,"parquet")
Datawriter.save_final_clean_data(final_loans, write_loans_csv_path,"CSV")

print("End processing of Loans data")

####################################### Cleaning of Loan Repayments ###########################

print("Start processing of Loans Repayment data")

loans_repayment_current_time_stamp=DataManipulation.add_ingest_date(loans_repayment_raw_df)

loan_repay_col_to_check = ["total_principle_received", "total_interest_received","total_late_fee_received", "total_payment_received", "last_payment_amount"]

loan_repay_drop_null_col_values=DataManipulation.drop_null_columns(loans_repayment_current_time_stamp,loan_repay_col_to_check)

loan_repay_fix_principal_recieved =DataManipulation.loan_repay_fix_principal(loan_repay_drop_null_col_values)

loan_repay_remove_zero_values_total_payment=DataManipulation.loan_repy_remove_zero_total_payment(loan_repay_fix_principal_recieved)

col_value="last_payment_date"

loan_repay_fix_payment_last_date=DataManipulation.loan_repy_fix_invalid_payment_date(loan_repay_remove_zero_values_total_payment,col_value)

col_value_2="next_payment_date"

loan_repay_fix_payment_next_date=DataManipulation.loan_repy_fix_invalid_payment_date(loan_repay_fix_payment_last_date,col_value_2)

write_loan_repay_parquet_path = conf["clean.data.parquet.loan_repayment.path"]
write_loan_repay_csv_path = conf["clean.data.csv.loan_repayment.path"]

Datawriter.save_final_clean_data(loan_repay_fix_payment_next_date, write_loan_repay_parquet_path,"parquet")
Datawriter.save_final_clean_data(loan_repay_fix_payment_next_date, write_loan_repay_csv_path,"CSV")

print("end of Laon repyament")

###########################Cleaning of Loan Defaulters ###################################################

print("Start processing of Loans Defaulters data")

loans_defaulter_current_time_stamp=DataManipulation.add_ingest_date(loan_defaulter_raw_df)

col_val="delinq_2yrs"

loan_defulter_conver_delinq_to_int=DataManipulation.loan_defulter_convert_column_to_int(loans_defaulter_current_time_stamp,col_val)

loan_defulter_conver_delinq_to_int.createOrReplaceTempView("loan_defualters")

loan_defualt_final=spark.sql("""  select  member_id, delinq_2yrs, delinq_amnt, int(mths_since_last_delinq) from loan_defualters                             
                                  where delinq_2yrs >0 or mths_since_last_delinq > 0
                             
                             """)

write_loan_defaulter_parquet_path=conf["clean.data.parquet.loan_defaulter.path"]
write_loan_defaulter_csv_path=conf["clean.data.csv.loan_defaulter.path"]

Datawriter.save_final_clean_data(loan_defualt_final, write_loan_defaulter_parquet_path,"parquet")
Datawriter.save_final_clean_data(loan_defualt_final, write_loan_defaulter_csv_path,"csv")

col_val2="pub_rec"

loan_defulter_conver_pub_rec_to_int=DataManipulation.loan_defulter_convert_column_to_int(loan_defulter_conver_delinq_to_int,col_val2)

col_val3="pub_rec_bankruptcies"

loan_defualter_public_rec_bank_to_int=DataManipulation.loan_defulter_convert_column_to_int(loan_defulter_conver_pub_rec_to_int,col_val3)

col_val4="inq_last_6mths"

loan_defaulter_details=DataManipulation.loan_defulter_convert_column_to_int(loan_defualter_public_rec_bank_to_int,col_val4)

loan_defaulter_details.createOrReplaceTempView("loan_defaulters_details")

loans_def_details=spark.sql("select member_id, delinq_2yrs ,pub_rec, pub_rec_bankruptcies, inq_last_6mths from loan_defaulters_details")

write_loan_defaulter_detail_parquet_path=conf["clean.data.parquet.loan_defaulter_details.path"]
write_loan_defaulter_detail_csv_path=conf["clean.data.csv.loan_defaulter_details.path"]

Datawriter.save_final_clean_data(loan_defualt_final, write_loan_defaulter_detail_parquet_path,"parquet")
Datawriter.save_final_clean_data(loan_defualt_final, write_loan_defaulter_detail_csv_path,"csv")

print("End of Loan defaulters")

##########################
spark.sql("create database if not exists itv012760_ak1_lending_club")

spark.sql("use itv012760_ak1_lending_club ")

ext_customer_table_path = conf["clean.data.parquet.customer.path"]

spark.sql("drop table if exists itv012760_ak1_lending_club.customer")

customer_sql=f""" create external table itv012760_ak1_lending_club.customer(member_id string, 
                 emp_title string, emp_length integer, home_ownership string, annual_income float,
                 address_state string, address_zip_code string, address_country string, grade string, 
                 sub_grade string, verification_status string, total_high_credit_limit float, application_type string ,
                 joint_annual_income float, verification_status_joint string, ingest_date timestamp)
                 stored as parquet
                 location '{ext_customer_table_path}'
              """ 
spark.sql(customer_sql)

#spark.sql("select * from itv012760_lending_club.customer ").show()

spark.sql("drop table if exists itv012760_ak1_lending_club.loan")

ext_loan_table_path=conf["clean.data.parquet.loan.path"]

loan_sql=f"""create external table itv012760_ak1_lending_club.loan 
            (loan_id string, member_id  string, loan_amount float, funded_amnt float,
             loan_term_years integer, interest_rate float, monthly_installment float, 
             issue_date string, loan_status string, loan_purpose string, loan_title string, ingest_date timestamp) 
             stored as parquet 
             location '{ext_loan_table_path}'
          """

spark.sql(loan_sql)

ext_loan_repayment_path= conf["clean.data.parquet.loan_repayment.path"]

spark.sql("drop table  if exists itv012760_ak1_lending_club.loans_repayment")

loan_repay_sql=f"""create external table itv012760_ak1_lending_club.loans_repayment (loan_id string, total_principle_received float,
             total_interest_received float, total_late_fee_received float,total_payment_received float, last_payment_amount float, 
             last_payment_date string, next_payment_date string, ingest_date timestamp)
             stored as parquet 
             location '{ext_loan_repayment_path}' 
             """
             
spark.sql(loan_repay_sql)         

ext_loan_defualter_delinq_path=conf["clean.data.parquet.loan_defaulter.path"]

spark.sql("drop table  if exists  itv012760_ak1_lending_club.loans_defualter_delinq")

loan_defualter_sql=f"""create external table itv012760_ak1_lending_club.loans_defualter_delinq
                      (member_id string, delinq_2yrs integer, delinq_amnt float, mths_since_last_delinq integer)
                      stored as parquet 
                      location '{ext_loan_defualter_delinq_path}' 
                    """ 
spark.sql(loan_defualter_sql) 

ext_loan_defaulter_delinq_detail=conf["clean.data.parquet.loan_defaulter_details.path"]

spark.sql("drop table  if exists  itv012760_ak1_lending_club.loans_defualter_delinq_detail")

loan_defualter_delinq_detail_sql=f""" create external table itv012760_ak1_lending_club.loans_defualter_delinq_detail
                                     (member_id string, delinq_2yrs integer, pub_rec integer, pub_rec_bankruptcies integer, inq_last_6mths integer)
                                     stored as parquet 
                                     location '{ext_loan_defaulter_delinq_detail}' """

spark.sql(loan_defualter_delinq_detail_sql)                                

spark.sql("drop table if exists itv012760_ak1_lending_club.customer_loan_t")

spark.sql(""" create table itv012760_ak1_lending_club.customer_loan_t as select 
l.loan_id,
c.member_id,
c.emp_title,
c.emp_length,
c.home_ownership,
c.annual_income,
c.address_state,
c.address_zip_code,
c.address_country,
c.grade,
c.sub_grade,
c.verification_status,
c.total_high_credit_limit,
c.application_type,
c.joint_annual_income,
c.verification_status_joint,
l.loan_amount,
l.funded_amnt,
l.loan_term_years,
l.interest_rate,
l.monthly_installment,
l.issue_date,
l.loan_status,
l.loan_purpose,
r.total_principle_received as total_principal_received,
r.total_interest_received,
r.total_late_fee_received,
r.last_payment_date,
r.next_payment_date,
d.delinq_2yrs,
d.delinq_amnt,
d.mths_since_last_delinq,
e.pub_rec,
e.pub_rec_bankruptcies,
e.inq_last_6mths from itv012760_ak1_lending_club.customer c
LEFT JOIN  itv012760_ak1_lending_club.loan l ON l.member_id=c.member_id
LEFT JOIN itv012760_ak1_lending_club.loans_repayment r ON l.loan_id = r.loan_id
LEFT JOIN itv012760_ak1_lending_club.loans_defualter_delinq d ON c.member_id = d.member_id
LEFT JOIN itv012760_ak1_lending_club.loans_defualter_delinq_detail e ON c.member_id = e.member_id
""")

bad_customer_records=spark.sql("select member_id from (select count(*) as cnt, member_id  from itv012760_ak1_lending_club.customer c  group by member_id having cnt >1)")

bad_customer_data_path=conf["bad.data.csv.customer"]

Datawriter.save_bad_data(bad_customer_records,bad_customer_data_path,"csv")

########################################################################################################

bad_delinq_recrds_df=spark.sql("""select member_id from(select member_id, count(*)
as total from itv012760_ak1_lending_club.loans_defualter_delinq
group by member_id having total > 1)""")

bad_delinq_defu_path=conf["bad.data.csv.loan_defaulter_delinq"]

Datawriter.save_bad_data(bad_delinq_recrds_df,bad_delinq_defu_path,"csv")

#####################################################################################################
bad_loan_defualter_delinq_detail_records=spark.sql("""select member_id from (select count(*) as cnt, member_id  
                                                      from itv012760_ak1_lending_club.loans_defualter_delinq_detail group by member_id having cnt >1)""")

bad_loan_defaulter_detail_path=conf["bad.data.csv.loan_defaulter_detail"]

Datawriter.save_bad_data(bad_loan_defualter_delinq_detail_records,bad_loan_defaulter_detail_path,"csv")

total_bad_customer_data_df = bad_customer_records.select("member_id") \
.union(bad_loan_defualter_delinq_detail_records.select("member_id")) \
.union(bad_delinq_recrds_df.select("member_id"))

final_bad_customers=total_bad_customer_data_df.distinct()

final_bad_cus=conf["final.bad.csv.customer"]

Datawriter.save_bad_data(final_bad_customers,final_bad_cus,"csv")

final_bad_customers.createOrReplaceTempView("bad_data_customer")

customer_new_clean = spark.sql("""select * from itv012760_ak1_lending_club.customer
where member_id NOT IN (select member_id from bad_data_customer)
""")

clean_cus_data_par_location=conf["new.clean.data.parquet.customer.path"]
clean_cus_data_csv_location=conf["new.clean.data.csv.customer.path"]

Datawriter.save_final_clean_data_with_repart(customer_new_clean,clean_cus_data_csv_location,"csv")
Datawriter.save_final_clean_data_with_repart(customer_new_clean,clean_cus_data_par_location,"parquet")

loans_defaulters_delinq_df = spark.sql("""select * from itv012760_ak1_lending_club.loans_defualter_delinq
where member_id NOT IN (select member_id from bad_data_customer)
""")

clean_delinq_data_par_location=conf["new.clean.data.parquet.defaulter.delinq.path"]
clean_delinq_data_csv_location=conf["new.clean.data.csv.defaulter.delinq.path"]

Datawriter.save_final_clean_data_with_repart(loans_defaulters_delinq_df,clean_delinq_data_csv_location,"csv")
Datawriter.save_final_clean_data_with_repart(loans_defaulters_delinq_df,clean_delinq_data_par_location,"parquet")

loans_defaulters_detail_rec_df = spark.sql("""select * from itv012760_ak1_lending_club.loans_defualter_delinq_detail
where member_id NOT IN (select member_id from bad_data_customer)
""")

clean_delinq__detail_data_par_location=conf["new.clean.data.parquet.loan_defaulter_details.path"]
clean_delinq_detail_data_csv_location=conf["new.clean.data.csv.loan_defaulter_details.path"]

Datawriter.save_final_clean_data_with_repart(loans_defaulters_detail_rec_df,clean_delinq_detail_data_csv_location,"csv")
Datawriter.save_final_clean_data_with_repart(loans_defaulters_detail_rec_df,clean_delinq__detail_data_par_location,"parquet")

new_cus_file_path=conf["new.clean.data.parquet.customer.path"]

spark.sql("drop table if exists itv012760_ak1_lending_club.customer_new ")

create_customer_new_sql=f"""create external table itv012760_ak1_lending_club.customer_new (member_id string, emp_title string, emp_length integer, home_ownership string, annual_income float,
                           address_state string, address_zip_code string, address_country string, grade string, sub_grade string, verification_status string, total_high_credit_limit float, application_type string ,
                           joint_annual_income float, verification_status_joint string, ingest_date timestamp) stored as parquet location '{new_cus_file_path}'
                        """
spark.sql(create_customer_new_sql)                

new_delinq_file_path=conf["new.clean.data.parquet.defaulter.delinq.path"]

spark.sql("drop table if exists itv012760_ak1_lending_club.loans_defualter_delinq_new ")

create_delinq_new_sql=f"""create external table itv012760_ak1_lending_club.loans_defualter_delinq_new
                            (member_id string, delinq_2yrs integer, delinq_amnt float, mths_since_last_delinq integer)
                             stored as parquet 
                             location '{new_delinq_file_path}' 
                        """ 
spark.sql(create_delinq_new_sql)    


new_delinq_detail_file_path=conf["new.clean.data.parquet.loan_defaulter_details.path"]

spark.sql("drop table if exists itv012760_ak1_lending_club.loans_defualter_delinq_detail_new")

create_delinq_detail_new_sql=f""" create external table itv012760_ak1_lending_club.loans_defualter_delinq_detail_new
                                     (member_id string, delinq_2yrs integer, pub_rec integer, pub_rec_bankruptcies integer, inq_last_6mths integer)
                                     stored as parquet 
                                     location '{new_delinq_detail_file_path}'
                            """
spark.sql(create_delinq_detail_new_sql)

unaccepted_rated_pts=conf["spark.sql.unaccepted_rated_points"]
very_bad_rated_pts=conf["spark.sql.very_bad_rated_points"]
bad_rated_pts=conf["spark.sql.bad_rated_points"]
good_rated_pts=conf["spark.sql.good_rated_points"]
very_good_rated_pts=conf["spark.sql.very_good_rated_points"]
excellent_rated_pts=conf["spark.sql.excellent_rated_points"]

ph_sql=f""" SELECT l.member_id,
CASE
WHEN lp.last_payment_amount <= (0.5 * l.monthly_installment) THEN {very_bad_rated_pts}
WHEN lp.last_payment_amount >= (0.5 * l.monthly_installment) AND lp.last_payment_amount <  l.monthly_installment THEN {bad_rated_pts}
WHEN (lp.last_payment_amount = (0.5 * l.monthly_installment)) THEN {good_rated_pts}
WHEN lp.last_payment_amount > (l.monthly_installment)  AND lp.last_payment_amount <= (1.50 * l.monthly_installment) THEN {very_good_rated_pts}
WHEN lp.last_payment_amount > (1.50 * l.monthly_installment) THEN {excellent_rated_pts}
ELSE {unaccepted_rated_pts}
END AS last_payment_pts,
CASE
WHEN lp.total_payment_received > (0.5 * l.funded_amnt) THEN {very_good_rated_pts}
WHEN lp.total_payment_received < (0.5 * l.funded_amnt) THEN {good_rated_pts}
WHEN lp.total_payment_received =0 OR  (lp.total_payment_received) IS NULL THEN {unaccepted_rated_pts}
END as total_payments_pts
from itv012760_ak1_lending_club.loan l
inner join itv012760_ak1_lending_club.loans_repayment lp on l.loan_id=lp.loan_id
"""
ph_df=spark.sql(ph_sql)

ph_df.createOrReplaceTempView("ph_pts")

#spark.sql("select * from ph_pts").show(truncate=False)

ldh_ph_sql=f"""select p.*, 
                        CASE 
                        WHEN d.delinq_2yrs = 0 THEN {excellent_rated_pts} 
                        WHEN d.delinq_2yrs BETWEEN 1 AND 2  THEN {bad_rated_pts} 
                        WHEN d.delinq_2yrs BETWEEN 3 AND 5 THEN {very_bad_rated_pts} 
                        WHEN d.delinq_2yrs > 5  OR d.delinq_2yrs IS NULL THEN {unaccepted_rated_pts} 
                        END as delinq_pts, 
                        CASE 
                        WHEN nd.pub_rec = 0 THEN  {excellent_rated_pts} 
                        WHEN nd.pub_rec BETWEEN 1 AND 2  THEN {bad_rated_pts} 
                        WHEN nd.pub_rec BETWEEN 3 AND 5 THEN {very_bad_rated_pts} 
                        WHEN nd.pub_rec > 5  OR nd.pub_rec IS NULL THEN {very_bad_rated_pts} 
                        END as public_rec_pts , 
                        CASE 
                        WHEN nd.pub_rec_bankruptcies = 0 THEN  {excellent_rated_pts} 
                        WHEN nd.pub_rec_bankruptcies BETWEEN 1 AND 2  THEN {bad_rated_pts} 
                        WHEN nd.pub_rec_bankruptcies BETWEEN 3 AND 5 THEN {very_bad_rated_pts} 
                        WHEN nd.pub_rec_bankruptcies > 5  OR nd.pub_rec_bankruptcies IS NULL THEN {very_bad_rated_pts} 
                        END as public_record_bankruptcies_pts,
                        CASE                        
                        WHEN nd.inq_last_6mths = 0 THEN  {excellent_rated_pts} 
                        WHEN nd.inq_last_6mths BETWEEN 1 AND 2  THEN {bad_rated_pts} 
                        WHEN nd.inq_last_6mths BETWEEN 3 AND 5 THEN {very_bad_rated_pts} 
                        WHEN nd.inq_last_6mths > 5  OR nd.inq_last_6mths IS NULL THEN {very_bad_rated_pts} 
                        END as public_inq_last6_mths_pts                             
                        from ph_pts p 
                        inner join itv012760_ak1_lending_club.loans_defualter_delinq_new d on d.member_id=p.member_id 
                        inner join itv012760_ak1_lending_club.loans_defualter_delinq_detail_new nd on d.member_id =nd.member_id
                        WHERE nd.member_id NOT IN (SELECT b.member_id from bad_data_customer b)
"""

ldh_ph_df=spark.sql(ldh_ph_sql)

ldh_ph_df.createOrReplaceTempView("ldh_ph_pts")

#spark.sql("select * from ldh_ph_pts").show()

fh_ldh_ph_sql = f"""SELECT ldh.*,
CASE 
 WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN {excellent_rated_pts}
 WHEN LOWER(l.loan_status) LIKE '%current%' THEN {good_rated_pts}
 WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN  {bad_rated_pts} 
 WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' THEN {very_bad_rated_pts}
 WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN {unaccepted_rated_pts} 
 ELSE {unaccepted_rated_pts} 
 END as loan_rated_pts,
 CASE
 WHEN LOWER(c.home_ownership) LIKE '%own%' THEN {excellent_rated_pts} 
 WHEN LOWER(c.home_ownership) LIKE '%rent%' THEN {good_rated_pts} 
 WHEN LOWER(c.home_ownership) LIKE '%mortgage%' THEN {bad_rated_pts} 
 WHEN LOWER(c.home_ownership) LIKE '%any%' OR LOWER(c.home_ownership) IS NULL THEN {very_bad_rated_pts} 
 END AS home_pts,
 CASE
 WHEN l.funded_amnt <=(c.total_high_credit_limit * 0.10) THEN {excellent_rated_pts}
 WHEN l.funded_amnt > (c.total_high_credit_limit * 0.10) AND l.funded_amnt <=(c.total_high_credit_limit * 0.20) THEN {very_good_rated_pts}
 WHEN l.funded_amnt > (c.total_high_credit_limit * 0.20) AND l.funded_amnt <=(c.total_high_credit_limit * 0.30)  THEN {good_rated_pts}
 WHEN l.funded_amnt > (c.total_high_credit_limit * 0.30) AND l.funded_amnt <=(c.total_high_credit_limit * 0.50)  THEN {bad_rated_pts}
 WHEN l.funded_amnt > (c.total_high_credit_limit * 0.50) AND l.funded_amnt <=(c.total_high_credit_limit * 0.70)  THEN {very_bad_rated_pts}
 WHEN l.funded_amnt > (c.total_high_credit_limit * 0.70) THEN {unaccepted_rated_pts}
 ELSE {unaccepted_rated_pts} 
 END as credit_rated_pts,
 CASE
 WHEN c.grade = 'A' AND c.sub_grade = 'A1' THEN {excellent_rated_pts}
 WHEN c.grade = 'A' AND c.sub_grade = 'A2' THEN ({excellent_rated_pts} * 0.95)
 WHEN c.grade = 'A' AND c.sub_grade = 'A3' THEN ({excellent_rated_pts} * 0.90)
 WHEN c.grade = 'A' AND c.sub_grade = 'A4' THEN ({excellent_rated_pts} * 0.85) 
 WHEN c.grade = 'A' AND c.sub_grade = 'A5' THEN ({excellent_rated_pts} * 0.80)
 WHEN c.grade = 'B' AND c.sub_grade = 'B1' THEN {very_good_rated_pts}
 WHEN c.grade = 'B' AND c.sub_grade = 'B2' THEN ({very_good_rated_pts} * 0.95)
 WHEN c.grade = 'B' AND c.sub_grade = 'B3' THEN ({very_good_rated_pts} * 0.90)
 WHEN c.grade = 'B' AND c.sub_grade = 'B4' THEN ({very_good_rated_pts} * 0.85)
 WHEN c.grade = 'B' AND c.sub_grade = 'B5' THEN ({very_good_rated_pts} * 0.80)
 WHEN c.grade = 'C' AND c.sub_grade = 'C1' THEN {good_rated_pts}
 WHEN c.grade = 'C' AND c.sub_grade = 'C2' THEN ({good_rated_pts} * 0.95)
 WHEN c.grade = 'C' AND c.sub_grade = 'C3' THEN ({good_rated_pts} * 0.90)
 WHEN c.grade = 'C' AND c.sub_grade = 'C4' THEN ({good_rated_pts} * 0.85)
 WHEN c.grade = 'C' AND c.sub_grade = 'C5' THEN ({good_rated_pts} * 0.80)
 WHEN c.grade = 'D' AND c.sub_grade = 'D1' THEN {bad_rated_pts}
 WHEN c.grade = 'D' AND c.sub_grade = 'D2' THEN ({bad_rated_pts} * 0.95)
 WHEN c.grade = 'D' AND c.sub_grade = 'D3' THEN ({bad_rated_pts} * 0.90)
 WHEN c.grade = 'D' AND c.sub_grade = 'D4' THEN ({bad_rated_pts} * 0.85)
 WHEN c.grade = 'D' AND c.sub_grade = 'D5' THEN ({bad_rated_pts} * 0.80)
 WHEN c.grade = 'E' AND c.sub_grade = 'E1' THEN {very_bad_rated_pts}
 WHEN c.grade = 'E' AND c.sub_grade = 'E2' THEN ({very_bad_rated_pts} * 0.95)
 WHEN c.grade = 'E' AND c.sub_grade = 'E3' THEN ({very_bad_rated_pts} * 0.90)
 WHEN c.grade = 'E' AND c.sub_grade = 'E4' THEN ({very_bad_rated_pts} * 0.85)
 WHEN c.grade = 'E' AND c.sub_grade = 'E5' THEN ({very_bad_rated_pts} * 0.80)
 WHEN c.grade IN ('F', 'G') THEN {unaccepted_rated_pts}
 ELSE {unaccepted_rated_pts}
 END AS grade_pts 
FROM ldh_ph_pts ldh
INNER JOIN itv012760_ak1_lending_club.loan l ON l.member_id = ldh.member_id
INNER JOIN itv012760_ak1_lending_club.customer_new c ON ldh.member_id = c.member_id
WHERE ldh.member_id NOT IN (SELECT b.member_id FROM bad_data_customer b)
"""

fh_ldh_ph_df=spark.sql(fh_ldh_ph_sql)

fh_ldh_ph_df.createOrReplaceTempView("final_data_loan_score")

#spark.sql("select * from final_data_loan_score").show()

loan_score_df=spark.sql(""" SELECT member_id ,
              ((last_payment_pts+ total_payments_pts) *0.20) as payment_histoy_pts,
              ((delinq_pts + public_rec_pts + public_record_bankruptcies_pts + public_inq_last6_mths_pts)* 0.45) as defualter_history_pts,
              ((loan_rated_pts + home_pts + credit_rated_pts + grade_pts)*0.35) as financial_health_pts 
              from final_data_loan_score
"""
)

loan_score_df.show()

final_loan_score= loan_score_df.withColumn("total_loan_score", loan_score_df.payment_histoy_pts + loan_score_df.defualter_history_pts + loan_score_df.financial_health_pts)

final_loan_score.createOrReplaceTempView("loan_score_eval")

spark.sql("select * from loan_score_eval").show()

unacceptable_grade_pts=conf["spark.sql.unacceptable_grade_pts"]
very_bad_grade_pts= conf["spark.sql.very_bad_grade_pts"]
bad_grade_pts= conf["spark.sql.bad_grade_pts"]
good_grade_pts= conf["spark.sql.good_grade_pts"]
very_good_grade_pts= conf["spark.sql.very_good_grade_pts"]

loan_score_final_sql = f"""select ls.*,CASE
WHEN total_loan_score > {very_good_grade_pts} THEN 'A'
WHEN total_loan_score <= {very_good_grade_pts} AND total_loan_score > {good_grade_pts} THEN 'B'
WHEN total_loan_score <= {good_grade_pts} AND total_loan_score > {bad_grade_pts} THEN 'C'
WHEN total_loan_score <= {bad_grade_pts} AND total_loan_score  > {very_bad_grade_pts} THEN 'D'
WHEN total_loan_score <= {very_bad_grade_pts} AND total_loan_score > {unacceptable_grade_pts} THEN 'E'
WHEN total_loan_score <= {unacceptable_grade_pts} THEN 'F'
end as loan_final_grade
from loan_score_eval ls"""

loan_score_final=spark.sql(loan_score_final_sql)

loan_score_final.createOrReplaceTempView("final_loan_scoring_with_grades")

write_final_loan_data=conf["new.loan.scoring.data.with.grades"]

print(write_final_loan_data)

Datawriter.save_final_clean_data(loan_score_final, write_final_loan_data,"parquet")