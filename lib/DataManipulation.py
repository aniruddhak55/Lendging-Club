from pyspark.sql.functions import *
from pyspark.sql.functions import col

def customer_col_names_changes(customer_df):
    return customer_df.withColumnRenamed("annual_inc","annual_income")\
        .withColumnRenamed("addr_state","address_state")\
        .withColumnRenamed("zip_code","address_zip_code")\
        .withColumnRenamed("country","address_country")\
        .withColumnRenamed("tot_hi_cred_lim","total_high_credit_limit")\
        .withColumnRenamed("annual_inc_joint","join_annual_income")\
        .withColumn("ingest_date",current_timestamp())       

                
def filter_customer_null(customer_df):
    return customer_df.filter("annual_income is not null")

def fix_employee_length_value(customer_df):
    return customer_df.withColumn("emp_length",regexp_replace(col("emp_length"),"(\D)",""))

def fix_employee_lenght_data_type (customer_df):
    return customer_df.withColumn("emp_length",customer_df.emp_length.cast('int'))

    
def drop_na_values_avg_emp_length(dataframe,avg_length):
    return dataframe.na.fill(avg_length,subset=['emp_length'])
     
def fix_state_name(dataframe):
    return dataframe.withColumn("address_state", when(length(col("address_state"))>2, "NA").otherwise(col("address_state")))

def add_ingest_date(dataframe):
    return dataframe.withColumn("ingest_date",current_timestamp())

def drop_null_columns(dataframe, col_list):
    return dataframe.na.drop(subset=col_list)

def change_loan_tenure_to_years(dataframe):
    return dataframe.withColumn("loan_term_months",((regexp_replace(col("loan_term_months")," months","")).cast("float")/12).cast("int")).withColumnRenamed("loan_term_months","loan_term_years")

loan_purpose_lookup = ["debt_consolidation", "credit_card","home_improvement", "other", "major_purchase", "medical", "small_business","car", "vacation","moving", "house", "wedding", "renewable_energy","educational"]

def change_loan_purpose(dataframe):
    return dataframe.withColumn("loan_purpose",when(col("loan_purpose").isin(loan_purpose_lookup),col("loan_purpose")).otherwise("other"))

def loan_repay_fix_principal(dataframe):
    return dataframe.withColumn("total_payment_received", when(
    (col("total_payment_received")==0.0) & (col("total_principle_received") != 0.0),col("total_principle_received")+col("total_interest_received")+col("total_late_fee_received")).otherwise(col("total_payment_received")))
    
def loan_repy_remove_zero_total_payment(dataframe):
    return dataframe.filter("total_payment_received !=0")

def loan_repy_fix_invalid_payment_date(dataframe,col_value):
    return dataframe.withColumn(col_value, when(col(col_value)==0.0,None).otherwise(col(col_value)))

def loan_defulter_convert_column_to_int(dataframe, col_name):
    return dataframe.withColumn(col_name,col(col_name).cast("integer")).fillna("0",subset=[col_name])
