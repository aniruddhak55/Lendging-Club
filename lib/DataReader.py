from lib import configreader
""" importing config reader """

def get_customer_schema():
    """ Get Customer Schema """
    customer_schema = "member_id string, emp_title string, emp_length string, \
               home_ownership string, annual_inc float, addr_state string, zip_code string, \
               country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, \
               application_type string, annual_inc_joint float, verification_status_joint string"
    return customer_schema

def get_loans_schema():
    """ get loans schema """
    loan_schema = "loan_id string, member_id string, loan_amount float, funded_amnt float, loan_term_months string, interest_rate float, \
                   monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string"
    return loan_schema

def get_loan_repayment_schema():
    """ get loan repayment schema """
    loan_repayment_schema = "loan_id string, total_principle_received float, total_interest_received float, total_late_fee_received float, \
                             total_payment_received float, last_payment_amount float, last_payment_date string, next_payment_date string"
    return loan_repayment_schema

def get_loan_defaulter_schema():
    """ get loan defaulter schema """
    loan_defaulter_schema = "member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float, \
                             inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"
                                                          
    return loan_defaulter_schema

def read_customer_data(spark, env):
    """ reading customer data file to create customer DF """
    conf = configreader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_customer_schema()) \
        .load(customers_file_path)
        
def read_loan_data(spark, env):
    """ read loan data and create loans df """
    conf = configreader.get_app_config(env)
    loan_file_path = conf["loan.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_loans_schema()) \
        .load(loan_file_path)
        
def read_loan_repayment_data(spark, env):
    """ read loan repayment data and create loans df """
    conf = configreader.get_app_config(env)
    loan_repayment_file_path = conf["loan_repayment.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_loan_repayment_schema()) \
        .load(loan_repayment_file_path)
        
def read_loan_defaulter_data(spark, env):
    """ read loan defaulter data and create loans df """
    conf = configreader.get_app_config(env)
    loan_defaulter_file_path = conf["loan_defaulter.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_loan_defaulter_schema()) \
        .load(loan_defaulter_file_path)
        
def read_bad_customer_data (spark,env):
    conf = configreader.get_app_config(env)
    bad_cus_path=conf["bad.data.csv.customer"]
    return spark.read \
    .option("header", 'True')\
    .format("csv")\
    .load(bad_cus_path)
    
def read_bad_default_delinq_data (spark,env):
    conf = configreader.get_app_config(env)
    bad_delinq_path=conf["bad.data.csv.loan_defaulter_delinq"]
    return spark.read \
    .option("header", 'True')\
    .format("csv")\
    .load(bad_delinq_path)    
    
def read_bad_loan_defau_detail_data (spark,env):
    conf = configreader.get_app_config(env)
    bad_loan_defau_detail_path=conf["bad.data.csv.loan_defaulter_detail"]
    return spark.read \
    .option("header", 'True')\
    .format("csv")\
    .load(bad_loan_defau_detail_path)