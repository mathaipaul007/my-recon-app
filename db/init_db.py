from db.base import Base
from dbmodels.User import User
from dbmodels.Client import Client
from dbmodels.UserRoles import UserRoles
from dbmodels.ClientUser import ClientUser
from dbmodels.FileUploads import FileUploads
from dbmodels.AppCfgClients import AppCfgClients
from dbmodels.FileColumnMapping import FileColumnMapping
from dbmodels.UploadRawData import UploadRawData
from dbmodels.ReconReport import ReconReport
from dbmodels.ReconReportBatch import ReconReportBatch
from dbmodels.UploadRawData import UploadRawData
#from dbmodels.ReconRuleCfg import ReconRuleCfg
from dbmodels.ReconRuleScriptCfg import ReconRuleScriptCfg
from dbmodels.ReconRuleWorkflow import Reconworkflow
from dbmodels.ReconRuleWorkflow import ReconRuleWorkflow
from dbmodels.SchedulerJob import SchedulerJob
from db.session import SessionLocal
from sqlalchemy.orm import Session
from sqlalchemy import select, delete
import bcrypt

def init_db():
    Base.metadata.create_all(bind=engine)

def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
def get_setup_users():
    db: Session = SessionLocal()
    try:
        stmt = (
            select(User)
            .where(User.usermail == 'admin@wireconn.com')
        )
        users = db.execute(stmt).scalars().all()
        return users

    finally:
        db.close()

def force_seed_data() -> None:
    print ("force Seed data....")
    users = get_setup_users()
    if(True):
        db: Session = SessionLocal()
        fcm10 = FileColumnMapping(
            client_id="12345",
            recon_file_type="DEDUCTION_GL",
            sheet_idx=0,
            header_row=2,
            json_data=[{"columnname" : "Company", "keyname": "company", "merged" : "yes"},
                       {"columnname" : "Deduction", "keyname": "deduction","merged" : "yes"},
                       {"columnname" : "Settlement Run Number", "keyname": "settlement_no", "merged" : "yes"},
                       {"columnname" : "Employee ID", "keyname": "employee_id"},
                       {"columnname" : "Sum of Result Line Amount", "keyname": "sum_amount"}],
            mandatory_column=[{"columnname" : "employee_id" }]
        )
    try:
        db.add_all([
            fcm10
        ])
        db.commit()
        print("■ Force Seed data inserted")
    finally:
        db.close()

def seed_data() -> None:
    print ("Seed data....")
    users = get_setup_users()
    if(len(users) == 0):
        db: Session = SessionLocal()
        userRole1 = UserRoles(role="ADMIN", descr="Admin")
        userRole2 = UserRoles(role="PROCESSOR", descr="Processor")
        userRole3 = UserRoles(role="CLIENT_USER", descr="Client User")

        fcm1 = FileColumnMapping(
            client_id="12345",
            recon_file_type="INPUTRECON",
            header_row=4,
            json_data=[{"columnname": "Posted", "keyname": "posted", "type":"date"},
                       {"columnname": "Value Date", "keyname": "value_date", "type":"date"},
                       {"columnname": "Description", "keyname": "description"}, {"columnname" : "Debit", "keyname": "debit"},
                       {"columnname" : "Credit", "keyname": "credit"},{"columnname" : "Net", "keyname": "net"},
                       {"columnname": "LBX Validation", "keyname": "lbx_validation"},
                       {"columnname" : "LBX Report Received Date ", "keyname": "lbx_date"},
                       {"columnname" : "Unnamed: 8", "keyname": "u_8"},
                       {"columnname" : "Unnamed: 9", "keyname": "u_9"},{"columnname" : "Unnamed: 10", "keyname": "u_10"}]
        )
        fcm2 = FileColumnMapping(
            client_id="12345",
            recon_file_type="NETPAYREGISTRY",
            sheet_idx=0,
            header_row=2,
            json_data=[{"columnname": "Company", "keyname": "Company", "merged" : "yes"},
                       {"columnname": "Settlement Run", "keyname": "settlement_run", "merged" : "yes"},
                       {"columnname": "Payment Date/Reversal Date", "keyname": "payment_date_reversal_date"},
                       {"columnname": "Check #", "keyname": "Check_Num"},
                       {"columnname": "Employee ID", "keyname": "employee_id"},
                       {"columnname": "Sum of Net Pay", "keyname": "net_pay"}]
        )

        fcm3 = FileColumnMapping(
            client_id="12345",
            recon_file_type="BMOFUNDING",
            sheet_idx=0,
            header_row=11,
            json_data=[{"columnname" : "DETAILS", "keyname": "details" },
                       { "columnname" : "PAY DATE", "keyname": "pay_date","type":"date" },
                       { "columnname" : "Settlement Run No", "keyname": "settlement_no" },
                       { "columnname" : "AMOUNTS", "keyname": "amounts" },
                       { "columnname" : "SUBTOTALS", "keyname": "subtotals" }],
            mandatory_column=[{"columnname" : "amounts" }]
        )

        fcm4 = FileColumnMapping(
            client_id="12345",
            recon_file_type="NBFUNDING",
            sheet_idx=0,
            header_row=11,
            json_data=[{"columnname" : "DETAILS", "keyname": "details" },{"columnname" : "PAY DATE", "keyname": "pay_date","type":"date" },
                       { "columnname" : "Settlement Run Number", "keyname": "settlement_no" },
                       { "columnname" : "Settlement Run No", "keyname": "settlement_no" },{"columnname" : "AMOUNTS", "keyname": "amounts" },
                       { "columnname" : "SUBTOTALS", "keyname": "subtotals" }],
            mandatory_column=[{"columnname" : "amounts" }]
        )

        fcm5 = FileColumnMapping(
            client_id="12345",
            recon_file_type="LOCKBOX",
            sheet_idx=0,
            header_row=11,
            json_data=[{"columnname" : "REPORT DATE", "keyname": "report_date","type":"date"},
                       { "columnname" : "Item Number", "keyname": "item_no"},{"columnname" : "Ref no.", "keyname": "ref_no"},
                       { "columnname" : "Cheque Serial", "keyname": "cheque_serial"},{"columnname" : "Transit Number", "keyname": "transit_no"},
                       { "columnname" : "Check Account", "keyname": "check_acc"},{"columnname" : "Cheque Amount", "keyname": "cheque_amount"},
                       { "columnname" : "COMPANY", "keyname": "company"},{"columnname" : "EIN", "keyname": "ein"},
                       { "columnname" : "FUNDING ADJ", "keyname": "funding_adj"},{"columnname" : "Comments", "keyname": "comment"}],
            mandatory_column=[{"columnname" : "cheque_amount" }]
        )

        fcm6 = FileColumnMapping(
            client_id="12345",
            recon_file_type="STPV",
            sheet_idx=0,
            header_row=0,
            json_data=[{"columnname" : "Transit", "keyname": "transit"},{"columnname" : "Account Number", "keyname": "account_number"},
                       {"columnname" : "Serial Number", "keyname": "serial_number"},{"columnname" : "Issued Amount", "keyname": "issued_amount"},
                       {"columnname" : "Issued Date", "keyname": "issued_date"},{"columnname" : "Payee Name", "keyname": "payee_name"},
                       {"columnname" : "Stop Date", "keyname": "stop_date"},{"columnname" : "Void Date", "keyname": "void_date"}],
            mandatory_column=[{"columnname" : "serial_number"}]
        )

        fcm7 = FileColumnMapping(
            client_id="12345",
            recon_file_type="NETPAYREGISTRY-RS",
            sheet_idx=1,
            header_row=9,
            json_data=[{"columnname" : "Worker", "keyname": "worker"},{"columnname" : "Employee ID", "keyname": "employee_id"},
                       {"columnname" : "Company", "keyname": "company"},
                       {"columnname" : "Lookup Company ID from Payroll Result", "keyname": "lookup_company_id"},
                       {"columnname" : "Pay Group", "keyname": "pay_group"},
                       {"columnname" : "Payroll Result Pay Cycle Type", "keyname": "payroll_pay_cycle_type"},
                       {"columnname" : "Payroll Off-cycle Type", "keyname": "payroll_off_cycle_type"},
                       {"columnname" : "Result Type - Description", "keyname": "result_type_descr"},
                       {"columnname" : "Settlement Run", "keyname": "settlement_run"},{"columnname" : "Period", "keyname": "period"},
                       {"columnname" : "Payment Date/Reversal Date", "keyname": "payment_date_rev_date","type":"date"},
                       {"columnname" : "Reversal Date", "keyname": "rev_date"},{"columnname" : "Gross Pay", "keyname": "gross_pay"},
                       {"columnname" : "Taxes/Deductions", "keyname": "tax_deduction"},{"columnname" : "Net Pay", "keyname": "net_pay"},
                       {"columnname" : "Payment Types", "keyname": "payment_types"},{"columnname" : "Completed Moment", "keyname": "completed_moment"},
                       {"columnname" : "Check #", "keyname": "check_no"}],
            mandatory_column=[{}]
        )

        fcm8 = FileColumnMapping(
            client_id="12345",
            recon_file_type="ACH_REJECT_TRACKING",
            sheet_idx=0,
            header_row=5,
            json_data=[{"columnname" : "DEFR Run Date", "keyname": "run_date","type":"date"},
                       {"columnname" : "Value Date", "keyname": "value_date","type":"date"},
                       {"columnname" : "Type of Reject", "keyname": "type_of_reject"},{"columnname" : "EIN", "keyname": "ein"},
                       {"columnname" : "Dedit/Credit", "keyname": "deb_cred"},{"columnname" : "Amount", "keyname": "amount"},
                       {"columnname" : "Reports updated", "keyname": "reports_updated"},{"columnname" : "SR #", "keyname": "sr_no"},
                       {"columnname" : "T&A Comments", "keyname": "ta_comments"},
                       {"columnname" : "Payroll Action/Comments", "keyname": "payroll_action_comments"}],
            mandatory_column=[{"columnname" : "amount" }]
        )

        fcm9 = FileColumnMapping(
            client_id="12345",
            recon_file_type="HANA_MASTER_BREAK_UP",
            sheet_idx=0,
            header_row=0,
            json_data=[{"columnname" : "Reports Date/ Run Date", "keyname": "report_date"},
                       {"columnname" : "Pay Date", "keyname": "pay_date","type":"date"},{"columnname" : "EIN", "keyname": "ein"},
                       {"columnname" : "EE Name", "keyname": "ee_name"},{"columnname" : "Reason Code", "keyname": "reason_code"},
                       {"columnname" : "Reason", "keyname": "reason"},{"columnname" : "Dr", "keyname": "dr"},
                       {"columnname" : "Cross Charge on", "keyname": "cross_charge","type":"date"},
                       {"columnname" : "Company", "keyname": "company"}],
            mandatory_column=[{"columnname" : "pay_date" }]
        )

        fcm10 = FileColumnMapping(
            client_id="12345",
            recon_file_type="DEDUCTION_GL",
            sheet_idx=0,
            header_row=2,
            json_data=[{"columnname" : "Company", "keyname": "company", "merged" : "yes"},
                       {"columnname" : "Deduction", "keyname": "deduction", "merged" : "yes"},
                       {"columnname" : "Settlement Run Number", "keyname": "settlement_no", "merged" : "yes"},
                       {"columnname" : "Employee ID", "keyname": "employee_id"},
                       {"columnname" : "Sum of Result Line Amount", "keyname": "sum_amount"}],
            mandatory_column=[{"columnname" : "employee_id" }]
        )
        rrc1 = ReconRuleCfg(
            client_id="12345",
            rule_name="RULE1",
            rule_description="Matching BMO-Funding amount with Settltment amount",
            rule_adapter_id="BmoAdviceRule",
            status="A"
        )

        rrc2 = ReconRuleCfg(
            client_id="12345",
            rule_name="RULE2",
            rule_description="Matching BMO-Funding amount with Settltment amount",
            rule_adapter_id="BmoAddRule",
            status="A"
        )

        rrc3 = ReconRuleCfg(
            client_id="12345",
            rule_name="RULE3",
            rule_description="Matching BMO-Funding amount with Settltment amount",
            rule_adapter_id="BmoLockBoxRule",
            status="A"
        )
        try:
            db.add_all([
                Client(id="12345",name="ABCD", descr="ABCD Company",status="A"),
                User(id=1,username="Admin", usermail="admin@wireconn.com", password= hash_password("test123")),
                userRole1, userRole2, userRole3
                ])
            db.commit()
            db.add_all([
                ClientUser(id=1,client_id="12345",user_id=1,role="ADMIN",status="A"),
                fcm1, fcm2, fcm3, fcm4, fcm5, fcm6,fcm7,fcm8,fcm9, fcm10, rrc1, rrc2, rrc3
            ])
            db.commit()
            print("■ Seed data inserted")
        finally:
            db.close()

#drop table application_cfg_clients;
#drop table upload_raw_data;
#drop table file_column_mapping;
#drop table recon_client_users;
#drop table recon_report;
#drop table recon_report_batch;
#drop table file_uploads;
#drop table recon_user_role;
#drop table recon_users;
#drop table recon_rule_cfg_clients;
#drop table recon_clients;