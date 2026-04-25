                    
from datetime import datetime, timedelta
import json
import struct

import pandas as pd
from sqlalchemy import create_engine
# from sqlalchemy_access import pyodbc
import pyodbc
import urllib

from simulate_filetransfer import get_data_for_job_rules, get_filetransfer_cmd, process_jobs


def bytes_to_datetime(bytes_data):
    if isinstance(bytes_data, bytes) and len(bytes_data) == 8:
        days_fraction = struct.unpack('<d', bytes_data)[0]  # Little-endian double
        base = datetime(1899, 12, 30)
        return base + timedelta(days=days_fraction)
    return pd.NaT  # Or handle errors

def read_ms_access_error_db():
 

    # Define the connection string
    # Change the driver name if your check above shows a slightly different version
    db_file = r"\\hpfs\SharedSecure$\Operations\IS\ProductionControl\Mastech Production Turnover\HPPProdIssueLogDB_Sep2024_v1.accdb"
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file};"

    # Establish the connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    sql_Statement = ''' SELECT distinct [Jobname/Title] as job_name, [Failure Reason] as error   FROM issues WHERE [Jobname/Title] in (     'JOBS.DAILY_SFTP_SDS_CLAIMSI_FILE_DOWNLOAD',
'JOBS.DAILY_COPY_SDS_CLAIMSI_TO_USTHP', 'JOBS.DAILY_SFTP_SDS_TO_USTHP_CLAIMSI_UPLOAD', 'JOBS.DAILY_SDS_CLAIMSI_TO_USTHP_ARCHIVE', 'JOBS.DAILY_SFTP_SDS_CLAIMSP_FILE_DOWNLOAD', 'JOBS.DAILY_COPY_SDS_CLAIMSP_TO_USTHP', 'JOBS.DAILY_SFTP_SDS_TO_USTHP_CLAIMSP_UPLOAD', 'JOBS.DAILY_SDS_CLAIMSP_TO_USTHP_ARCHIVE', 'JOBS.DAILY_BAD_NDC_CODE_REPORT', 'JOBS.SFTP_SDS_PROD_JHP_REJECT_DOWNLOAD', 'JOBS.DAILY_SDS_PROD_JHP_REJECT_COPY', 'JOBS.DAILY_IPLUS_SFTP_FTPTODAY_USTHP_277CA_DOWNLOAD', 'JOBS.DAILY_IPLUS_SFTP_FTPTODAY_USTHP_999_DOWNLOAD', 'JOBS.DAILY_COPY_277_999_TO_SDS', 'JOBS.SFTP_SDS_PROD_277_UPLOAD', 'JOBS.SFTP_SDS_PROD_999_UPLOAD', 'JOBS.ARCHIVE_SDS_PROD_OUTBOUND_FILE', 'JOBS.DAILY_MEMBERSHIP_UNZIP_SEGOV', 'JOBS.DAILY_MEMBESHIP_ARCHIVE_MOVE_SEGOV', 'JOBS.DAILY_COPY_DPW_MEMBERSHIP_TO_LZ', 'EVNT.FILE_EDIFECS_CAID_834_DAILY', 'JOBS.DELETE_DAILY_EDIFECS_CAID_834', 'JOBS.INFA.WF_HRP_DAILY_834_CAID_ENROLLMENT_REDESIGN', 'JOBS.INFA.WF_HRP_DAILY_MEDICAID_MACESS_ENROLLMENT', 'JOBS.COPY_SALESFORCE_MACESS_FILES', 'JOBS.DAILY_SALESFORCE_CAID_ENROLLMENT_BUL', 'JOBS.MONTHLY_SALESFORCE_CAID_ENROLLMENT_TPLHMS', 'JOBS.DAILY_SALESFORCE_CAID_MEMBERELIGIBILITYISSUE', 'JOBS.DAILY_SALESFORCE_CAID_ENROLLMENT_AA', 'JOBS.DAILY_SALESFORCE_CAID_ENROLLMENT_TPLMM', 'JOBS.DAILY_SALESFORCE_CHIP_ENROLLMENT_BUL_ERROR', 'JOBS.FEEDER_MEDICAID_834_DONE_COPY', 'JOBS.INFA.WF_HRP_DAILY_ENROLL_PCPAUTO_DRPT_OUTBOUND', 'JOBS.COPY_DAILY_PCPAUTO_RPT', 'JOBS.DAILY_IPLUS_SFTP_FTPTODAY_USTHP_CAID_PCPAUTO_DOWNLOAD', 'JOBS.DAILY_IPLUS_CAID_PCPAUTO_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_CAID_PCPAUTO_FTPTODAY_USTHP_ARCHIVE', 'OBSOLETE.JOBS.INFA.WF_HRCM_DAILY_DEATH_ALERTS_INBOUND', 'JOBS.DAILY_IPLUS_SFTP_FTPTODAY_USTHP_CAID_WLCMKT_DOWNLOAD', 'JOBS.DAILY_IPLUS_CAID_MEMBERKITS_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_CAID_WLCMKT_FTPTODAY_USTHP_ARCHIVE', 'JOBS.INFA.WF_HRP_DAILY_CRSPN_WLCMKT_TRGR_CAID_OUTBOUND', 'JOBS.INFA.WF_HRP_DAILY_CRSPN_WLCMKT_EXTRACT_CAID_OUTBOUND', 'JOBS.DAILY_CAID_MEMBERKITS_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_SFTP_FTPTODAY_USTHP_CAID_ONCONTACT_DOWNLOAD', 'JOBS.DAILY_IPLUS_CAID_ONCONTACT_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_CAID_ONCONTACT_FTPTODAY_USTHP_ARCHIVE', 'JOBS.INFA.WF_HRP_CRSPN_DAILY_ONCNTCT_LTRS_TRIGGER', 'JOBS.INFA.WF_HRP_CRSPN_DAILY_ONCNTCT_LTRS_EXTRACT', 'JOBS.DAILY_IPLUS_SFTP_FTPTODAY_USTHP_CHIP_ONCONTACT_DOWNLOAD', 'JOBS.DAILY_IPLUS_CHIP_ONCONTACT_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_CHIP_ONCONTACT_FTPTODAY_USTHP_ARCHIVE', 'JOBS.INFA.WF_HRP_CRSPN_DAILY_ONCNTCT_CHIP_MEM_CRSPN_TRIGGER.', 'JOBS.INFA.WF_HRP_CRSPN_DAILY_ONCNTCT_CHIP_MEM_CRSPN_EXTRACT', 'JOBS.COPY_ON_CONTACT_MEDICAID_FROM_LZ', 'JOBS.DAILY_COPY_CAID_834_FILES_FOR_USER', 'JOBS.INFA.WF_HRP_CORE_DAILY_NMO_OUTBOUND', 'JOBS.COPY_CAID_NMO_OUTBOUND_FROM_LZ', 'JOBS.INFA.WF_HRP_DAILY_CRSPN_NMO_CHIP_OUTBOUND', 'JOBS.COPY_NMO_OUTBOUND_FROM_LZ', 'JOBS.INFA.WF_HRP_DAILY_CRSPN_NMO_CARE_OUTBOUND', 'JOBS.COPY_CARE_NMO_OUTBOUND_FROM_L', 'JOBS.COPY_NEWBORN_REPORT_FOR_USER', 'JOBS.DAILY_HRDW2IAPP_MEMBERAUTOASSIGN', 'JOBS.MOVE_10TAPE_FD8V_CHIP_MEMBERSHIP_UNZIP_FILE', 'JOBS.DAILY_CHIP_MEMBERSHIP_834_SEGOV_DOWNLOAD', 'JOBS.DAILY_CHIP_MEMBERSHIP_UNZIP_SEGOV', 'JOBS.DAILY_CHIP_MEMBESHIP_ARCHIVE_MOVE_SEGOV', 'JOBS.DAILY_COPY_CHIP_DPW_MEMBERSHIP_TO_LZ', 'EVNT.FILE_EDIFECS_CHIP_834_DAILY', 'JOBS.INFA.WF_HRP_DAILY_834_CHIP_ENROLLMENT_REDESIGN', 'JOBS.INFA.WF_HRP_DAILY_CHIP_MACESS_ENROLLMENT', 'JOBS.DAILY_834_CHIP_LANDING_ZONE_REPORT_COPY', 'JOBS.DAILY_IPLUS_SFTP_FTPTODAY_USTHP_CHIP_WLCMKT_DOWNLOAD', 'JOBS.DAILY_IPLUS_CHIP_MEMBERKITS_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_CHIP_WLCMKT_FTPTODAY_USTHP_ARCHIVE', 'JOBS.INFA.WF_HRP_DAILY_CRSPN_WLCMKT_TRGR_CHIP_OUTBOUND', 'JOBS.INFA.WF_HRP_DAILY_CRSPN_WLCMKT_EXTRACT_CHIP_OUTBOUND', 'JOBS.DAILY_MEMBERKITS_LANDING_ZONE_COPY', 'JOBS.INFA.WF_HRP_DAILY_CHIP_INBOUND_DHS', 'JOBS.DAILY_COPY_CHIP_INBOUND_FILE', 'JOBS.DAILY_SFTP_CHIP_INBOUND_FILE_UPLOAD', 'JOBS.DAILY_ARCHIVE_CHIP_INBOUND_FILE', 'JOBS.INFA.WF_HRP_DAILY_CHIP_ENROLL_PCPAUTO_DRPT_OUTBOUND', 'JOBS.COPY_DAILY_PCPAUTO_RPT_CHIP', 'JOBS.DAILY_IPLUS_SFTP_FTPTODAY_USTHP_CHIP_PCPAUTO_DOWNLOAD', 'JOBS.DAILY_IPLUS_CHIP_PCPAUTO_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_CHIP_PCPAUTO_FTPTODAY_USTHP_ARCHIVE', 'JOBS.FEEDER_CHIP_834_DONE_COPY', 'JOBS.DAILY_MHK_EXTRACTS_DATA_UPLOAD_COPY',
'JOBS.SFTP_MHK_DAILY_DATA_EXTRACTS_UPLOAD', 'JOBS.DAILY_MHK_EXTRACTS_DATA_UPLOAD_ARCHIVE', 'JOBS.SFTP_TDBANK_EBPPR_MEDICARE_DOWNLOAD', 'JOBS.SFTP_TDBANK_LOCKBOX_42946_DOWNLOAD', 'JOBS.COPY_TD_BANK_EBPPR_MEDICARE_HPAPPWORXTS', 'JOBS.ARCHIVE_TD_BANK_EBPPR_MEDICARE', 'JOBS.COPY_TD_BANK_LOCKBOX_42946_HPAPPWORXTS', 'JOBS.RENAME_TD_BANK_LOCKBOX_42946', 'JOBS.SFTP_TDBANK_LOCKBOX_08702_DOWNLOAD', 'JOBS.SFTP_TDBANK_LOCKBOX_42971_DOWNLOAD', 'JOBS.SFTP_TDBANK_EBPPR_CHIP_DOWNLOAD', 'JOBS.COPY_TD_BANK_LOCKBOX_42971_HPAPPWORXTS', 'JOBS.RENAME_TD_BANK_LOCKBOX_42971', 'JOBS.COPY_TD_BANK_LOCKBOX_08702_HPAPPWORXTS', 'JOBS.RENAME_TD_BANK_LOCKBOX_08702', 'JOBS.DAILY_IPLUS_TDBANK_PAYMENT_FILE_LZ_COPY', 'JOBS.DAILY_IPLUS_SFTP_TDBANK_PLBOX_FILE_UPLOAD', 'JOBS.DAILY_IPLUS_TDBANK_PAYMENT_FILE_ARCHIVE', 'JOBS.INFA.WF_HRP_DAILY_BANK_PAYMENT_FILE_INBOUND', 'JOBS.LOCKBOX_INFORMATICA_REPORTS', 'JOBS.COPY_TD_BANK_EBPPR_CHIP_HPAPPWORXTS', 'JOBS.DAILY_IPLUS_TDBANK_EBILL_FILE_LZ_COPY', 'JOBS.DAILY_IPLUS_SFTP_TDBANK_EBILL_FILE_UPLOAD', 'JOBS.DAILY_IPLUS_TDBANK_EBILL_FILE_ARCHIVE', 'JOBS.EBPPR_INFORMATICA_REPORTS', 'JOBS.ARCHIVE_TD_BANK_EBPPR_CHIP', 'JOBS.DAILY_SFTP_KIDZPARTNERS_MEMBER_ERROR_DOWNLOAD', 'JOBS.DAILY_SFTP_KIDZPARTNERS_MEMBER_PAYMENT_DOWNLOAD', 'JOBS.DAILY_SFTP_KIDZPARTNERS_MEMBER_SUMMARY_DOWNLOAD', 'JOBS.DAILY_SFTP_BANK_PAYMENT_AUDIT_BALANCE_REPORT_DOWNLOAD', 'JOBS.DAILY_COPY_KIDZPARTNERS_MEMBER_FILES_AND_BANK_PAYMENT_AUDIT_BALANCE_REPORT', 'JOBS.DAILY_KIDZPARTNERS_MEMBER_FILES_AND_BANK_PAYMENT_AUDIT_BALANCE_REPORT_ARCHIVE', 'OBSOLETE.JOBS.DAILY_MOVE_MEDICARE_CA277_FILES_FROM_EDI_TO_HPSSISINPUT', 'JOBS.DAILY_CMS_FILE_COPY', 'JOBS.MEDICARE_ENCOUNTER_MAO_FILEPROCESSING', 'JOBS.DAILY_EDIFECS_PROD_MEDICARE_DOWNLOAD_COPY_ARCHIVE', 'JOBS.MEDICARE_ENCOUNTER_277_PROCESSING', 'JOBS.TXN_FILE_LOAD', 'JOBS.LOAD_277_DETAILS',
'JOBS.SFTP_CVS_HEALTH_DAILY_CET_MCARE_DOWNLOAD', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_MCARE_DOWNLOAD2', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_RPT_MCARE_DOWNLOAD', 'JOBS.DAILY_CVS_HEALTH_MCARE_COPY_UNZIP_CET_PCT_FILES', 'JOBS.DAILY_CVS_PCT_LOAD', 'JOBS.SFTP_CVS_HEALTH_DAILY_MCARE_PCT_UPLOAD_TO_SPAP', 'JOBS.DAILY_SPAP_MCARE_CET_PCT_ARCHIVE_FILES', 'JOBS.INFA.WF_HRP_DAILY_MEDICARE_CVS_INTEGRATION_INBOUND', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_MCAID_DOWNLOAD', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_MCAID_DOWNLOAD2', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_RPT_MCAID_DOWNLOAD', 'JOBS.DAILY_CVS_HEALTH_MCAID_COPY_UNZIP_CET_PCT_FILES', 'JOBS.HMS_RENAME_OF_CET_DCET3892A_FILE', 'JOBS.HMS_RENAME_OF_CET_DCET3892C_FILE', 'JOBS.HMS_RENAME_OF_CET_MNT412.HPPMC_FILE', 'JOBS.HMS_RENAME_OF_CET_MNT412.HPPMCRJ_FILE', 'JOBS.SFTP_FTP_TODAY_HPHMS_DAILY_CET_PCT_UPLOAD', 'JOBS.DAILY_FTP_TODAY_HPHMS_MCAID_CET_PCT_ARCHIVE_FILES', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_PPO_DOWNLOAD', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_PPO_DOWNLOAD2', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_RPT_PPO_DOWNLOAD', 'JOBS.DAILY_CVS_HEALTH_PPO_COPY_UNZIP_CET_PCT_FILES', 'JOBS.SFTP_PPO_HMO_CVS_PRIME_FILES_UPLOAD', 'JOBS.SFTP_PPO_HMO_CVS_PRIME_RPT_FILES_UPLOAD', 'JOBS.MOVE_ARCHIVE_PRIME_PPO_PCT_FILES', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_ACA_DOWNLOAD', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_ACA_DOWNLOAD2', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_RPT_ACA_DOWNLOAD', 'JOBS.DAILY_CVS_HEALTH_ACA_COPY_UNZIP_CET_PCT_FILES', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_ACA_PA_PPO_DOWNLOAD', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_ACA_PA_PPO_DOWNLOAD2', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_RPT_ACA_PA_PPO_DOWNLOAD', 'JOBS.DAILY_CVS_HEALTH_ACA_PA_PPO_COPY_UNZIP_CET_PCT_FILES', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_MCARE_NJ_PPO_DOWNLOAD', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_MCARE_NJ_PPO_DOWNLOAD2', 'JOBS.SFTP_CVS_HEALTH_DAILY_CET_RPT_MCARE_NJ_PPO_DOWNLOAD', 'JOBS.DAILY_CVS_HEALTH_MCARE_NJ_PPO_COPY_UNZIP_CET_PCT_FILES', 'JOBS.SFTP_MCARE_NJ_PPO_CVS_PRIME_PCT_FILES_UPLOAD', 'JOBS.SFTP_MCARE_NJ_PPO_CVS_PRIME_RPT_FILES_UPLOAD', 'JOBS.DAILY_PRIME_MCARE_NJ_PPO_CET_PCT_ARCHIVE_FILES', 'JOBS.COPY_PROD_CVS_FILES_TO_HPQASSIS01',
'JOBS.DAILY_IPLUS_SFTP_ACA_834_BALANCE_REPORT_DOWNLOAD', 'JOBS.DAILY_IPLUS_SFTP_ACA_834_LOAD_STATUS_DOWNLOAD', 'JOBS.DAILY_IPLUS_SFTP_ACA_834_RESPONSE_FILE_DOWNLOAD', 'JOBS.DAILY_IPLUS_ACA_834_REPORT_FILES_COPY_ARCHIVE', 'JOBS.DAILY_IPLUS_SFTP_ACA_ENROLL_FILE_DOWNLOAD', 'JOBS.DAILY_IPLUS_SFTP_ACA_BULL_ENROLL_FILE_DOWNLOAD', 'JOBS.DAILY_IPLUS_ACA_ENROLL_FILES_COPY_ARCHIVE', 'JOBS.COPY_ACA_SALESFORCE_ENROLLMENT_FILES', 'JOBS.DAILY_ACA_SALESFORCE_ENROLLMENT_BUL', 'JOBS.COPY_VISION_DIRECTORY_FILES_TO_SSIS01', 'JOBS.COPY_PHARMACY_DIRECTORY_FILES_TO_SSIS01', 'JOBS.COPY_DENTAL_DIRECTORY_FILES_TO_SSIS01', 'JOBS.COPY_BEHAVIORAL_HEALTH_DIRECTORY_FILES_TO_SSIS01', 'JOBS.COPY_INTELLICRED_DIRECTORY_FILES_TO_SSIS01', 'JOBS.IICS.TF_MDM_PROVIDER_LOAD_START', 'JOBS.IICS.TF_MDM_PROVIDER_PBS_LANDING_LOAD', 'JOBS.INFA.WF_MDM_MELISSA_ADDRESS_VALIDATION', 'JOBS.IICS.TF_MDM_PROVIDER_PBS_LANDING_LOAD_2', 'JOBS.IICS.TF_MDM_PROVIDER_POST_LANDING_LOOKUP_LOAD', 'JOBS.IICS.TF_MDM_PROVIDER_EXPORT_HCC_ID', 'JOBS.IICS.TF_MDM_PROVDER_BO_DATA_LOAD', 'JOBS.IICS.TF_MDM_PROVIDER_ABC_LOAD', 'JOBS.INFA.WF_MDM_PROVIDER_BO_FLAT_LOAD', 'JOBS.COPY_PCMH_FILES_TO_SSIS01', 'JOBS.LOAD_VENDOR_DIRECTORY_FILES', 'JOBS.INFA.WF_PROVIDERS_DIRECTORY', 'JOBS.INFA.WF_PROV_DIR_MELISSA_ADDR_VLDTN', 'JOBS.INFA.WF_PROVIDER_DIRECTORY_POST_MELADDR_PROCESS', 'JOBS.BUILD_PORTAL_DIRECTORY_DATA', 'JOBS.DAILY_SFTP_ZELIS_CLAIMSI_FILE_DOWNLOAD', 'JOBS.DAILY_COPY_ZELIS_CLAIMSI_FTPTODAY_USTHP', 'JOBS.DAILY_SFTP_FTPTODAY_USTHP_CLAIMSI_UPLOAD', 'JOBS.DAILY_ZELIS_CLAIMSI_FTPTODAY_USTHP_ARCHIVE', 'JOBS.DAILY_SFTP_ZELIS_CLAIMSP_FILE_DOWNLOAD', 'JOBS.DAILY_COPY_ZELIS_CLAIMSP_FTPTODAY_USTHP', 'JOBS.DAILY_SFTP_FTPTODAY_USTHP_CLAIMSP_UPLOAD', 'JOBS.DAILY_ZELIS_CLAIMSP_FTPTODAY_USTHP_ARCHIVE', 'JOBS.DAILY_SFTP_FTPTODAY_USTHP_TRIGGER_UPLOAD', 'JOBS.DAILY_SFTP_FTPTODAY_USTHP_837_CLAIMSI_DOWNLOAD', 'JOBS.DAILY_FTPTODAY_USTHP_CLAIMSI_ZELIS', 'JOBS.DAILY_SFTP_ZELIS_837_CLAIMSI_FILE_UPLOAD', 'JOBS.DAILY_FTPTODAY_USTHP_CLAIMSI_ZELIS.ARCHIVE', 'JOBS.DAILY_SFTP_FTPTODAY_USTHP_837_CLAIMSP_DOWNLOAD', 'JOBS.DAILY_FTPTODAY_USTHP_837_CLAIMSP_ZELIS_COPY', 'JOBS.DAILY_SFTP_837_ZELIS_CLAIMSP_FILE_UPLOAD', 'JOBS.DAILY_FTPTODAY_USTHP_837_CLAIMSP_ZELIS.ARCHIVE', 'JOBS.DAILY_IPLUS_SFTP_FTPTODAY_ZELIS_AGING_REPORT_DOWNLOAD', 'JOBS.DAILY_COPY_FTPTODAY_ZELIS_AGING_REPORT', 'JOBS.DAILY_SFTP_FTPTODAY_USTHP_ZELIS_CLAIMSP_DOWNLOAD', 'JOBS.DAILY_SFTP_FTPTODAY_USTHP_ZELIS_CLAIMSI_DOWNLOAD', 'JOBS.DAILY_FTPTODAY_USTHP_ZELIS_FILES_COPY', 'JOBS.DAILY_SFTP_ZELIS_CLAIMSP_FILE_UPLOAD', 'JOBS.DAILY_SFTP_ZELIS_CLAIMSI_FILE_UPLOAD', 'JOBS.DAILY_FTPTODAY_USTHP_ZELIS_FILES_ARCHIVE', 'JOBS.DAILY_SFTP_ZELIS_CLAIMP_TO_IPLUS_DOWNLOAD', 'JOBS.DAILY_SFTP_ZELIS_CLAIMI_TO_IPLUS_DOWNLOAD', 'JOBS.DAILY_ZELIS_CLAIMS_TO_IPLUS_FILES_COPY', 'JOBS.DAILY_IPLUS_CLAIMSP_UPLOAD_FROM_ZELIS', 'JOBS.DAILY_IPLUS_CLAIMSI_UPLOAD_FROM_ZELIS', 'JOBS.DAILY_ZELIS_CLAIMS_FILES_TO_IPLUS_ARCHIVE', 'JOBS.CAID_DHS_COPY_FIRST_820FILE_TO_LZ', 'JOBS.CHIP_DHS_COPY_FIRST_820FILE_TO_LZ',
'JOBS.MONTHLY_IPLUS_CHIP_820_REMITTANCE_FILE_MOVE_TO_USTHP', 'JOBS.MONTHLY_IPLUS_USTHP_CHIP_820_REMITTANCE_FILE_UPLOAD', 'JOBS.MONTHLY_IPLUS_USTHP_CHIP_820_REMITTANCE_FILE_ARCHIVE', 'JOBS.CAID_DHS_COPY_SECOND_820FILE_TO_LZ', 'JOBS.MONTHLY_IPLUS_CAID_820_REMITTANCE_FILE_MOVE_TO_USTHP', 'JOBS.MONTHLY_IPLUS_USTHP_CAID_820_REMITTANCE_FILE_UPLOAD', 'JOBS.MONTHLY_IPLUS_USTHP_CAID_820_REMITTANCE_FILE_ARCHIVE', 'JOBS.DELAY_2_HOURS', 'JOBS.MONTHLY_IPLUS_USTHP_CAID_820_REMITTANCE_FILE_DOWNLOAD', 'JOBS.MONTHLY_IPLUS_USTHP_CAID_820_REMITTANCE_COPY', 'JOBS.MONTHLY_IPLUS_USTHP_CHIP_820_REMITTANCE_FILE_DOWNLOAD', 'JOBS.MONTHLY_IPLUS_USTHP_CHIP_820_REMITTANCE_FILE_COPY', 'JOBS.MONTHLY_IPLUS_USTHP_CAID_820_REMITTANCE_SET1_MOVE', 'JOBS.CAID_DHS_820_REMITTANCE_FILE_COPY_FROM_EDIFECS', 'JOBS.INFA.WF_DAWN_RUN_ON_DEMAND_S_M_ETL_REMITTANCE_CAID', 'JOBS.MONTHLY_820_DPWREMIT', 'JOBS.CAID_DHS_820_REMITTANCE_FILE_COPY_AND_ARCHIVE', 'JOBS.MONTHLY_REMITTANCE_820_FILE_VERIFICATION_EMAIL', 'JOBS.MONTHLY_IPLUS_USTHP_CAID_820_REMITTANCE_SET2_MOVE', 'OBSOLETE.JOBP.MONTHLY_EDIFECS_RECIPIENTID_MEMBERHCCID_PROCESS', 'JOBS.CHIP_DHS_820_REMITTANCE_FILE_COPY_FROM_EDIFECS', 'JOBS.MONTHLY_PREMIUM_INVOICE_LD', 'JOBS.CHIP_DHS_820_REMITTANCE_FILE_COPY_AND_ARCHIVE', 'JOBS.MONTHLY_REMITTANCE_820_CHIP_FILE_VERIFICATION_EMAIL', 'JOBS.HCFAM00170503', 'JOBS.HCMEM706', 'JOBS.COPY_CHIP_PREMIUM_BILLING_FILES_TO_SSIS01', 'JOBS.CHIP_PREMIUM_BILLING_INVOICES', 'JOBS.COPY_MP_PREMIUM_BILLING_FILES_TO_SSIS01', 'JOBS.MEDICARE_PREMIUM_BILLING_INVOICES', 'JOBS.SFTP_TDBANK_MEDICARE_INVOICE_UPLOAD', 'JOBS.MEDICARE_PREMIUM_BILLING_INVOICE_ARCHIVE', 'JOBS.MONTHLY_MEMBERSHIP_UNZIP_SEGOV', 'JOBS.MONTHLY_MEMBERSHIP_ARCHIVE_MOVE_SEGOV', 'EVNT.FILE_EDIFECS_CAID_834_45_MONTHLY', 'EVNT.FILE_EDIFECS_CAID_834_3B_MONTHLY', 'EVNT.FILE_EDIFECS_CAID_834_3F_MONTHLY', 'EVNT.FILE_EDIFECS_CAID_834_3J_MONTHLY', 'EVNT.FILE_EDIFECS_CAID_834_3L_MONTHLY', 'JOBS.DELAY_10_MIN', 'JOBS.INFA.WF_HRP_MONTHLY_834_CAID_ENROLLMENT_REDESIGN', 'JOBS.INFA.WF_HRP_MONTHLY_MACESS_ENROLLMENT', 'JOBS.INFA.WF_HRP_MONTHLY_CAID_MACESS_ENROLLMENT', 'JOBS.DELETE_MONTHLY_EDIFECS_CAID_834', 'JOBS.MONTHLY_COPY_CAID_834_FILES_FOR_USER', 'JOBS.COPY_CAID_MONTHLY_MACESS_SALESFORCE_FILES', 'JOBS.MONTHLY_SALESFORCE_CAID_BUL_RPLYLIST', 'JOBS.MOVE_10TAPE_FM8V_CHIP_MEMBERSHIP_FILE', 'JOBS.MONTHLY_CHIP_MEMBERSHIP_834_SEGOV_DOWNLOAD', 'JOBS.COPY_OF_MONTHLY_CHIP_834_FILE_UST_NEW', 'JOBS.CHIP_MONTHLY_MEMBERSHIP_ARCHIVE_MOVE_SEGOV', 'JOBS.DELAY_30_MIN', 'JOBS.INFA.WF_HRP_MONTHLY_834_CHIP_ENROLLMENT_REDESIGN', 'JOBS.INFA.WF_HRP_MONTHLY_CHIP_MACESS_ENROLLMENT', 'JOBS.MONTHLY_CHIP_834_MISMATCH_REPORT_COPY',
'JOBS.MONTHLY_CHIP_SALESFORCE_ENROLLMENT_FILE_COPY', 'JOBS.MONTHLY_CHIP_SALESFORCE_ENROLLMENT_BAT', 'JOBS.CAPITATION_EXTRACT', 'JOBS.UDDB_FUNDDATA2SAS', 'JOBS.MONTHLY_COPY_HPIMEDICAID_HPISCHIP_FOR_CAPITATION', 'JOBS.DAILY_COPY_CAID_BONUS_PYMNT_TO_USTHP', 'JOBS.DAILY_SFTP_CAID_BONUS_PYMNT_TO_USTHP_UPLOAD', 'JOBS.DAILY_USTHP_CAID_BONUS_PYMNT_FILE_ARCHIVE', 'JOBS.DAILY_COPY_CARE_BONUS_PYMNT_TO_USTHP', 'JOBS.DAILY_SFTP_CARE_BONUS_PYMNT_TO_USTHP_UPLOAD', 'JOBS.DAILY_USTHP_CARE_BONUS_PYMNT_FILE_ARCHIVE', 'JOBS.DAILY_IPLUS_USTHP_CAID_BONUS_PYMNT_DOWNLOAD', 'JOBS.DAILY_IPLUS_CAID_BONUS_PYMNT_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_CAID_BONUS_PYMNT_USTHP_ARCHIVE', 'JOBS.DAILY_IPLUS_USTHP_CARE_BONUS_PYMNT_DOWNLOAD', 'JOBS.DAILY_IPLUS_CARE_BONUS_PYMNT_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_CARE_BONUS_PYMNT_USTHP_ARCHIVE', 'JOBS.DAILY_IPLUS_USTHP_CHIP_BONUS_PYMNT_DOWNLOAD', 'JOBS.DAILY_IPLUS_CHIP_BONUS_PYMNT_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_CHIP_BONUS_PYMNT_USTHP_ARCHIVE', 'JOBS.DAILY_IPLUS_USTHP_ACA_BONUS_PYMNT_DOWNLOAD', 'JOBS.DAILY_IPLUS_ACA_BONUS_PYMNT_LANDING_ZONE_COPY', 'JOBS.DAILY_IPLUS_ACA_BONUS_PYMNT_USTHP_ARCHIVE', 'JOBS.SFTP_ECHO_MONTHLY_BONUS_FILES_UPLOAD', 'JOBS.MONTHLY_ECHO_BONUS_FILES_ARCHIVE', 'JOBS.MHK_MATERNITY_BILLING_OUTBOUND', 'JOBS.WEEKLY_MATERNITY_COPY_ARCHIVE_837P_FROM_LZ', 'EVNT.WEEKLY_MATERNITY_FINANCE_VALIDATION', 'JOBS.COPY_MATERNITY_VERIFICATION_FILE_LZ', 'EVNT.WEEKLY_MATERNITY_EDIFECS_837_CREATED_VERIFICATION', 'EVNT.WEEKLY_MATERNITY_EDIFECS_837_3B_CREATED_VERIFICATION', 'EVNT.WEEKLY_MATERNITY_EDIFECS_837_3F_CREATED_VERIFICATION', 'EVNT.WEEKLY_MATERNITY_EDIFECS_837_3J_CREATED_VERIFICATION', 'EVNT.WEEKLY_MATERNITY_EDIFECS_837_3L_CREATED_VERIFICATION', 'JOBS.COPY_MATERNITY_EDIFECS_FILE', 'JOBS.SFTP_DHS_WEEKLY_MATERNITY_837_UPLOAD', 'JOBS.SFTP_DHS_WEEKLY_MATERNITY_837_3B_UPLOAD', 'JOBS.SFTP_DHS_WEEKLY_MATERNITY_837_3F_UPLOAD', 'JOBS.SFTP_DHS_WEEKLY_MATERNITY_837_3J_UPLOAD', 'JOBS.SFTP_DHS_WEEKLY_MATERNITY_837_3L_UPLOAD', 'JOBS.WEEKLY_MATERNITY_837_VERIFICATION', 'JOBS.WEEKLY_MATERNITY_837_ARCHIVE', 'EVNT.FILE.WEEKLY_MATERNITY_REMIT', 'JOBS.WEEKLY_MATERNITY_REMIT_UNZIP', 'JOBS.INFA.WF_HRP_WEEKLY_DHS_835_CAID_INBOUND', 'JOBS.WEEKLY_COPY_MATERNITY_REMIT_FILE', 'JOBS.WEEKLY_DPWMATERNITY_835', 'JOBS.WEEKLY_MATERNITY_REMIT_FILE_APPLY', 'JOBS.WEEKLY_835_CAID_COUNTS',
 )'''
    # Execute a query
    #cursor.execute( sql_Statement )
    #cursor.execute("SELECT * FROM issues ")

    # Iterate through results
    #for row in cursor.fetchall():
    #    print(f"{row[0]},{row[1]},{row[2]}")
    #    break
    #column_names = [column[0] for column in cursor.description]

    # 2. Iterate through the rows
    #for row in cursor.fetchall():
    #    # 3. Zip the column names with the row values to create a clean display
    #    print("--- New Row ---")
    #    for name, value in zip(column_names, row):
    #        print(f"{name}: {value}")
    #    break
    
    df = pd.read_sql(sql_Statement, conn)
    df['error'] = df['error'].fillna("")
    # 2. Group by the job name and join the errors with a pipe
    result = df.groupby('job_name')['error'].apply(lambda x: ' | '.join(x)).reset_index()

    # 3. View or export the result
    #print(result)
    result.to_csv("grouped_errors.csv", index=False)     

    # Cleanup
    cursor.close()
    conn.close()    

def read_ms_db_table(table,db_path,colum_name):
    ls_data = []
     #db_path = r'C:\Users\YourName\Documents\MyDatabase.accdb'
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'

    # Establish connection
    conn = pyodbc.connect(conn_str)

    # Use pandas to read the SQL query directly
    sql_query = f"SELECT * FROM {table}"
    df = pd.read_sql(sql_query, conn)

    # Close connection
    conn.close()
    
    # Now you have a DataFrame!
    #print(df.head())
    for row in df.itertuples(index=False):
        value = getattr(row, colum_name)
        ls_data.append(value)

    return ls_data

def read_ms_db_workflow_table(db_path,table="workflows"):
    ls_data = []
     #db_path = r'C:\Users\YourName\Documents\MyDatabase.accdb'
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'

    # Establish connection
    conn = pyodbc.connect(conn_str)

    # Use pandas to read the SQL query directly
    sql_query = f"SELECT * FROM {table} where is_active = True"
    df = pd.read_sql(sql_query, conn)

    # Close connection
    conn.close()
    
    # Now you have a DataFrame!
    #print(df.head())
    for row in df.itertuples(index=False):
    
    #for index, row in df.iterrows():
        
        workflow_id = row.workflow_id #getattr(row, "id")
        object_name = getattr(row, "object_name")
        object_type = getattr(row, "object_type")
        data ={"workflow_id":workflow_id,"object_name":object_name, "object_type":object_type}
        ls_data.append(data)

    return ls_data

def read_ms_db_by_query (db_path, sql_query, reqired_columns:list = None, params = None):
    ls_data = []
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'

    # Establish connection
    conn = pyodbc.connect(conn_str)

    # Use pandas to read the SQL query directly     
    if params:
        df = pd.read_sql(sql_query, conn, params=params)
    else:
        df = pd.read_sql(sql_query, conn)

    # Close connection
    conn.close()
    
    # Now you have a DataFrame!
    #print(df.head())
    for row in df.itertuples(index=False):    
    #for index, row in df.iterrows():        
        data_out = {}
        for col in reqired_columns:
            data_out[col] = getattr(row, col)                   
        ls_data.append(data_out)

    return ls_data

def read_ms_db_by_query_with_groupby (db_path, sql_query, reqired_columns:list = None,group_by_coll=None,order_col=None):
    ls_data = []
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'

    # Establish connection
    conn = pyodbc.connect(conn_str)

    # Use pandas to read the SQL query directly     
    df = pd.read_sql(sql_query, conn)

    # Close connection
    conn.close()
    
    # Now you have a DataFrame!
    #print(df.head())
    for row in df.itertuples(index=False):    
    #for index, row in df.iterrows():
        
        data_out = {}
        for col in reqired_columns:
            data_out[col] = getattr(row, col)                   
        ls_data.append(data_out)
    
    grouped_data = {}

 

    for workflow_run_id, group_df in df.groupby(group_by_coll, sort=False):
        grouped_data[workflow_run_id] = group_df.to_dict(orient="records")

    return grouped_data
 

def add_data_to_job_stats(data,db_file):


    #db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_support_modern.accdb"
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file};"
    # Create a connection string for SQLAlchemy
    quoted_conn_str = urllib.parse.quote_plus(conn_str)
    engine = create_engine(f"access+pyodbc:///?odbc_connect={quoted_conn_str}")

    # Convert list of dicts to a DataFrame
    df = pd.DataFrame(data)
    df = df.rename(columns={'name': 'object_name'})
    # Write to Access in one line
    df.to_sql('job_stats', engine, if_exists='append', index=False)

def upsert_data_to_job_stats(data_list,db_file):

    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file};"
    # Create a connection string for SQLAlchemy
    quoted_conn_str = urllib.parse.quote_plus(conn_str)
    engine = create_engine(f"access+pyodbc:///?odbc_connect={quoted_conn_str}")

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        #print_dict_list_table(data_list)
        #print(data_list)
        for entry in data_list:
            workflow_run_id = entry['workflow_run_id']
            run_id = entry['run_id']
            new_status = entry['status']
            
            # --- STEP 1: CHECK IF RECORD EXISTS ---
            cursor.execute("SELECT [Status] FROM job_stats WHERE workflow_run_id = ? and run_id = ?", (workflow_run_id,run_id))
            row = cursor.fetchone()

            if row:
                # --- STEP 2: UPDATE LOGIC (With Conditions) ---
                current_status = row[0]
                 
                # if current_status == 'Active':
                #     
                #     continue 
                
                # CONDITION: Only update if the status has actually changed 
                if int(current_status) != new_status:
                #if True:

                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                       
                    sql_update = "UPDATE job_stats SET Status = ?,status_text=? ,start_time=?, end_time = ? ,activator=?,activator_id =? , estimated_runtime= ? , runtime =?,ert_analysis_result=?  WHERE run_id = ?"
                    cursor.execute(sql_update, (entry.get("status"),entry.get("status_text"),entry.get("start_time"),entry.get("end_time"),
                                                entry.get("activator"),entry.get("activator_id"),entry.get("estimated_run_time"),entry.get("real_run_time"),
                                                 entry.get("ert_analysis_result"), run_id))
                    print(f"Updated ID {run_id}  from {current_status} to {new_status}.")
                    
                else:
                    print(f"No change for ID {run_id}. Skipping.")

            else:
                # --- STEP 3: INSERT LOGIC ---
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                sql_insert = "INSERT INTO job_stats ( workflow_run_id,parent_run_id,run_id,object_type,object_name,status,status_text,start_time,end_time,activator,activator_id,estimated_run_time,real_run_time,ert_analysis_result ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                cursor.execute(sql_insert, (entry.get("workflow_run_id"), entry.get("parent_run_id"), entry.get("run_id"), entry.get("object_type"), entry.get("object_name"), entry.get("status"), entry.get("status_text"), entry.get("start_time"), entry.get("end_time"), entry.get("activator"), entry.get("activator_id"), entry.get("estimated_run_time"), entry.get("real_run_time"), entry.get("ert_analysis_result")))
                print(f"Inserted new record for ID {run_id}.")

        # 4. Save all changes
        conn.commit()

    except Exception as e:
        print(f"upsert_data_to_job_stats - Error: {e}")
        conn.rollback() # Undo changes if something goes wrong
    finally:
        conn.close()


def get_last_completed_workflow_stats_from_db(db_path,workflow_id):
     
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        status ="Completed"
        # Use '?' as a placeholder
        #FORMAT([start_time], 'yyyy-mm-dd hh:nn:ss') as job_start_time // run_id, start_time, workflow_id,analysis_status
        sql = "SELECT TOP 1 run_id,workflow_id , analysis_status,start_time  FROM workflow_stats where workflow_id = ? and analysis_status= ? and start_time is not null  order by start_time desc"
        #sql = 'SELECT  run_id,workflow_id ,FORMAT(start_time, "yyyy-mm-dd hh:nn:ss") AS job_start_time  FROM workflow_stats where workflow_id = ? '
        #sql = 'SELECT  run_id,workflow_id,CStr(start_time) as job_start_time  FROM workflow_stats'
        #sql = "SELECT  run_id,workflow_id , analysis_status, start_time  FROM workflow_stats where workflow_id = ? and analysis_status= ?  order by start_time desc"
        
        cursor.execute(sql,(workflow_id,status,))

        row = cursor.fetchone()

        # 2. CHECK: No Data Found Scenario
        if row is None:
            print(f"⚠️ No records found for status: {status}")
            return None

        columns = [column[0] for column in cursor.description]
        data_dict = dict(zip(columns, row))

        # 4. CLEANUP: Handle any remaining byte data (Date/Time Extended)
        for key, value in data_dict.items():
            if isinstance(value, bytes):
                try:
                    # Attempt to decode the Access internal byte format
                    data_dict[key] = value.decode('utf-8').strip('\x00')
                except:
                    data_dict[key] = str(value) # Fallback to string representation

        return data_dict

    except Exception as e:
        print(f"get_run_id_stats_from_db - Error: {e}")
        raise 
    finally:
        conn.close()


def get_last_completed_workflow_stats_from_db_df(db_path,workflow_id):
     
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'
    try:
        conn = pyodbc.connect(conn_str)
    

        # Get a list of all ODBC drivers installed on your system
        all_drivers = [driver for driver in pyodbc.drivers()]

        print("--- Installed ODBC Drivers ---")
        for driver in all_drivers:
            print(f"Found: {driver}")

        # Check specifically for the MS Access drivers
        access_drivers = [d for d in all_drivers if 'Access' in d]

        print("\n--- MS Access Specific Check ---")
        if not access_drivers:
            print("❌ NO MS Access drivers found! You need to install the Microsoft Access Database Engine.")
        else:
            for d in access_drivers:
                print(f"✅ Available: {d}")
        status ="Completed"
        # Use '?' as a placeholder
        #FORMAT([start_time], 'yyyy-mm-dd hh:nn:ss') as job_start_time // run_id, start_time, workflow_id,analysis_status
        #sql = "SELECT  run_id,workflow_id , analysis_status,CDate(start_time) AS Job_start_time  FROM workflow_stats where workflow_id = ? and analysis_status= ?  order by start_time desc"
        #sql = 'SELECT  run_id,workflow_id ,FORMAT(start_time, "yyyy-mm-dd hh:nn:ss") AS job_start_time  FROM workflow_stats where workflow_id = ? '
        sql = 'SELECT  run_id,workflow_id,start_time as job_start_time  FROM workflow_stats'
        #sql = "SELECT  run_id,workflow_id , analysis_status, start_time  FROM workflow_stats where workflow_id = ? and analysis_status= ?  order by start_time desc"
        table_name ="workflow_stats"
        with pyodbc.connect(conn_str) as conn:
        # 2. Load the entire table (or a filtered query) into a DataFrame
        # It's better to filter in SQL if the table is huge, but Pandas is more robust for 'Extended' types
            query = f"SELECT run_id, start_time FROM {table_name} where workflow_id = ?"
            df = pd.read_sql(query, conn, params=[workflow_id])
            

        if df.empty:
            print("No record found with that ID.")
            return None            
        else:
            df['start_time_str'] = df['start_time'].apply(bytes_to_datetime)
            #pd.to_datetime(df['start_time'], errors='coerce', infer_datetime_format=True)
            print(df)
            latest = df.sort_values('start_time_str', ascending=False).head(1)
            print(latest)
            if not latest.empty:
 
                data_dict = latest.iloc[0].to_dict()
            
                # print(data_dict['run_id'])              
                # print(data_dict['start_time_str'])

            return  data_dict

    except Exception as e:
        print(f"get_run_id_stats_from_db - Error: {e}")
        raise 
    finally:
        conn.close()

def get_run_id_stats_from_db(db_path,run_id,workflow_id=None):
     
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Use '?' as a placeholder
        sql = "SELECT run_id, workflow_id,analysis_status FROM workflow_stats where run_id = ? "
        # Pass the variable as a tuple (search_id,)
        cursor.execute(sql, (run_id,) )
        
        row = cursor.fetchone()

        if row:
            print(f"Found runid: {row.run_id} with worflow_id: {row.workflow_id}")
            return {"run_id":row.run_id,"worflow_id":row.workflow_id,"analysis_status":row.analysis_status }
        else:
            print("No record found with that ID.")
            return None

    except Exception as e:
        print(f"get_run_id_stats_from_db - Error: {e}")
    finally:
        conn.close()

def add_run_id_workflow_status(db_path,data):
    # 1. Setup your connection
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'

    # 2. Your data variables
    
    start_date = data.get("start_time")
    if start_date:
        data["start_time"] = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
     

    # 1. Extract columns and create placeholders (?, ?, ?)
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    for key, value in data.items():
        if callable(value):
            print(f"❌ Found the culprit! Key '{key}' has a function value: {value}")
    try:
        # 3. Connect
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 4. Prepare the SQL Statement
        # Note: Use [brackets] if your column names have spaces
        #sql = "INSERT INTO workflow_stats (run_id, start_time,end_time,analysis_status) VALUES (?, ?,?,?)"
        sql = f"INSERT INTO workflow_stats ({columns}) VALUES ({placeholders})"
    
         # 3. Get the values in the same order as the columns
        values = tuple(data.values())

        # 5. Execute with data as a tuple
        cursor.execute(sql, values)

        # 6. IMPORTANT: Commit the transaction 
        conn.commit()        
        print("Record added successfully!")
    except Exception as e:

        print(f"add_run_id_workflow_status -   An error occurred: {e}")
        raise e

    finally:
        # 7. Close the connection
        if 'conn' in locals():
            conn.close()


def update_run_id_workflow_status(db_path,data):

    # 1. Setup your connection
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'

    # 2. Your data variables
    run_id = data.get("run_id") 
    start_time = data.get("start_time") 
    end_date = data.get("end_time")
    analysis_status =  data.get("analysis_status")
    if start_time:
        data["start_time"] = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")

    try:
        # 3. Connect
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        del data["run_id"]
        set_clause = ", ".join([f"[{k}] = ?" for k in data.keys() if k != "run_id"])
        #print(set_clause)
        # 4. Prepare the SQL Statement
        # Note: Use [brackets] if your column names have spaces
        #sql = "update workflow_stats set end_time=?, analysis_status = ? where run_id = ? "
        sql = f"update workflow_stats set {set_clause} where run_id = ? "

        params = list(data.values())
        params.append(run_id)

        # 5. Execute with data as a tuple
        cursor.execute(sql, params )

        # 3. Check if any row was actually updated
        if cursor.rowcount == 0:
            print("No record found with that ID. Nothing updated.")
        else:
            # 4. CRITICAL: Save the changes
            conn.commit()
            print(f"Successfully updated {cursor.rowcount} row(s).")
            # 6. IMPORTANT: Commit the transaction 
            conn.commit()         
    except Exception as e:
        print(f"update_run_id_workflow_status - An error occurred: {e}")

    finally:
        # 7. Close the connection
        if 'conn' in locals():
            conn.close()

def update_ms_access_table(db_path,table_name,update_id, data):
    # 1. Setup your connection
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'

    # 2. Your data variables
    id = data.get(update_id) 
    start_time = data.get("start_time") 
    end_date = data.get("end_time")
    analysis_status =  data.get("analysis_status")
    if start_time:
        data["start_time"] = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")

    try:
        # 3. Connect
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        del data[update_id ]
        set_clause = ", ".join([f"[{k}] = ?" for k in data.keys() if k != update_id])
        #print(set_clause)
        # 4. Prepare the SQL Statement
        # Note: Use [brackets] if your column names have spaces
        #sql = "update workflow_stats set end_time=?, analysis_status = ? where run_id = ? "
        sql = f"update {table_name} set {set_clause} where run_id = ? "

        params = list(data.values())
        params.append(id)

        # 5. Execute with data as a tuple
        cursor.execute(sql, params )

        # 3. Check if any row was actually updated
        if cursor.rowcount == 0:
            print("No record found with that ID. Nothing updated.")
        else:
            # 4. CRITICAL: Save the changes
            conn.commit()
            print(f"Successfully updated {cursor.rowcount} row(s).")
            # 6. IMPORTANT: Commit the transaction 
            conn.commit()         
    except Exception as e:
        print(f"update_run_id_workflow_status - An error occurred: {e}")

    finally:
        # 7. Close the connection
        if 'conn' in locals():
            conn.close()

def add_ms_access_table(db_path,table_name,data):
    # 1. Setup your connection
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'

    # 2. Your data variables
    
    # start_date = data.get("last_updated_time")
    # if start_date:
    #     data["start_time"] = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
     

    # 1. Extract columns and create placeholders (?, ?, ?)
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    for key, value in data.items():
        if callable(value):
            print(f"Found the culprit! Key '{key}' has a function value: {value}")
    try:
        # 3. Connect
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 4. Prepare the SQL Statement
        # Note: Use [brackets] if your column names have spaces
        #sql = "INSERT INTO workflow_stats (run_id, start_time,end_time,analysis_status) VALUES (?, ?,?,?)"
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
         # 3. Get the values in the same order as the columns
        values = tuple(data.values())

        # 5. Execute with data as a tuple
        cursor.execute(sql, values)

        # 6. IMPORTANT: Commit the transaction 
        conn.commit()        
        print("Record added successfully!")
    except Exception as e:

        print(f"add_run_id_workflow_status -   An error occurred: {e}")
        raise e

    finally:
        # 7. Close the connection
        if 'conn' in locals():
            conn.close() 

def get_job_id_by_name(db_path,object_name,object_type="JOBS"):
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        sql = "SELECT TOP 1 job_id  FROM jobs where object_name = ? and object_type= ?  "

        cursor.execute(sql,(object_name,object_type,))

        row = cursor.fetchone()

        # 2. CHECK: No Data Found Scenario
        if row is None:
            print(f"⚠️ No records found for object name : {object_name}")
            return None

        columns = [column[0] for column in cursor.description]
        data_dict = dict(zip(columns, row))

        return data_dict["job_id"]

    except Exception as e:
        print(f"get_job_id_by_name - Error: {e}")
        raise 
    finally:
        conn.close()


def get_list_of_file_transfer_jobs(jobp_name, wf_details_file):
    df = pd.read_csv(wf_details_file )
    
    filtered_df  = df[(df['workflow_name'] == jobp_name )& ( (df['is_ftp'] == True) |  (df['is_copy_move'] == True)) ]
    filtered_df = filtered_df[['workflow_name','job_name','job_type','is_ftp','is_copy_move']]

    return filtered_df

def add_jobs_to_ms_access_db(db_path, wf_details_file,workflow_name, workflow_id ):
    df = get_list_of_file_transfer_jobs(workflow_name,wf_details_file)
    
    # 1. Setup your connection
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'
    ls_keys = ['object_name','object_type','workflow_id']
    try:
        # 3. Connect
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        set_clause = ", ".join([f"[{k}] = ?" for k in ls_keys  ])
        #print(set_clause)
        # 4. Prepare the SQL Statement
        # Note: Use [brackets] if your column names have spaces
        #sql = "update workflow_stats set end_time=?, analysis_status = ? where run_id = ? "
        update_sql = f"update jobs set {set_clause} where object_name = ? and  object_type = ?  and workflow_id = ?"
        columns = "object_name , object_type, workflow_id "
        placeholders = "?,?,?"
        insert_sql = f"INSERT INTO jobs ({columns}) VALUES ({placeholders})"
 
        for _, row in df.iterrows():
            params =  [row["job_name"],
                row["job_type"],
                workflow_id,
                row["job_name"],
                row["job_type"],
                workflow_id]

            # 5. Execute with data as a tuple
            cursor.execute(update_sql, params )

            # 3. Check if any row was actually updated
            if cursor.rowcount == 0:
                params  = (row["job_name"], row["job_type"], workflow_id)
                print("No record found with that ID. Nothing updated.")
                cursor.execute(insert_sql, params ) 
                print(f"Successfully inserted {cursor.rowcount} row(s).")
                conn.commit()

            else:
                # 4. CRITICAL: Save the changes
                conn.commit()
                print(f"Successfully updated {cursor.rowcount} row(s).")
                # 6. IMPORTANT: Commit the transaction 
                conn.commit()         
    except Exception as e:
        print(f"update_run_id_workflow_status - An error occurred: {e}")

    finally:
        # 7. Close the connection
        if 'conn' in locals():
            conn.close()
    

def add_job_rules_to_ms_access_db(db_path, wf_details_file,workflow_name, workflow_id ):
    # df = get_list_of_file_transfer_jobs(workflow_name,wf_details_file)
    ls_job_data = get_filetransfer_cmd(workflow_name,wf_details_file)

    out = process_jobs(ls_job_data)
    print(ls_job_data)
    print(out)
    #return None 
    rows = get_data_for_job_rules(out,ls_job_data)
    print(len(rows))
    # 1. Setup your connection
    conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'
    ls_keys = ['rule_id','job_id','execution_order','rule_param','is_active']
    try:
        # 3. Connect
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        set_clause = ", ".join([f"[{k}] = ?" for k in ls_keys  ])
       
        # 4. Prepare the SQL Statement
        
        update_sql = f"update job_rules set {set_clause} where rule_id = ? and  job_id = ? and execution_order=?  "
        
        placeholders = "?,?,?,?,?"
        columns = columns = ", ".join(ls_keys)
        insert_sql = f"INSERT INTO job_rules ({columns}) VALUES ({placeholders})"
 
        for   row in rows:
            rule_id  = 1 
            job_name = row["job_name"]
            folders = row["folders"]
            job_id = get_job_id_by_name(db_path,job_name)
            execution_order = 1 
            rule_param = []
            is_active = True 
            params =  [ (rule_id,job_id,execution_order,json.dumps(rule_param),is_active)]
            ls_rule2_param = []
            ls_rule4_param = []
            if folders : 
                for folder in folders :

                    operation = folder["command"]
                    files_ext = folder["files_ext"]
                    folder_path = folder["folder"]
                    ls_rule2_param.append({"file_path":folder_path, "operation":operation.lower() ,"ref_rule_id": 1 })
                
                    ls_rule4_param.append({"file_path":folder_path, "operation":operation.lower() ,"ref_rule_id": 1,"file_type":files_ext})
                params.append((2,job_id,2,json.dumps(ls_rule2_param),is_active))
                params.append((4,job_id,3,json.dumps(ls_rule4_param),is_active))

            cursor.executemany(insert_sql, params ) 
            conn.commit()
            # 5. Execute with data as a tuple
            #cursor.execute(update_sql, params )

            # 3. Check if any row was actually updated
            # if cursor.rowcount == 0:
            #     params  = (row["job_name"], row["job_type"], workflow_id)
            #     print("No record found with that ID. Nothing updated.")
            #     cursor.execute(insert_sql, params ) 
            #     print(f"Successfully inserted {cursor.rowcount} row(s).")
            #     conn.commit()

            # else:
            #     # 4. CRITICAL: Save the changes
            #     conn.commit()
            #     print(f"Successfully updated {cursor.rowcount} row(s).")
            #     # 6. IMPORTANT: Commit the transaction 
            #     conn.commit()         
    except Exception as e:
        print(f"add_job_rules_to_ms_access_db - An error occurred: {e}")
        import traceback
        traceback.print_exception(e)

    finally:
        # 7. Close the connection
        if 'conn' in locals():
            conn.close()


def configur_jobs_n_rules(db_file, wf_details_file, workflow_name, workflow_id):
    add_jobs_to_ms_access_db(db_file, wf_details_file, workflow_name, workflow_id)
    add_job_rules_to_ms_access_db(db_file, wf_details_file, workflow_name, workflow_id)