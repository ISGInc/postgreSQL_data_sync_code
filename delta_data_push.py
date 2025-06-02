import pandas as pd
import time
import pyodbc
from sqlalchemy import create_engine, text
import psycopg2
from urllib.parse import quote_plus
from sklearn.feature_extraction.text import CountVectorizer

# First part - SQL Server connection and query and passing all the infor such as driver, server, database, etc.
start_time = time.time()
mssql_conn_str = (
    'mssql+pyodbc://isg_powerbi_login:Uk1NZ3JJpMERG1XW@uae-db.app-prod3.unanetae.biz:1433/isg_infocus?driver=ODBC+Driver+17+for+SQL+Server'
)
#intiating the engine with the connection string
db_engine = create_engine(mssql_conn_str, echo=True)
#creating the connection object using the engine created above.
connection = db_engine.connect()
#Creating the query which will be executed on the database and pull the data from unanet to local.
query = """SELECT
    p.projectId AS project_id,
    p.projectCode AS project_code,
    p.projectName AS project_name,
    p.projectLongName AS project_long_name,
    p.parentProjectId AS parent_project_id,
    a.ParentAddressID AS parent_address_id,
    p.LevelOneProjectId AS level_one_project_id,
    p.projectLevel AS project_level,
    p.projectPath AS project_path,
    p.factId AS fact_id,
    p.isBottom AS is_bottom,
    COALESCE(pf.egid,
        (SELECT TOP 1 pf_child.egid
         FROM projects p_child
         JOIN projectFacts pf_child ON p_child.factId = pf_child.factId
         WHERE p_child.parentProjectId = p.projectId
         AND p_child.factId IS NOT NULL
         AND pf_child.isactive = 1
         ORDER BY p_child.projectId), 1) AS billing_group_id,
    eg.egname AS billing_group_name,
    COALESCE(ec.ecid,
        (SELECT TOP 1 ei_child.ecid
         FROM projects p_child
         JOIN projectFacts pf_child ON p_child.factId = pf_child.factId
         JOIN expenseGroups eg_child ON pf_child.egid = eg_child.egid
         JOIN expenseGroupDates ed_child ON pf_child.egid = ed_child.egid
         JOIN expenseGroupItems ei_child ON ed_child.egdid = ei_child.egdid
         AND GETDATE() >= ed_child.startdate
         AND GETDATE() <= COALESCE(ed_child.enddate, '12/31/2099')
         JOIN expenseCodes ec_child ON ei_child.ecid = ec_child.ecid
         WHERE p_child.parentProjectId = p.projectId
         AND p_child.factId IS NOT NULL
         AND pf_child.isActive = 1
         AND ec_child.ecid IN (162,163,174)
         ORDER BY p_child.projectId), 162) AS expense_code_id,
    ec.ecname AS expense_code_name,
    f.ParentFirmID AS parent_firm_id,
    f.FirmID AS firm_id,
    f.FirmName AS firm_name,
	pudf.prj_Latitude AS latitude,
	pudf.prj_Longitude AS longitude,
	pf.orgid AS org_id,
    IsBillTermsNode AS is_bill_terms_node,
    ReportTypeID AS report_type_id,
	pf.pmempid AS pm_emp_id,
	pmemp.properName AS pm_name,
	pmudf.vecTitle AS pm_title,
	pmCnt.WorkEmail AS pm_email,
	pf.picempid AS pic_emp_id,
	picemp.properName AS pic_name,
	picudf.vecTitle AS pic_title,
	picCnt.WorkEmail AS pic_email,
	pf.EGID AS eg_id,
	PAEmpID as pa_emp_id,
	pf.IsActive AS is_active,
	pf.ProjectNote AS project_note,
	f.FirmCode as firm_code,
	f.MainAddressID as main_address_id,
	a.addressid as address_id,
	street1,
	street2,
	street3,
	street4,
	city,
	state,
	zip,
    pudf.prj_BusinessUnit AS business_unit,
	p.CreateDate AS CreateDate,
	p.ModifyDate AS last_updated
FROM projects p
LEFT JOIN projectFacts pf ON p.factId = pf.factId
LEFT JOIN expenseGroups eg ON pf.egid = eg.egid
LEFT JOIN expenseGroupDates ed ON pf.egid = ed.egid
LEFT JOIN expenseGroupItems ei ON ed.egdid = ei.egdid
    AND GETDATE() >= ed.startdate
    AND GETDATE() <= COALESCE(ed.enddate, '12/31/2099')
LEFT JOIN expenseCodes ec ON ei.ecid = ec.ecid
LEFT JOIN Firms f ON pf.FirmID = f.FirmID
LEFT JOIN Addresses a on f.MainAddressID = a.addressid
LEFT JOIN employees picEmp
    ON pf.picEmpId = picEmp.empId
LEFT JOIN employeeUDFs picudf
    ON pf.picempid = picudf.empId
LEFT JOIN contacts picCnt
	ON picEmp.EmpId = picCnt.empId
LEFT JOIN employees pmEmp
    ON pf.pmEmpId = pmEmp.empId
LEFT JOIN employeeUDFs pmudf
    ON pf.pmEmpId = pmudf.empId
LEFT JOIN contacts pmCnt
	ON pmEmp.EmpId = pmCnt.EmpId
LEFT JOIN projectUDFs pudf ON pf.factId = pudf.factId
WHERE p.projectLevel = 1
/*AND (p.CreateDate > (CURRENT_TIMESTAMP - 1) OR p.ModifyDate > (CURRENT_TIMESTAMP - 1))*/
AND p.ProjectCode NOT LIKE '__-____'
AND (
    p.factId IS NULL -- Include Rollup projects
    OR (
        pf.isActive = 1 -- Only applied to Standard projects
        AND ec.ecid IN (162,163,174) -- Only applied to Standard projects
    ))
ORDER BY p.ProjectID;"""

#reading the data into df dataframe using pandas library.
df = pd.read_sql(query, con=connection)
#closing the connection which is used for pulling the data from unanet.
connection.close()
pd.set_option('display.max_columns', None)  # Show all columns
#sorting the dataframes into two type where one dataframe goes for creating records and other goes for updating records.
#below block of code is for dataframe which picks records created in last day. and creates those records in PostgreSQL.
created_last_day_df = df[df['CreateDate'] > (pd.Timestamp.now() - pd.Timedelta(days=1))]
created_last_day_df = created_last_day_df.drop(columns=['CreateDate'])

#dropping the rows which have same created and modified date.

##below block of code is for dataframe which picks records modified in last day. and update those records in PostgreSQL.
modified_last_day = df[~df.index.isin(created_last_day_df.index)]
#dropping the rows which are inserted on the base of created date
modified_last_day_df = modified_last_day[modified_last_day['last_updated'] > (pd.Timestamp.now() - pd.Timedelta(days=1))]
modified_last_day_df = modified_last_day_df.drop(columns=['CreateDate'])

# PostgreSQL connection test
try:
#Variables with postgreSQL dev instances credentials.
    pg_user = 'isgdev'
    pg_password = quote_plus('Dev@ISG1973!')  # URL encode the password
    pg_host = '52.5.191.29'
    pg_port = '5432'
    pg_database = 'isgapi'

# Create the connection string with the variables above
    pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}'
    pg_engine = create_engine(pg_conn_str)

    # Test connection
    with pg_engine.connect() as connection:
        try:
            #df.to_sql('project_test_v2', connection, if_exists='replace', index=False)
            created_last_day_df.to_sql('project_test_v2', connection, if_exists='append', index=False)
            print(f"Data successfully processed in PostgreSQL: {len(created_last_day_df)}")
            print(created_last_day_df['project_code'])

        except Exception as e:
            print(f"Error inserting data in PostgreSQL: {str(e)} ")

        try:
            # Handle updates for modified records
            update_count = 0
            print("\nUpdating records in PostgreSQL:")
            for index, row in modified_last_day_df.iterrows():
                print(f"Updating Project ID: {row['project_id']} - {row['project_name']}")
                update_query = text("""
                                    UPDATE project_test_v2
                                    SET project_code         = :project_code,
                                        project_name         = :project_name,
                                        project_long_name    = :project_long_name,
                                        parent_project_id    = :parent_project_id,
                                        parent_address_id    = :parent_address_id,
                                        level_one_project_id = :level_one_project_id,
                                        project_level        = :project_level,
                                        project_path         = :project_path,
                                        fact_id              = :fact_id,
                                        is_bottom            = :is_bottom,
                                        billing_group_id     = :billing_group_id,
                                        billing_group_name   = :billing_group_name,
                                        expense_code_id      = :expense_code_id,
                                        expense_code_name    = :expense_code_name,
                                        parent_firm_id       = :parent_firm_id,
                                        firm_id              = :firm_id,
                                        firm_name            = :firm_name,
                                        latitude             = :latitude,
                                        longitude            = :longitude,
                                        org_id               = :org_id,
                                        is_bill_terms_node   = :is_bill_terms_node,
                                        report_type_id       = :report_type_id,
                                        pm_emp_id            = :pm_emp_id,
                                        pm_name              = :pm_name,
                                        pm_title             = :pm_title,
                                        pm_email             = :pm_email,
                                        pic_emp_id           = :pic_emp_id,
                                        pic_name             = :pic_name,
                                        pic_title            = :pic_title,
                                        pic_email            = :pic_email,
                                        eg_id                = :eg_id,
                                        pa_emp_id            = :pa_emp_id,
                                        is_active            = :is_active,
                                        project_note         = :project_note,
                                        firm_code            = :firm_code,
                                        main_address_id      = :main_address_id,
                                        address_id           = :address_id,
                                        street1              = :street1,
                                        street2              = :street2,
                                        street3              = :street3,
                                        street4              = :street4,
                                        city                 = :city,
                                        state                = :state,
                                        zip                  = :zip,
                                        business_unit        = :business_unit
                                    WHERE project_id = :project_id""")

                connection.execute(update_query, row.to_dict())
                connection.commit()
                update_count += 1
            print(f"\nTotal records updated: {update_count}")
            print(modified_last_day_df[['project_code', 'project_name']])

        except Exception as e:
            print(f"Error updating records in PostgreSQL: {str(e)}")

finally:
    # Ensure PostgreSQL connection is properly closed
    if 'pg_engine' in locals():
        pg_engine.dispose()

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Query executed in {elapsed_time:.2f} seconds.")
