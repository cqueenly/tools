import pandas as pd
import os
from database_tools import sql_tools as qlt
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta  # Import relativedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define the start and end of the month
today = datetime.now().date()
month_start = today.replace(day=1)
last_day_of_month = today + relativedelta(day=31)  # Calculate the last day of the current month

def create_monitoring_df(therapeutic_category=None):
   logger.info("Fetching campaign projection data...")

   target_query = """
       select distinct output.campaign,
           output.monitor_name,
           output.rate_classification AS Status,
           output.delivery_target AS MonthlyTarget,
           output.delivered_engagements AS MTDEons,
           output.projected_engagements AS TotalMonthProjection,
           ROUND(output.projected_over_under_budget * 100, 2) AS ProjectedTotal,
           output.datediffs,
           output.SubscriptionProgram,
           output.ConditionCategory AS TherapeuticCategory,
           output.TotalEngagements AS ContractedTotal,
           output.avg_target_per_month,
           output.notes
       from Messaging_DB.dash.DashMonitoringOutput output
           inner join Messaging_DB.dbo.vCampaignHit hit on hit.groupName = output.campaign
       WHERE (stream_level_projection = 'false' OR rate_level_projection = 'false')
       ORDER BY campaign
   """

   # Execute the query and get the data as a DataFrame
   target_df = qlt.execute_query_to_df(query=target_query, server_name='DSMAIN')

   # Filter by therapeutic category if provided
   if therapeutic_category:
       logger.info(f"Applying therapeutic category filter: {therapeutic_category}")
       if therapeutic_category.lower() == 'cm':
           # Filter to the therapeutic categories assigned to Chelsea
           chelsea_categories = [
               'Dermatology', 'Endocrinology', 'Hematology', 'Infectious Disease',
               'Ophthalmology', 'Other', 'Rare Disease', 'Rheumatology & Bone'
           ]
           target_df = target_df[target_df['TherapeuticCategory'].isin(chelsea_categories)]
       else:
           # Filter by the specified therapeutic category
           target_df = target_df[target_df['TherapeuticCategory'] == therapeutic_category]

   # Exclude campaigns with specific names
   excluded_campaigns = ['BayCare T65 2023', 'BCBS of Florida SEP 2023','BCBS of Florida T65 2023', 'Guided Solutions 5-Star 2023','e-TeleQuote 5 Star 2023', 'Guided Solutions Med Supp 2023','Guided Solutions Med Supp FL 2023', 'SingleCare 2023', 'MercyOne 2023']
   target_df = target_df[~target_df['campaign'].isin(excluded_campaigns)]

   return target_df

def retrieve_contracted_total(target_df):
   return target_df[['campaign', 'ContractedTotal']]

def retrieve_current_campaign_total():
   campaign_total_query = f"""
       select distinct groupName, count(distinct portalSessionKey) as CurrentCampaignTotal
       from Messaging_DB.dbo.vCampaignHit hit
       inner join Messaging_DB.dash.DashMonitoringOutput output on hit.groupName = output.campaign
       group by groupName
   """

   campaign_total_df = qlt.execute_query_to_df(query=campaign_total_query, server_name='DSMAIN')

   return campaign_total_df

def calculate_percent_contract_complete(row, current_campaign_total):
   campaign_name = row['campaign']
   matching_row = current_campaign_total[current_campaign_total['groupName'] == campaign_name]
   if not matching_row.empty:
       current_total = matching_row.iloc[0]['CurrentCampaignTotal']
       return (current_total / row['ContractedTotal']) * 100
   return 0  # Handle the case where there's no matching row

def retrieve_monthly_target(target_df):
   return target_df[['campaign', 'MonthlyTarget']]

def retrieve_current_monthly_total():
   monthly_total_query = f"""
       select distinct groupName, count(distinct portalSessionKey) as CurrentMonthlyTotal
       from Messaging_DB.dbo.vCampaignHit hit
       inner join Messaging_DB.dash.DashMonitoringOutput output on hit.groupName = output.campaign
       where datekey between '{month_start.strftime('%Y%m%d')}' and '{last_day_of_month.strftime('%Y%m%d')}'
       group by groupName
   """

   monthly_total_df = qlt.execute_query_to_df(query=monthly_total_query, server_name='DSMAIN')
   return monthly_total_df

def calculate_mtd_percentage_completed(row, current_monthly_total):
   campaign_name = row['campaign']
   matching_row = current_monthly_total[current_monthly_total['groupName'] == campaign_name]
   if not matching_row.empty:
       current_total = matching_row.iloc[0]['CurrentMonthlyTotal']
       return (current_total / row['MonthlyTarget']) * 100
   return 0  # Handle the case where there's no matching row

def create_campaign_totals_df(target_df):
   contracted_total = retrieve_contracted_total(target_df)
   current_campaign_total = retrieve_current_campaign_total()
   contracted_total['% Contract Complete'] = contracted_total.apply(lambda row: calculate_percent_contract_complete(row, current_campaign_total), axis=1)

   monthly_target = retrieve_monthly_target(target_df)
   current_monthly_total = retrieve_current_monthly_total()
   monthly_target['MTD % Complete'] = monthly_target.apply(lambda row: calculate_mtd_percentage_completed(row, current_monthly_total), axis=1)

   campaign_totals_df = pd.merge(contracted_total, monthly_target, on='campaign')
   campaign_totals_df = campaign_totals_df.drop_duplicates()
   return campaign_totals_df

def retrieve_campaign_manufacturer():
   campaign_manufacturer_query = """
       select distinct groupname, label_name
       from Messaging_DB.dash.DashActiveCampaignLabels
       where label_category = 'Manufacturer'
   """
   campaign_manufacturer_df = qlt.execute_query_to_df(query=campaign_manufacturer_query, server_name='DSMAIN')
   return campaign_manufacturer_df

if __name__ == "__main__":
   parser = argparse.ArgumentParser()
   parser.add_argument("-cm", "--chelsea_filter", action='store_true', help="Apply Chelsea's therapeutic category filter")
   parser.add_argument("-tc", "--therapeutic_category", type=str, default=None, help="Filter by therapeutic category")
   args = parser.parse_args()

   logger.info("Starting script...")

   # Determine the therapeutic category filter based on arguments
   if args.chelsea_filter:
       therapeutic_category = 'cm'
   else:
       therapeutic_category = args.therapeutic_category

   # Create the monitoring DataFrame with optional filters
   monitoring_df = create_monitoring_df(therapeutic_category=therapeutic_category)

   # Generate the filename
   abs_path = os.path.abspath(f"Campaign_Monitoring_{current_datetime}.xlsx")
   logger.info(f"Creating Excel file...")

   # Create a Pandas Excel writer using xlsxwriter as the engine.
   writer = pd.ExcelWriter(f"Campaign_Monitoring_{current_datetime}.xlsx", engine='xlsxwriter')

   # Write the original DataFrames to separate worksheets
   df_10_biz = monitoring_df[monitoring_df['monitor_name'] == '10 bizhr, 5w avg']
   df_7d = monitoring_df[monitoring_df['monitor_name'] == '7d, 5w avg']

   df_10_biz.to_excel(writer, sheet_name='10 Hour Projection', index=False)
   df_7d.to_excel(writer, sheet_name='7 Day Projection', index=False)

   # Create the campaign totals DataFrame
   campaign_totals_df = create_campaign_totals_df(monitoring_df)
   campaign_manufacturer_df = retrieve_campaign_manufacturer()

   # Save the result to an Excel file
   campaign_totals_df.to_excel(writer, sheet_name='Campaign Totals', index=False)

   writer.save()

   print(f"Excel file saved to: {abs_path}")
