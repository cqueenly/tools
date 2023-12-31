import pandas as pd
import os
from database_tools import sql_tools as qlt
import logging
from datetime import datetime
import argparse
import re
import constants as c

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Set date-times
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
current_date = datetime.now().date()
first_dom = current_date.replace(day=1)
last_dom = current_date.replace(day=31)

######################## CAMPAIGN PROJECTIONS ###########################
########################################################################

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
                        output.ConditionCategory AS TherapeuticCategory,
                        output.TotalEngagements AS ContractedTotal,
                        output.avg_target_per_month,
                        output.notes,
                        CASE
                            WHEN CHARINDEX('Survey Control', output.campaign) > 0 THEN 'False'
                            WHEN (output.SubscriptionProgram IS NULL OR LEN(output.SubscriptionProgram)= 0) THEN 'False'
                            ELSE output.SubscriptionProgram
                        END AS SubscriptionProgram
        from Messaging_DB.dash.DashMonitoringOutput output
                inner join Messaging_DB.dbo.vCampaignHit hit on hit.groupName = output.campaign
        WHERE (stream_level_projection = 'false' OR rate_level_projection = 'false')
        ORDER BY campaign
   """

    # Execute the query and get the data as a DataFrame
    target_df = qlt.execute_query_to_df(
        query=target_query, server_name='DSMAIN')

    # replace monthly target with custom target if provided
    logger.info("Incorperating custom targets...")
    target_df['MonthlyTarget'] = target_df['campaign'].map(
        c.custom_targets).fillna(target_df['MonthlyTarget'])

    # recalculate projected total based on custom_target if provided
    target_df['ProjectedTotal'] = target_df.apply(
        lambda row: round((row['TotalMonthProjection'] / c.custom_targets.get(
            row['campaign'], row['MonthlyTarget'])) * 100, 2)
        if row['campaign'] in c.custom_targets else row['ProjectedTotal'],
        axis=1
    )

    # Filter by therapeutic category if provided
    if therapeutic_category:
        logger.info(
            f"Applying therapeutic category filter: {therapeutic_category}")
        if therapeutic_category.lower() == 'cm':
            # Filter to the therapeutic categories assigned to Chelsea
            target_df = target_df[target_df['TherapeuticCategory'].isin(
                c.chelsea_categories)]
        else:
            # Filter by the specified therapeutic category
            target_df = target_df[target_df['TherapeuticCategory']
                                  == therapeutic_category]

    # Exclude campaigns with specific names
    target_df = target_df[~target_df['campaign'].isin(c.excluded_campaigns)]

    return target_df

# ** stream level targets, stream level reports

######################## CAMPAIGN TOTALS ###########################
####################################################################


def retrieve_contracted_total(target_df):
    return target_df[['campaign', 'ContractedTotal']]


def retrieve_current_campaign_total():
    campaign_total_query = """
       select distinct groupName, count(distinct portalSessionKey) as CurrentCampaignTotal
       from Messaging_DB.dbo.vCampaignHit hit
       inner join Messaging_DB.dash.DashMonitoringOutput output on hit.groupName = output.campaign
       group by groupName
   """

    campaign_total_df = qlt.execute_query_to_df(
        query=campaign_total_query, server_name='DSMAIN')

    return campaign_total_df


def calculate_percent_contract_complete(row, current_campaign_total):
    campaign_name = row['campaign']
    matching_row = current_campaign_total[current_campaign_total['groupName'] == campaign_name]
    if not matching_row.empty:
        current_total = matching_row.iloc[0]['CurrentCampaignTotal']
        return round(((current_total / row['ContractedTotal']) * 100), 2)
    return 0  # Handle the case where there's no matching row


def retrieve_monthly_target(target_df):
    return target_df[['campaign', 'MonthlyTarget']]


def retrieve_current_monthly_total():
    monthly_total_query = f"""
       select distinct groupName, count(distinct portalSessionKey) as CurrentMonthlyTotal
       from Messaging_DB.dbo.vCampaignHit hit
       inner join Messaging_DB.dash.DashMonitoringOutput output on hit.groupName = output.campaign
       where datekey between '{first_dom.strftime('%Y%m%d')}' and '{last_dom.strftime('%Y%m%d')}'
       group by groupName
   """
    monthly_total_df = qlt.execute_query_to_df(
        query=monthly_total_query, server_name='DSMAIN')
    return monthly_total_df


def calculate_mtd_percentage_completed(row, current_monthly_total):
    campaign_name = row['campaign']
    matching_row = current_monthly_total[current_monthly_total['groupName']
                                         == campaign_name]
    if not matching_row.empty:
        current_total = matching_row.iloc[0]['CurrentMonthlyTotal']
        if campaign_name in c.custom_targets:
            return round((current_total / c.custom_targets[campaign_name]) * 100, 2)
        else:
            return round((current_total / row['MonthlyTarget']) * 100, 2)
    return 0  # Handle the case where there's no matching row


def create_campaign_totals_df(target_df):
    contracted_total = retrieve_contracted_total(target_df)
    current_campaign_total = retrieve_current_campaign_total()
    contracted_total['% Contract Complete'] = contracted_total.apply(
        lambda row: calculate_percent_contract_complete(row, current_campaign_total), axis=1)

    monthly_target = retrieve_monthly_target(target_df)
    current_monthly_total = retrieve_current_monthly_total()
    monthly_target['MTD % Complete'] = monthly_target.apply(
        lambda row: calculate_mtd_percentage_completed(row, current_monthly_total), axis=1)
    campaign_totals_df = pd.merge(
        contracted_total, monthly_target, on='campaign')

    # Merge and reorder
    campaign_totals_df = campaign_totals_df.merge(current_monthly_total[[
                                                  'groupName', 'CurrentMonthlyTotal']], left_on='campaign', right_on='groupName', how='left')
    campaign_totals_df = campaign_totals_df[[
        'campaign', 'ContractedTotal', '% Contract Complete', 'CurrentMonthlyTotal', 'MonthlyTarget', 'MTD % Complete']]
    campaign_totals_df = campaign_totals_df.drop_duplicates()

    campaign_totals_df = campaign_totals_df.sort_values(
        by=['% Contract Complete'], ascending=False)
    return campaign_totals_df


######################## DATEDIFF PROPOSALS ########################
####################################################################

# ** extract last update and update type (unassignment, pacing change, etc)

def extract_datediff(datediffs):
    pattern = r'DateDiffDays\(PatientAnswer\("Required.DateOfBirth"\), GetDate\(\)\)%\d+\s*(?:[<>]=?|<=|>=)\s*(\d+)'
    match = re.findall(pattern, datediffs)
    if len(match) > 1:
        if all(matches == match[0] for matches in match):
            return match[0]
        else:
            return "multiple datediffs"
    return int(match[0]) if match and match[0].isdigit() else 0


def calculate_proposed_datediff(row):
    datediff = row['datediff']
    current_projected = row['ProjectedTotal']
    desired_projection = 103

    try:
        # avoid typeerror by converting projected total to numeric and ensuring its valid
        current_projected = pd.to_numeric(current_projected, errors='coerce')
        datediff = pd.to_numeric(datediff, errors='coerce')

        # check if the dd is 99/999
        if pd.notnull(datediff) and datediff in (99, 999):
            return None

        if pd.notnull(current_projected):
            proposed_datediff = 100 - \
                ((desired_projection * (100 - datediff)) / current_projected)
            return round(proposed_datediff, 1)
        else:
            return None
    except ZeroDivisionError:
        return None


def apply_datediff_restrictions(row):
    proposed_datediff = row['Proposed Datediff']
    current = row['MTD % Complete']
    datediff = row['datediff']
    datediff = pd.to_numeric(datediff, errors='coerce')
    if pd.notnull(datediff):
        if proposed_datediff > 100:
            return 99
        if proposed_datediff < 0:
            return 0
        if proposed_datediff == 100:
            return None
        if proposed_datediff < 1 and datediff == 0:
            return None
    if current >=100 and isinstance(datediff,int) and "Survey Control" not in row['campaign']:
        if datediff in [99, 999]:
            return None
        else:
            return 999
    if current >=100 and "Survey Control" in row['campaign']:
        return None
        # this doesn't work >:(  all
    if (pd.isnull(datediff) or pd.isna(datediff) or datediff == 0) and proposed_datediff <= 1:
        return None
    else:
        return proposed_datediff


def apply_subscription_rules(row):
    subscription_program = row['SubscriptionProgram']
    proposed_datediff = row['Proposed Datediff']
    datediff = row['datediff']
    campaign = row['campaign']

    if (subscription_program == 1 or campaign in c.maxed_out) and pd.to_numeric(datediff, errors='coerce') == 0:
        return None
    elif (subscription_program == 1 or campaign in c.maxed_out):
        return 0
    else:
        return proposed_datediff


def retrieve_campaign_manufacturer():  # reference to quickly apply datediff changes
    campaign_manufacturer_query = """
        select distinct groupname as campaign, label_name AS Manufacturer
        from Messaging_DB.dash.DashActiveCampaignLabels
        where label_category = 'Manufacturer'
    """

    campaign_manufacturer_df = qlt.execute_query_to_df(
        query=campaign_manufacturer_query, server_name='DSMAIN')
    return campaign_manufacturer_df


# ** extract last update and update type (unassignment, pacing change, etc)
# ** stream level targets, stream level reports
# ** resolve returning-a-view-versus-a-copy

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-cm", "--chelsea_filter", action='store_true',
                        help="Apply Chelsea's therapeutic category filter")
    parser.add_argument("-tc", "--therapeutic_category", type=str,
                        default=None, help="Filter by therapeutic category")
    args = parser.parse_args()

    logger.info("Starting script...")

    # Determine the therapeutic category filter based on arguments
    if args.chelsea_filter:
        therapeutic_category = 'cm'
    else:
        therapeutic_category = args.therapeutic_category

    # Create the monitoring DataFrame with optional filters
    monitoring_df = create_monitoring_df(
        therapeutic_category=therapeutic_category)

    # Retrieve campaign information
    campaign_manufacturer_df = retrieve_campaign_manufacturer()
    campaign_totals_df = create_campaign_totals_df(monitoring_df)

    # Generate the filename
    abs_path = os.path.abspath(f"Campaign_Monitoring_{current_datetime}.xlsx")

    # Create a Pandas Excel writer using xlsxwriter as the engine.
    logger.info(f"Creating excel file...")
    writer = pd.ExcelWriter(
        f"Campaign_Monitoring_{current_datetime}.xlsx", engine='xlsxwriter')

    # Create the "Quick Guide" sheet
    logger.info(f"Creating quick guide...")
    quick_guide_df = monitoring_df[monitoring_df['monitor_name'] == '10 bizhr, 5w avg'][[
        'campaign', 'TherapeuticCategory', 'datediffs', 'Status', 'SubscriptionProgram', 'ProjectedTotal', 'notes']]
    
    # add campaign totals to quick guide
    quick_guide_df = quick_guide_df.merge(
        campaign_totals_df[['campaign', '% Contract Complete', 'MTD % Complete']], on='campaign', how='left')

    # add transformed date diff to quick guide
    quick_guide_df['datediff'] = quick_guide_df['datediffs'].apply(
        extract_datediff)
    quick_guide_df['datediff'].apply(
        pd.to_numeric, errors='coerce', downcast='integer')

    # Calculate propsed datediff and add it to the df
    quick_guide_df['Proposed Datediff'] = quick_guide_df.apply(
        lambda row: calculate_proposed_datediff(row), axis=1)
    # lambda row: calculate_proposed_datediff(datediff=row['datediff'], current_projected=row['ProjectedTotal']), is_subscription=is_subscription, axis=1)

    quick_guide_df['Proposed Datediff'] = quick_guide_df.apply(
        lambda row: apply_datediff_restrictions(row), axis=1)

    # Apply subscription rules to proposed datediff
    quick_guide_df['Proposed Datediff'] = quick_guide_df.apply(
        lambda row: apply_subscription_rules(row), axis=1)

    # add Manufacturer to quick guide
    quick_guide_df = quick_guide_df.merge(
        campaign_manufacturer_df[['campaign', 'Manufacturer']], on='campaign', how='left')

    # add personal notes to quick guide
    quick_guide_df['My Notes'] = quick_guide_df['campaign'].map(c.my_notes)

    # make pretty
    quick_guide_df = quick_guide_df.rename(columns={
        'notes': 'AI Notes',
        'SubscriptionProgram': 'Subscription',
        'TherapeuticCategory': 'Therapeutic Category',
        'MTD % Complete': 'Current %',
        'ProjectedTotal': '10 hr Projected %'
    })
    quick_guide_columns = ['campaign', 'Manufacturer', 'Therapeutic Category', 'Subscription', '% Contract Complete',
                           'datediff', 'Status', 'Current %', '10 hr Projected %', 'Proposed Datediff', 'My Notes', 'AI Notes']
    quick_guide_df = quick_guide_df[quick_guide_columns]
    quick_guide_df = quick_guide_df.sort_values(
        by=['Therapeutic Category', 'campaign'])

    # Save the quick guide df to excel
    quick_guide_df.to_excel(writer, sheet_name='Quick Guide', index=False)

    # Write the original DataFrames to separate worksheets
    logger.info(f"Filtering dash projections...")
    df_10_biz = monitoring_df[monitoring_df['monitor_name']
                              == '10 bizhr, 5w avg']
    df_7d = monitoring_df[monitoring_df['monitor_name'] == '7d, 5w avg']

    df_10_biz.to_excel(writer, sheet_name='10 Hour Projection', index=False)
    df_7d.to_excel(writer, sheet_name='7 Day Projection', index=False)

    # Save the result to an Excel file
    logger.info(f"Calculating campaign totals...")
    campaign_totals_df.to_excel(
        writer, sheet_name='Campaign Totals', index=False)

    writer.save()

    logger.info(f"Excel file saved to: {abs_path}")


# campaigns =['Sotyktu 2023', 'Reblozyl 2023']
# data_types = {}
# for campaign in campaigns:
#    campaign_subset = quick_guide_df[quick_guide_df['campaign'] == campaign]
#    data_types[campaign] = campaign_subset['datediff'].dtype
# print(data_types)

# unique_values = quick_guide_df[quick_guide_df['campaign'] == 'Sotyktu 2023']['datediff'].unique()

# print(unique_values)
