import pdfkit
from tabulate import tabulate
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import os

from datetime import datetime, date, timedelta
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable

now = datetime.now()
hook = MySqlHook(mysql_conn_id='mysql_adgg_db_production')

# Get the current file's path
current_file_path = os.path.abspath(__file__)
dag_folder = os.path.dirname(os.path.dirname(current_file_path))
scripts_dir = dag_folder + '/dags/utilities/scripts/reports'
output_dir = dag_folder + '/dags/utilities/output/'
css_file = dag_folder + '/dags/utilities/style/style.css'
banner_img = dag_folder + '/dags/utilities/img/banner.png'
distribution_list = Variable.get("weekly_distribution_list")

#pdf report background
sns.set_theme(style="white")

#defined default arguments for the dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_last_sunday():
    today = datetime.today()
    days_until_sunday = (today.weekday() - 6) % 7
    last_sunday = today - timedelta(days=days_until_sunday)
    # Check if last_sunday is equal to today (which means today is a Friday)
    # If today is a Sunday, then we want to get the Friday of the previous week
    if last_sunday == today:
        last_sunday -= timedelta(weeks=1)
    return last_sunday.date()

def get_last_monday():
    today = datetime.today()
    days_until_monday = (today.weekday() - 0) % 7
    last_monday = today - timedelta(days=days_until_monday)
    # Check if last_monday is equal to today (which means today is a Friday)
    # If today is a Monday, then we want to get the Friday of the previous week
    if last_monday == today:
        last_monday -= timedelta(weeks=1)
    return last_monday.date()

start_date = get_last_monday()
end_date = get_last_sunday()

dag_params = {
    'start_date': start_date,
    'end_date': end_date,
    "distribution-list": distribution_list
}

# Configure PDF options
pdf_options = {
    'page-size': 'A4',
    'enable-local-file-access': None
}

@dag(
    dag_id='Weekly-Data-Report',
    default_args=default_args,
    schedule_interval="30 4 * * 1",  # Every Monday at 7:30 AM
    template_searchpath=[scripts_dir],
    catchup=False,
    max_active_runs=1,  # Set the maximum number of active runs to 1
    params=dag_params
)
def weekly_data_report():
    @task(task_id="Start", provide_context=True)
    def start(**context):
        _start_date = context["params"]["start_date"]
        _recipients_email = context["params"]["distribution-list"]
        return _start_date, _recipients_email

    start = start()

    # Stage Data > Initial Extractions > Data Stored In a Temporary Table
    extract_data_flow_report_data = MySqlOperator(
        task_id='Stage-Report-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='extract_data_flow_records.sql'
    )

    extract_animals_data_by_type = MySqlOperator(
        task_id='Animal-Type-Summary',
        mysql_conn_id='mysql_adgg_db_production',
        sql='extract_data_flow_animal_reg_records.sql'
    )

    extract_farms_data_by_type = MySqlOperator(
        task_id='Farm-Type-Summary',
        mysql_conn_id='mysql_adgg_db_production',
        sql='extract_data_flow_farm_reg_records.sql'
    )

    extract_milk_data_by_type = MySqlOperator(
        task_id='Milk-Summary',
        mysql_conn_id='mysql_adgg_db_production',
        sql='extract_data_flow_milk_records.sql'
    )

    extract_weight_data_by_type = MySqlOperator(
        task_id='Weight-Summary',
        mysql_conn_id='mysql_adgg_db_production',
        sql='extract_data_flow_weight_records.sql'
    )

    @task(task_id="Generate-Report", provide_context=True)
    def data_flow_report(**kwargs):

        start_date, recipients_email = kwargs['ti'].xcom_pull()

        week_number = 0
        year_value = 0
        if isinstance(start_date, str):
            date_object = datetime.strptime(start_date, '%Y-%m-%d')
            week_number = date_object.strftime("%U")
            year_value = date_object.strftime("%Y")
        else:
            week_number = start_date.strftime("%U")
            year_value = start_date.strftime("%Y")

        # Define filename for the report
        report_pdf = output_dir + 'Weekly-Report-' + datetime.today().strftime('%Y%m%d') + '.pdf'

        # Define data summary file name
        fig_data_summary = output_dir + 'fig_data_summary_plots_' + datetime.today().strftime('%Y%m%d') + '.png'

        # Get Stats For each Enumerator
        sql_data_flow = f"SELECT country ,enumerator Enumerator,event,total FROM reports.rpt_data_flow"
        df_data_flow = hook.get_pandas_df(sql_data_flow)

        df_data_flow['Enumerator'] = df_data_flow['Enumerator'].apply(lambda x: x.capitalize())

        # Animal Reg Stats By Animal Type
        sql_animal_stats = f"SELECT Country,Animal_type,Total FROM reports.rpt_data_flow_animal_reg"
        df_animal_stats = hook.get_pandas_df(sql_animal_stats)

        # Farm Reg By Farm Type
        sql_farm_stats = f"SELECT Country, Farm_type, Total FROM reports.rpt_data_flow_farm_reg"
        df_farm_stats = hook.get_pandas_df(sql_farm_stats)

        # Milk Data
        sql_milk_data = f"SELECT Country, Total FROM reports.rpt_data_flow_milk"
        df_milk_data = hook.get_pandas_df(sql_milk_data)

        # Weight Data
        sql_weight_data = f"SELECT Country, Total FROM reports.rpt_data_flow_weight"
        df_weight_data = hook.get_pandas_df(sql_weight_data)


        # Data Quality Report
        sql_qa_data = f"SELECT c.name AS country,b.quality_check as 'Quality Check', a.total_records as 'Records', a.total_error_records as 'Error Records', a.error_rate as 'Error Rate(%)' FROM  interface_data_quality_report_data_weekly a INNER JOIN interface_data_quality_check b ON a.quality_check = b.id INNER JOIN core_country c ON a.country_id =c.id WHERE a.report_year={year_value} and a.report_week={week_number} and deleted = 0"
        df_qa_data = hook.get_pandas_df(sql_qa_data)

        countries = df_data_flow['country'].unique()

        # Generate Data Recording Plots
        def data_recording_plots():
            # Get unique event names
            unique_events = df_data_flow['event'].unique()
            # Calculate the number of rows required for subplots
            num_rows = (len(unique_events) + 2) // 3  # Ceiling division

            # Create subplots using matplotlib
            fig, axes = plt.subplots(nrows=num_rows, ncols=3, figsize=(12, 4 * num_rows))

            # Flatten the 2D array of axes into a 1D array for easy iteration
            axes = axes.flatten()

            # Plot the bar charts using sns.barplot on each subplot
            for i, event in enumerate(unique_events):
                sns.barplot(x='country', y='total', data=df_data_flow[df_data_flow['event'] == event], estimator=sum,
                            errorbar=None, ax=axes[i])
                axes[i].set_title(f'{event}')
                axes[i].set_xlabel('Country')
                axes[i].set_ylabel('Total Records')

            # Remove any empty subplots
            for j in range(i + 1, num_rows * 3):
                fig.delaxes(axes[j])

            plt.tight_layout()

            # Save the figure to a file
            plt.savefig(fig_data_summary)
            report = f"<div class ='page-break-before'><h3> Summary Of Data Recording </h3></div><br/><div><img src='{fig_data_summary}'/></div>"
            return report

        # Farm & Animal-Registration
        def farm_animal_reg_data_tables():
            df_data_flow_filtered_reg = df_data_flow[
                df_data_flow['event'].isin(['Animal-Reg', 'Farmer-Reg'])]
            df_data_flow_reg_grouped = df_data_flow_filtered_reg.groupby(['country', 'event']).sum()
            pvt_data_flow_reg_pvt = df_data_flow_reg_grouped.pivot_table(index='country', columns='event',
                                                                         values='total', fill_value=0)
            pvt_data_flow_reg_pvt.reset_index(inplace=True)
            summary = tabulate(pvt_data_flow_reg_pvt, headers=pvt_data_flow_reg_pvt.columns, tablefmt='html',
                                          showindex=False)
            report = "<br/><div><h3>Registration Summary</h3></div><div style= 'width: 40%;'>" + summary + "</div>"
            return report

        # Events Monitoring Summary
        def event_monitoring_summary():
            df_data_flow_filtered_mon = df_data_flow[
                ~df_data_flow['event'].isin(['Animal-Reg', 'Farmer-Reg'])]
            df_data_flow_mon_grouped = df_data_flow_filtered_mon.groupby(['country', 'event']).sum()
            pvt_data_flow_mon_pvt = df_data_flow_mon_grouped.pivot_table(index='country', columns='event',
                                                                         values='total', fill_value=0)
            pvt_data_flow_mon_pvt.reset_index(inplace=True)
            summary = tabulate(pvt_data_flow_mon_pvt, headers=pvt_data_flow_mon_pvt.columns, tablefmt='html',
                                          showindex=False)
            report = "<br/><div><h3>Monitoring Summary</h3></div>" + "<div>" + summary + "</div>"
            return report

        # Enumerator Activity Report
        def enumerator_activity_summary():           
            combined_summary = ""            
    
            # Step 1: Melt the DataFrame to transform 'event' into rows
            melted_data_flow = pd.melt(df_data_flow, id_vars=['country', 'Enumerator', 'event'], value_vars=['total'])
    
            # Step 2: Create the pivot table with 'event' as the column header
            pvt_data_flow = melted_data_flow.pivot_table(index=['country', 'Enumerator'], columns='event', values='value',
                                                         fill_value=0)
    
            pvt_data_flow.reset_index(inplace=True)
    
            for country in countries:
                filtered_df = pvt_data_flow[(pvt_data_flow['country'] == country)]
                filtered_df = filtered_df.drop('country', axis=1)  # remove country Column
    
                # Convert DataFrame to HTML table
                country_summary = tabulate(filtered_df, headers=filtered_df.columns, tablefmt='html',
                                                          showindex=False)
                sub_header = "<div><h3>" + country + "</h3></div>"
                combined_summary = combined_summary + sub_header + country_summary
            summary = "<div class ='float-clear'><br/><div><h3> Enumerator Performance Report</h3></div>" + combined_summary + "</div>"
            return summary

        # Top Ranking Enumerators
        # Define a function to get the top 5 records per group
        def get_top_5(group):
            return group.head(5)

        def top_5_ranking_summary():
            top_5_summary_all = ""
            bottom_5_summary_all = ""

            for country in countries:
                df_data_flow_filtered_country = df_data_flow[(df_data_flow['country'] == country)]
                df_data_flow_enum_aggregation = df_data_flow_filtered_country.groupby(['Enumerator']).sum()
                columns_to_drop = ['country', 'event']
                df_data_flow_enum_aggregation = df_data_flow_enum_aggregation.drop(columns=columns_to_drop)

                # top
                df_top_5_ranked_Enumerator = df_data_flow_enum_aggregation.sort_values('total', ascending=False)
                df_top_5_ranked_Enumerator = df_top_5_ranked_Enumerator.apply(get_top_5)
                df_top_5_ranked_Enumerator.reset_index(inplace=True)
                df_top_5_ranked_Enumerator['rank'] = df_top_5_ranked_Enumerator['total'].rank(ascending=False,
                                                                                              method='dense')
                df_top_5_ranked_Enumerator.columns = ['Enumerator', 'Records', 'Rank']
                top_5_summary = tabulate(df_top_5_ranked_Enumerator,
                                                            headers=df_top_5_ranked_Enumerator.columns, tablefmt='html',
                                                            showindex=False)
                top_5_summary_all = top_5_summary_all + "<div class ='float-child'><h3>" + country + "</h3>"+ top_5_summary + "</div>"

                # Bottom
                df_bottom_5_ranked_Enumerator = df_data_flow_enum_aggregation.sort_values('total', ascending=True)
                df_bottom_5_ranked_Enumerator = df_bottom_5_ranked_Enumerator.apply(get_top_5)
                df_bottom_5_ranked_Enumerator.reset_index(inplace=True)
                df_bottom_5_ranked_Enumerator['rank'] = df_bottom_5_ranked_Enumerator['total'].rank(ascending=False,
                                                                                                    method='dense')
                df_bottom_5_ranked_Enumerator.columns = ['Enumerator', 'Records', 'Rank']
                bottom_5_summary = tabulate(df_bottom_5_ranked_Enumerator,
                                                               headers=df_bottom_5_ranked_Enumerator.columns,
                                                               tablefmt='html',
                                                               showindex=False)
                bottom_5_summary_all = bottom_5_summary_all + "<div class ='float-child'><h3>" + country + "</h3>" + bottom_5_summary + "</div>"

            top_5_header = "<div class ='float-clear'> <h3>Top Performing Enumerators</h3></div>"
            bottom_5_header = "<div class ='float-clear'><h3>Least Performing Enumerators</h3></div>"
            report_top_5 = top_5_header + top_5_summary_all
            report_bottom_5 = bottom_5_header + bottom_5_summary_all

            report = report_top_5 + "<div class ='float-clear'><br/><br/></div>" + report_bottom_5
            return report

        #QA
        def qa_summary():
            header = "<br/><h3> Data Quality Report</h3>"
            summary_all = ""
            arr_qa_countries = df_qa_data['country'].unique()
            for country in arr_qa_countries:
                filtered_df_qa = df_qa_data[(df_qa_data['country'] == country)]
                filtered_df_qa = filtered_df_qa.drop('country', axis=1)  # remove country Column

                # Convert DataFrame to HTML table
                summary = tabulate(filtered_df_qa, headers=filtered_df_qa.columns, tablefmt='html',
                                                          showindex=False)
                sub_header = "<div><h3>" + country + "</h3></div>"
                summary_all = summary_all + sub_header + summary

            report = "<div class='float-clear'><div class ='float-child-70'>" + header + summary_all + "</div></div>"
            return report

        # animal stats
        def farm_animal_by_type_summary():
            pvt_animal_stats = df_animal_stats.pivot_table(index='Country', columns='Animal_type', values='Total',
                                                           fill_value=0)
            pvt_animal_stats.reset_index(inplace=True)
            animal_type_summary = tabulate(pvt_animal_stats, headers=pvt_animal_stats.columns, tablefmt='html',
                                         showindex=False)

            # Farm Stats
            pvt_farm_stats = df_farm_stats.pivot_table(index='Country', columns='Farm_type', values='Total', fill_value=0)
            pvt_farm_stats.reset_index(inplace=True)
            farm_type_summary = tabulate(pvt_farm_stats, headers=pvt_farm_stats.columns, tablefmt='html',
                                       showindex=False)

            report = "<br/><div class ='float-child-40'><h3>Categorized Animal Registration</h3>" + animal_type_summary + "</div><div class ='float-clear'><div class ='float-child'><h3><br/>Categorized Farmer Registration</h3>" + farm_type_summary + "</div><div class ='float-clear'><br/></div>"
            return report

        # Milk stats chart
        # Group by 'Country' column

        def milk_stats():
            df_milk_data_grouped = df_milk_data.groupby('Country')
            # Calculate count, maximum, minimum, standard deviation, median, average, and sum for each group
            milk_count = df_milk_data_grouped['Total'].count()
            milk_maximum = df_milk_data_grouped['Total'].max()
            milk_minimum = df_milk_data_grouped['Total'].min()
            milk_std_deviation = df_milk_data_grouped['Total'].std()
            milk_median = df_milk_data_grouped['Total'].median()
            milk_average = df_milk_data_grouped['Total'].mean()


            # Create a new DataFrame with the results
            milk_summary = pd.DataFrame({
                'Country': milk_count.index,
                'Count': milk_count,
                'Max': milk_maximum,
                'Min': milk_minimum,
                'Median': milk_median,
                'Avg': milk_average,
                'Std': milk_std_deviation
            })

            summary = tabulate(milk_summary, headers=milk_summary.columns, tablefmt='html',
                                         showindex=False)

            report = "<br/><div class ='float-child-50'><h3>Milk Summary</h3>" + summary + "</div><div class ='float-clear'>"
            return report

        # Weight stats chart
        def weight_stats():
            df_weight_data_grouped = df_weight_data.groupby('Country')
            # Calculate count, maximum, minimum, standard deviation, median, average, and sum for each group
            weight_count = df_weight_data_grouped['Total'].count()
            weight_maximum = df_weight_data_grouped['Total'].max()
            weight_minimum = df_weight_data_grouped['Total'].min()
            weight_std_deviation = df_weight_data_grouped['Total'].std()
            weight_median = df_weight_data_grouped['Total'].median()
            weight_average = df_weight_data_grouped['Total'].mean()

            # Create a new DataFrame with the results
            weight_summary = pd.DataFrame({
                'Country': weight_count.index,
                'Count': weight_count,
                'Max': weight_maximum,
                'Min': weight_minimum,
                'Median': weight_median,
                'Avg': weight_average,
                'Std': weight_std_deviation
            })

            summary = tabulate(weight_summary, headers=weight_summary.columns, tablefmt='html',
                                           showindex=False)

            report = "<br/><div class ='float-child-50'><h3>Weight Summary</h3>" + summary + "</div><div class ='float-clear'><br/></div>"
            return report

        # generate Data Recording plots


        # Report Header + Title
        rpt_banner = f"<div style ='padding: 5px;'><img src='{banner_img}'/></div><hr/>"
        report_title = "<div style ='padding: 5px;'><strong>Title</strong>: Data Report</div>"
        report_type = "<div style ='padding: 5px;'><strong>Report Type</strong>: Weekly</div>"
        report_date = f"<div style ='padding: 5px;'><strong>Report Date</strong>: {now.strftime('%Y-%m-%d')}</div>"
        report_period = f"<div style ='padding: 5px;'><strong>Report Period</strong>: {start_date} to {end_date} (week {week_number})</div>"

        report_header = rpt_banner + "<div class ='float-child-50'>" + report_title + report_type + "</div><div class ='float-child-50'>" + report_date + report_period + "</div> <hr/><br/"

        final_report = report_header + farm_animal_reg_data_tables() + farm_animal_by_type_summary() + event_monitoring_summary() + milk_stats() + weight_stats() + data_recording_plots() + top_5_ranking_summary()  + enumerator_activity_summary() + qa_summary()
        pdfkit.from_string(final_report, report_pdf, options=pdf_options, css=css_file)

        rpt_dict = {'subplots_file': fig_data_summary, 'report_file': report_pdf}
        kwargs['ti'].xcom_push(key='files', value=rpt_dict)

    @task(task_id="Distribute-Report")
    def email_reports(**kwargs):
        xcom_values = kwargs['ti'].xcom_pull(key='files')
        start_date, recipients_email = kwargs['ti'].xcom_pull()
        report_attachment = xcom_values['report_file']

        send_email_task = EmailOperator(
            task_id='Email-Reports',
            to=recipients_email,
            subject='Weekly Data Flow Report: '+ datetime.today().strftime('%Y-%m-%d'),
            html_content="Hello,<p>The weekly data report is ready for review.<br/>Please note that this email is system-generated; thus, pay attention to the attached file for the detailed report.</p><p>You are receiving this email because you are subscribed to the weekly data report service</p><br/>Regards<br/> Apache Airflow",
            files=[report_attachment]
        )

        return send_email_task.execute(context={})

    # Clean Transaction Tables
    flush_data = MySqlOperator(
        task_id='Flush-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='data_flow_flush_data.sql'
    )

    @task(task_id="Trash-Files")
    def trash_files(**kwargs):
        # Get Return values of Generate-Reports Task
        xcom_values = kwargs['ti'].xcom_pull(key='files')
        subplots_file = xcom_values['subplots_file']
        report_file = xcom_values['report_file']

        # remove the original CSV file
        os.remove(subplots_file)
        os.remove(report_file)

    @task(task_id="Finish")
    def finish():
        return "finish"

    start >> extract_data_flow_report_data >> [extract_animals_data_by_type, extract_farms_data_by_type,
              extract_milk_data_by_type, extract_weight_data_by_type] >> data_flow_report() >> email_reports() >> [
        flush_data, trash_files()] >> finish()


weekly_data_report()
