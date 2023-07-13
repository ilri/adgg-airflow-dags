import pdfkit
import uuid
from tabulate import tabulate
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

from datetime import datetime, date, timedelta
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable

now = datetime.now()
hook = MySqlHook(mysql_conn_id='mysql_adgg_db_production')
scripts_dir = f"{Variable.get('scripts_folder')}reports"
output_dir = Variable.get("output_folder")
default_email = Variable.get("default_email")
css_file = '/home/kosgei/airflow/misc/style/style.css'
sns.set_theme(style="white")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False
}

dag_params = {
    "uuid": str(uuid.uuid4()),
    'start_date': date.today() - timedelta(days=7),
    'end_date': date.today(),
}

# Configure PDF options
pdf_options = {
    'page-size': 'A4',
    'enable-local-file-access': None
}

@dag(
    dag_id='Daily-Data-Report',
    start_date=datetime(2021, 1, 1),
    default_args=default_args,
    template_searchpath=[scripts_dir],
    max_active_runs=1,
    schedule=None,
    catchup=False,
    params=dag_params
)
def daily_data_report():
    @task(task_id="Start", provide_context=True)
    def start(**context):
        _uuid = context["params"]["uuid"]
        return _uuid

    start = start()

    # Stage Data > Initial Extractions > Data Stored In a Temporary Table
    # extract_data_flow_report_data = MySqlOperator(
    #     task_id='Extract-Data-Flow-Report-Data',
    #     mysql_conn_id='mysql_adgg_db_production',
    #     sql='extract_data_flow_records.sql',
    #     params={"country": "{{ dag_run.conf['country']}}", "uuid": start}
    # )

    @task(task_id="Generate-Data-Flow-Report", provide_context=True)
    def data_flow_report(**kwargs):
        # Fetch unique uuid
        unique_id = kwargs['ti'].xcom_pull()
        # sql_data_flow = f"SELECT country ,enumerator,event,total FROM reports.rpt_data_flow WHERE uuid ='{unique_id}'"
        sql_data_flow = f"SELECT country ,enumerator,event,total FROM reports.rpt_data_flow "
        df_data_flow = hook.get_pandas_df(sql_data_flow)
        df_data_flow['enumerator'] = df_data_flow['enumerator'].apply(lambda x: x.capitalize())

        arr_distinct_countries = df_data_flow['country'].unique()

        pvt_data_flow = df_data_flow.pivot_table(columns=['event'], values=['total'],
                                                 index=['country', 'enumerator', ], fill_value=0)
        pvt_data_flow.reset_index(inplace=True)

        pvt_data_flow.columns = ['country', 'Enumerator', 'Animal-Reg', 'Calving', 'Exits', 'Farmer-Reg', 'Feeding',
                                 'Hoof-Health', 'Injury', 'Insemination', 'Milk', 'Parasite-Infection',
                                 'PD', 'Sync', 'Weight']

        # Summary Of Data Recording Per country
        html_rpt_title_summary_data_recording = "<br/><div><h3> 1) Summary Of Data Recording </h3></div><br/>"
        fig, axs = plt.subplots(nrows=3, ncols=3, figsize=(15, 15),
                                gridspec_kw={'width_ratios': [1, 1, 1], 'height_ratios': [1, 1, 1]})
        fig_data_summary = output_dir + 'fig_data_summary_plots.png'

        # chart : Animals registered per country
        sns.barplot(data=pvt_data_flow, x="country", y="Animal-Reg", estimator=sum, errorbar=None, ax=axs[0, 0])
        axs[0, 0].set_title('Animal-Registrations Per country')
        axs[0, 0].set_ylabel('Total Animals Registered')
        axs[0, 0].set_xlabel('Countries')

        # chart : Farm Registered per country
        sns.barplot(data=pvt_data_flow, x="country", y="Farmer-Reg", estimator=sum, errorbar=None, ax=axs[0, 1])
        axs[0, 1].set_title('Farm Registrations Per country')
        axs[0, 1].set_ylabel('Total Farms Registered')
        axs[0, 1].set_xlabel('Countries')

        # chart : Calving Registration per country
        sns.barplot(data=pvt_data_flow, x="country", y="Calving", estimator=sum, errorbar=None, ax=axs[0, 2])
        axs[0, 2].set_title('Calving Registrations Per country')
        axs[0, 2].set_ylabel('Total Calvings Registered')
        axs[0, 2].set_xlabel('Countries')

        # chart : Milk Volume per country
        sns.barplot(data=pvt_data_flow, x="country", y="Milk", estimator=sum, errorbar=None, ax=axs[1, 0])
        axs[1, 0].set_title('Milk Recording Per country')
        axs[1, 0].set_ylabel('Total Milk Records (Count)')
        axs[1, 0].set_xlabel('Countries')

        # chart : Weight & Growth Records per country
        sns.barplot(data=pvt_data_flow, x="country", y="Weight", estimator=sum, errorbar=None, ax=axs[1, 1])
        axs[1, 1].set_title('Weight & Growth Recording Per country')
        axs[1, 1].set_ylabel('Total Weight Records (Count)')
        axs[1, 1].set_xlabel('Countries')

        # chart : Insemination Records per country
        sns.barplot(data=pvt_data_flow, x="country", y="Insemination", estimator=sum, errorbar=None, ax=axs[1, 2])
        axs[1, 2].set_title('Insemination Recording Per country')
        axs[1, 2].set_ylabel('Total Insemination Records(Count)')
        axs[1, 2].set_xlabel('Countries')

        # chart : Pregnancy Diagnosis Records per country
        sns.barplot(data=pvt_data_flow, x="country", y="PD", estimator=sum, errorbar=None, ax=axs[2, 0])
        axs[2, 0].set_title('Pregnancy Diagnosis Recording Per country')
        axs[2, 0].set_ylabel('Total PD Records(Count)')
        axs[2, 0].set_xlabel('Countries')

        # chart : Synchronization Records per country
        sns.barplot(data=pvt_data_flow, x="country", y="Sync", estimator=sum, errorbar=None, ax=axs[2, 1])
        axs[2, 1].set_title('Synchronization Recording Per country')
        axs[2, 1].set_ylabel('Total Synchronization Records(Count)')
        axs[2, 1].set_xlabel('Countries')

        # chart : Feeding Records per country
        sns.barplot(data=pvt_data_flow, x="country", y="Feeding", estimator=sum, errorbar=None, ax=axs[2, 2])
        axs[2, 2].set_title('Feeding Recording Per country')
        axs[2, 2].set_ylabel('Total Feeding Records(Count)')
        axs[2, 2].set_xlabel('Countries')

        plt.tight_layout()
        # Save the figure to a file
        plt.savefig(fig_data_summary)

        # Farm & Animal-Registration
        html_rpt_title_reg_summary = "<br/><div class= 'page-break-before'><h3> 2) Registration Summary</h3></div>"
        df_data_flow_filtered_reg = df_data_flow[
            df_data_flow['event'].isin(['Animal-Reg', 'Farmer-Reg'])]
        df_data_flow_reg_grouped = df_data_flow_filtered_reg.groupby(['country', 'event']).sum()
        pvt_data_flow_reg_pvt = df_data_flow_reg_grouped.pivot_table(index='country', columns='event',
                                                                     values='total', fill_value=0)
        pvt_data_flow_reg_pvt.reset_index(inplace=True)
        html_data_flow_reg = tabulate(pvt_data_flow_reg_pvt, headers=pvt_data_flow_reg_pvt.columns, tablefmt='html',
                                      showindex=False)
        html_data_flow_reg = html_rpt_title_reg_summary + "<div style= 'width: 40%;'>" + html_data_flow_reg + "</div>"

        # Events Monitoring Summary
        html_rpt_title_reg_monitoring = "<br/><div><h3> 3) Monitoring Summary</h3></div>"
        df_data_flow_filtered_mon = df_data_flow[
            ~df_data_flow['event'].isin(['Animal-Reg', 'Farmer-Reg'])]
        df_data_flow_mon_grouped = df_data_flow_filtered_mon.groupby(['country', 'event']).sum()
        pvt_data_flow_mon_pvt = df_data_flow_mon_grouped.pivot_table(index='country', columns='event',
                                                                     values='total', fill_value=0)
        pvt_data_flow_mon_pvt.reset_index(inplace=True)
        html_data_flow_mon = tabulate(pvt_data_flow_mon_pvt, headers=pvt_data_flow_mon_pvt.columns, tablefmt='html',
                                      showindex=False)
        html_data_flow_mon = html_rpt_title_reg_monitoring + "<div>" + html_data_flow_mon + "</div>"

        # Enumerator Activity Report
        html_enumerator_activity_rpt_header = "<br/><div><h3> 6) Enumerator Performance Report</h3></div>"
        html_enumerator_activity_rpt = ""
        for country in arr_distinct_countries:
            filtered_df = pvt_data_flow[(pvt_data_flow['country'] == country)]
            filtered_df = filtered_df.drop('country', axis=1)  # remove country Column

            # Convert DataFrame to HTML table
            html_enumerator_activity_table = tabulate(filtered_df, headers=filtered_df.columns, tablefmt='html',
                                                      showindex=False)
            html_enumerator_activity_sub_header = "<div><h3>" + country + "</h3></div>"
            html_enumerator_activity_rpt = html_enumerator_activity_rpt + html_enumerator_activity_sub_header + html_enumerator_activity_table
        html_enumerator_activity_rpt = "<div class ='float-clear'>" + html_enumerator_activity_rpt_header + html_enumerator_activity_rpt + "</div>"

        # Top Ranking Enumerators
        # Define a function to get the top 5 records per group
        def get_top_5(group):
            return group.head(5)

        html_top_5_ranked_enumerator_title = "<div class ='float-clear'> <h3>4)Top Performing Enumerators</h3></div>"
        html_bottom_5_ranked_enumerator_title = "<div class ='float-clear'><h3>5)Least Performing Enumerators</h3></div>"
        html_top_5_ranked_enumerator_rpt = ""
        html_bottom_5_ranked_enumerator_rpt = ""
        for country in arr_distinct_countries:
            df_data_flow_filtered_country = df_data_flow[(df_data_flow['country'] == country)]
            df_data_flow_enum_aggregation = df_data_flow_filtered_country.groupby(['enumerator']).sum()
            html_enumerator_ranking_sub_header = "<h3>" + country + "</h3>"
            columns_to_drop = ['country', 'event']
            df_data_flow_enum_aggregation = df_data_flow_enum_aggregation.drop(columns=columns_to_drop)

            # top
            df_top_5_ranked_enumerator = df_data_flow_enum_aggregation.sort_values('total', ascending=False)
            df_top_5_ranked_enumerator = df_top_5_ranked_enumerator.apply(get_top_5)
            df_top_5_ranked_enumerator.reset_index(inplace=True)
            df_top_5_ranked_enumerator['rank'] = df_top_5_ranked_enumerator['total'].rank(ascending=False,
                                                                                          method='dense')
            df_top_5_ranked_enumerator.columns = ['Enumerator', 'Records', 'Rank']
            html_top_5_ranked_enumerator_tab = tabulate(df_top_5_ranked_enumerator,
                                                        headers=df_top_5_ranked_enumerator.columns, tablefmt='html',
                                                        showindex=False)
            html_top_5_ranked_enumerator_rpt = html_top_5_ranked_enumerator_rpt + "<div class ='float-child'>" + html_enumerator_ranking_sub_header + html_top_5_ranked_enumerator_tab + "</div>"

            # Bottom
            df_bottom_5_ranked_enumerator = df_data_flow_enum_aggregation.sort_values('total', ascending=True)
            df_bottom_5_ranked_enumerator = df_bottom_5_ranked_enumerator.apply(get_top_5)
            df_bottom_5_ranked_enumerator.reset_index(inplace=True)
            df_bottom_5_ranked_enumerator['rank'] = df_bottom_5_ranked_enumerator['total'].rank(ascending=False,
                                                                                                method='dense')
            df_bottom_5_ranked_enumerator.columns = ['Enumerator', 'Records', 'Rank']
            html_bottom_5_ranked_enumerator_tab = tabulate(df_bottom_5_ranked_enumerator,
                                                           headers=df_bottom_5_ranked_enumerator.columns,
                                                           tablefmt='html',
                                                           showindex=False)
            html_bottom_5_ranked_enumerator_rpt = html_bottom_5_ranked_enumerator_rpt + "<div class ='float-child'>" + html_enumerator_ranking_sub_header + html_bottom_5_ranked_enumerator_tab + "</div>"

        html_top_5_ranked_enumerator_rpt = html_top_5_ranked_enumerator_title + html_top_5_ranked_enumerator_rpt
        html_bottom_5_ranked_enumerator_rpt = html_bottom_5_ranked_enumerator_title + html_bottom_5_ranked_enumerator_rpt
        html_combined_ranking_rpt = html_top_5_ranked_enumerator_rpt + "<div class ='float-clear'><br/><br/></div>" + html_bottom_5_ranked_enumerator_rpt

        print(html_combined_ranking_rpt)

        # Report Header + Title
        rpt_banner = "<div style ='padding: 5px;'><img src='/home/kosgei/airflow/misc/img/banner.png'/></div><hr/>"
        report_title = "<div style ='padding: 5px;'><strong>Title</strong>: Data Report</div>"
        report_type = "<div style ='padding: 5px;'><strong>Report Type</strong>: Daily</div>"
        report_date = "<div style ='padding: 5px;'><strong>Report Date</strong>: 23/03/2023</div>"
        report_period = "<div style ='padding: 5px;'><strong>Report Period</strong>: 23/03/2023 -  30/03/2023</div>"
        report_generated_by = "<div style ='padding: 5px;'><strong>Report Generated By</strong>: System</div><hr/>"
        report_header = rpt_banner + report_title + report_type + report_date + report_period + report_generated_by

        html_enumerator_performance_plots = f"<div><img src='{fig_data_summary}'/></div>"
        combined_html = report_header + html_rpt_title_summary_data_recording + html_enumerator_performance_plots + html_data_flow_reg + html_data_flow_mon + html_combined_ranking_rpt + html_enumerator_activity_rpt
        pdfkit.from_string(combined_html, output_dir + 'daily_report.pdf', options=pdf_options, css=css_file)

    @task(task_id="Finish")
    def finish():
        return "finish"

    start >>  data_flow_report() >> finish()

    # start >> extract_data_flow_report_data() >> [data_flow()] >> finish()


daily_data_report()

# References
# 1. https://nicd.org.uk/knowledge-hub/creating-pdf-reports-with-reportlab-and-pandas
