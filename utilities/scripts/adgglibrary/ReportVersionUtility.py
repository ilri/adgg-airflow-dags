from airflow.models import Variable


class ReportVersionUtility:
    @staticmethod
    def get_desired_version(report_name):
        # Retrieve the report versions JSON from the Airflow Variable
        report_versions_json = Variable.get('report_version1', deserialize_json=True)

        # Search for the report_name in the report versions and fetch its desired version
        for report_version in report_versions_json['report_versions']:
            if report_version['report_name'] == report_name:
                return report_version['version']

        # If the report_name is not found, return a default version or handle it as needed
        return "Default_Version"

